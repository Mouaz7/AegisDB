package se.mouaz.aegisdb.transaction.distributed;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.common.TransactionId;
import se.mouaz.aegisdb.transaction.IsolationLevel;
import se.mouaz.aegisdb.transaction.OperationType;
import se.mouaz.aegisdb.transaction.Transaction;
import se.mouaz.aegisdb.transaction.TransactionState;
import se.mouaz.aegisdb.transaction.WriteOperation;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Cross-shard distributed transaction handle (Master Project Plan §10; US015, Milestone M4).
 * Buffers writes across multiple shards and coordinates Two-Phase Commit via DistributedTransactionCoordinator.
 */
public class DistributedTransaction implements Transaction {
    private static final Logger log = LoggerFactory.getLogger(DistributedTransaction.class);

    private final TransactionId id;
    private final IsolationLevel isolationLevel;
    private final Function<String, ShardId> shardResolver;
    private final BiFunction<ShardId, String, Optional<byte[]>> readFunction;
    private final DistributedTransactionCoordinator coordinator;
    private final long startTimestamp;

    private volatile TransactionState state = TransactionState.ACTIVE;
    private final Map<String, WriteOperation> writeBuffer = new ConcurrentHashMap<>();
    private final Map<String, Optional<byte[]>> readSet = new ConcurrentHashMap<>();
    private final Set<ShardId> touchedShards = ConcurrentHashMap.newKeySet();

    public DistributedTransaction(
            TransactionId id,
            IsolationLevel isolationLevel,
            Function<String, ShardId> shardResolver,
            BiFunction<ShardId, String, Optional<byte[]>> readFunction,
            DistributedTransactionCoordinator coordinator
    ) {
        this(id, isolationLevel, shardResolver, readFunction, coordinator, Instant.now().toEpochMilli());
    }

    public DistributedTransaction(
            TransactionId id,
            IsolationLevel isolationLevel,
            Function<String, ShardId> shardResolver,
            BiFunction<ShardId, String, Optional<byte[]>> readFunction,
            DistributedTransactionCoordinator coordinator,
            long startTimestamp
    ) {
        this.id = Objects.requireNonNull(id, "id cannot be null");
        this.isolationLevel = Objects.requireNonNull(isolationLevel, "isolationLevel cannot be null");
        this.shardResolver = Objects.requireNonNull(shardResolver, "shardResolver cannot be null");
        this.readFunction = Objects.requireNonNull(readFunction, "readFunction cannot be null");
        this.coordinator = Objects.requireNonNull(coordinator, "coordinator cannot be null");
        this.startTimestamp = startTimestamp;
    }

    @Override
    public TransactionId id() {
        return id;
    }

    @Override
    public TransactionState state() {
        return state;
    }

    @Override
    public IsolationLevel isolationLevel() {
        return isolationLevel;
    }

    @Override
    public Optional<byte[]> get(String key) {
        checkActive();
        Objects.requireNonNull(key, "key cannot be null");

        // 1. Read-your-own-writes from local transaction write buffer
        WriteOperation buffered = writeBuffer.get(key);
        if (buffered != null) {
            return switch (buffered.type()) {
                case PUT -> Optional.ofNullable(buffered.value());
                case DELETE -> Optional.empty();
            };
        }

        // 2. Read from target shard
        ShardId shardId = shardResolver.apply(key);
        touchedShards.add(shardId);
        Optional<byte[]> val = readFunction.apply(shardId, key);
        readSet.putIfAbsent(key, val);
        return val;
    }

    @Override
    public void put(String key, byte[] value) {
        checkActive();
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");

        ShardId shardId = shardResolver.apply(key);
        touchedShards.add(shardId);
        writeBuffer.put(key, WriteOperation.put(key, value));
    }

    @Override
    public void delete(String key) {
        checkActive();
        Objects.requireNonNull(key, "key cannot be null");

        ShardId shardId = shardResolver.apply(key);
        touchedShards.add(shardId);
        writeBuffer.put(key, WriteOperation.delete(key));
    }

    @Override
    public void prepare() {
        checkActive();
        state = TransactionState.PREPARED;
    }

    @Override
    public long commit() {
        if (state.isTerminal()) {
            throw new IllegalStateException("Transaction " + id + " is already terminal: " + state);
        }

        state = TransactionState.PREPARING;

        if (writeBuffer.isEmpty()) {
            // Read-only transaction commits immediately
            state = TransactionState.COMMITTED;
            return startTimestamp;
        }

        // Group writes by target shard
        Map<ShardId, List<WriteOperation>> writesByShard = new HashMap<>();
        for (WriteOperation op : writeBuffer.values()) {
            ShardId shardId = shardResolver.apply(op.key());
            writesByShard.computeIfAbsent(shardId, k -> new ArrayList<>()).add(op);
        }

        // Group reads by target shard
        Map<ShardId, Map<String, Optional<byte[]>>> readsByShard = new HashMap<>();
        for (Map.Entry<String, Optional<byte[]>> entry : readSet.entrySet()) {
            ShardId shardId = shardResolver.apply(entry.getKey());
            readsByShard.computeIfAbsent(shardId, k -> new HashMap<>()).put(entry.getKey(), entry.getValue());
        }

        try {
            coordinator.commit(id, writesByShard, readsByShard, startTimestamp).join();
            state = TransactionState.COMMITTED;
            return startTimestamp;
        } catch (Exception e) {
            state = TransactionState.ABORTED;
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof DistributedTransactionAbortedException dte) {
                throw dte;
            }
            throw new DistributedTransactionAbortedException("2PC commit failed for txId=" + id, cause);
        }
    }

    @Override
    public void abort() {
        if (state.isTerminal()) {
            return;
        }

        state = TransactionState.ABORTED;
        if (!touchedShards.isEmpty()) {
            try {
                coordinator.abort(id, touchedShards, null).join();
            } catch (Exception e) {
                log.warn("Error aborting distributed txId={}: {}", id, e.getMessage());
            }
        }
    }

    private void checkActive() {
        if (state != TransactionState.ACTIVE && state != TransactionState.PREPARED) {
            throw new IllegalStateException("Transaction " + id + " is not active (state=" + state + ")");
        }
    }
}
