package se.mouaz.aegisdb.raft.statemachine;

import se.mouaz.aegisdb.common.ByteArrayKey;
import se.mouaz.aegisdb.common.DatabaseException;
import se.mouaz.aegisdb.common.ErrorCode;
import se.mouaz.aegisdb.common.ShardLifecycle;
import se.mouaz.aegisdb.protocol.SplitCommandCodec;
import se.mouaz.aegisdb.protocol.pb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Replicated Key-Value store state machine with Multi-Raft Range Partitioning & Dynamic Splitting (§28, Phase 1).
 * Supports:
 * - Post-Prepare write fences at appliedLogIndex
 * - Dual index boundaries (writeFenceIndex vs snapshotBarrierIndex)
 * - Non-destructive FinalizeSplit with delayed safe GC
 * - Tombstoning on abort
 */
public class KeyValueStateMachine implements StateMachine {
    private static final Logger log = LoggerFactory.getLogger(KeyValueStateMachine.class);

    private final ConcurrentNavigableMap<String, byte[]> store = new ConcurrentSkipListMap<>();
    private volatile long lastAppliedIndex = 0L;

    // Multi-Raft Sharding State
    private volatile ShardLifecycle lifecycle = ShardLifecycle.ACTIVE;
    private volatile ByteArrayKey splitKey = null;
    private volatile long writeFenceIndex = -1L;
    private volatile long snapshotBarrierIndex = -1L;
    private volatile String activeSplitOperationId = null;

    private final Set<String> unroutableStaleKeys = ConcurrentHashMap.newKeySet();
    private final Set<String> inFlightPreparedTxns = ConcurrentHashMap.newKeySet();

    public KeyValueStateMachine() {}

    @Override
    public synchronized byte[] apply(long index, byte[] commandBytes) {
        if (commandBytes == null || commandBytes.length == 0) {
            this.lastAppliedIndex = index;
            return new byte[0];
        }

        // 1. Check for Multi-Raft Split Coordination Commands
        if (SplitCommandCodec.isSplitCommand(commandBytes)) {
            this.lastAppliedIndex = index;
            return handleSplitCommand(index, commandBytes);
        }

        // 2. Standard Key-Value Operations (KvCommand)
        KvCommand command;
        try {
            command = KvCommand.fromBytes(commandBytes);
        } catch (Exception e) {
            log.warn("Failed to parse KvCommand at index {}: {}", index, e.getMessage());
            this.lastAppliedIndex = index;
            return new byte[0];
        }

        this.lastAppliedIndex = index;

        // Check if shard is in READY state (not yet activated)
        if (lifecycle == ShardLifecycle.READY) {
            throw new DatabaseException(ErrorCode.SHARD_NOT_ACTIVATED, "Shard is in READY state, not yet activated");
        }

        // Check if shard is TOMBSTONED
        if (lifecycle == ShardLifecycle.TOMBSTONED) {
            throw new DatabaseException(ErrorCode.NODE_STOPPED, "Shard is TOMBSTONED and cannot process operations");
        }

        // Check unroutable stale keys (keys >= splitKey after FinalizeSplit awaiting delayed GC)
        if (unroutableStaleKeys.contains(command.key())) {
            throw new DatabaseException(ErrorCode.STALE_EPOCH, "Key " + command.key() + " belongs to retired range on this parent shard");
        }

        // Check post-Prepare write fence
        if (lifecycle == ShardLifecycle.SPLITTING && (command.opType() == KvCommand.OpType.PUT || command.opType() == KvCommand.OpType.DELETE)) {
            ByteArrayKey key = ByteArrayKey.of(command.key());
            if (splitKey != null && key.compareTo(splitKey) >= 0) {
                if (inFlightPreparedTxns.remove(command.key())) {
                    log.info("Draining pre-existing PREPARED transaction key={} at index={} (> fenceIndex={})",
                            command.key(), index, writeFenceIndex);
                } else {
                    log.debug("Write fence rejected key={} at index={}, fenceIndex={}", command.key(), index, writeFenceIndex);
                    throw new DatabaseException(ErrorCode.SPLITTING_RANGE, "Key " + command.key() + " is in splitting range >= " + splitKey);
                }
            }
        }

        return switch (command.opType()) {
            case PUT -> {
                byte[] previous = store.put(command.key(), command.value());
                log.debug("Applied PUT key={}, valLen={}, at index={}", command.key(), command.value().length, index);
                yield previous != null ? previous : new byte[0];
            }
            case GET -> {
                byte[] val = store.get(command.key());
                yield val != null ? val : new byte[0];
            }
            case DELETE -> {
                byte[] removed = store.remove(command.key());
                log.debug("Applied DELETE key={} at index={}", command.key(), index);
                yield removed != null ? removed : new byte[0];
            }
        };
    }

    private byte[] handleSplitCommand(long index, byte[] commandBytes) {
        byte tag = SplitCommandCodec.getTag(commandBytes);
        try {
            switch (tag) {
                case SplitCommandCodec.TAG_PREPARE_SPLIT -> {
                    PrepareSplitCommandProto cmd = SplitCommandCodec.decodePrepareSplit(commandBytes);
                    applyPrepareSplit(index, cmd);
                }
                case SplitCommandCodec.TAG_SNAPSHOT_BARRIER -> {
                    SplitSnapshotBarrierCommandProto cmd = SplitCommandCodec.decodeSnapshotBarrier(commandBytes);
                    applySnapshotBarrier(index, cmd);
                }
                case SplitCommandCodec.TAG_FINALIZE_SPLIT -> {
                    FinalizeSplitCommandProto cmd = SplitCommandCodec.decodeFinalizeSplit(commandBytes);
                    applyFinalizeSplit(index, cmd);
                }
                case SplitCommandCodec.TAG_ACTIVATE_SHARD -> {
                    ActivateShardCommandProto cmd = SplitCommandCodec.decodeActivateShard(commandBytes);
                    applyActivateShard(index, cmd);
                }
                case SplitCommandCodec.TAG_ABORT_SPLIT -> {
                    AbortSplitCommandProto cmd = SplitCommandCodec.decodeAbortSplit(commandBytes);
                    applyAbortSplit(index, cmd);
                }
                case SplitCommandCodec.TAG_ABORT_BOOTSTRAP -> {
                    AbortBootstrapCommandProto cmd = SplitCommandCodec.decodeAbortBootstrap(commandBytes);
                    applyAbortBootstrap(index, cmd);
                }
                case SplitCommandCodec.TAG_BOOTSTRAP_COMPLETE -> {
                    BootstrapCompleteCommandProto cmd = SplitCommandCodec.decodeBootstrapComplete(commandBytes);
                    applyBootstrapComplete(index, cmd);
                }
                default -> log.warn("Unhandled split command tag: {}", tag);
            }
        } catch (Exception e) {
            log.error("Failed to apply split command at index {}", index, e);
            throw new DatabaseException(ErrorCode.INTERNAL_ERROR, "Failed to apply split command: " + e.getMessage(), e);
        }
        return new byte[0];
    }

    public synchronized void applyPrepareSplit(long index, PrepareSplitCommandProto cmd) {
        String opId = cmd.getSplitOperationId();
        if (opId.equals(activeSplitOperationId) && lifecycle == ShardLifecycle.SPLITTING) {
            log.info("Idempotent duplicate PrepareSplit at index={} for opId={}", index, opId);
            return;
        }

        if (lifecycle != ShardLifecycle.ACTIVE) {
            throw new DatabaseException(ErrorCode.SPLIT_PRECONDITION_FAILED,
                    "Cannot prepare split: shard is in state " + lifecycle + ", expected ACTIVE");
        }

        this.lifecycle = ShardLifecycle.SPLITTING;
        this.splitKey = ByteArrayKey.of(cmd.getSplitKey().toByteArray());
        this.writeFenceIndex = index;
        this.activeSplitOperationId = opId;
        log.info("Applied PrepareSplit: opId={}, splitKey={}, writeFenceIndex={}", opId, splitKey, writeFenceIndex);
    }

    public synchronized void applySnapshotBarrier(long index, SplitSnapshotBarrierCommandProto cmd) {
        String opId = cmd.getSplitOperationId();
        if (lifecycle != ShardLifecycle.SPLITTING || !opId.equals(activeSplitOperationId)) {
            log.warn("Ignoring SnapshotBarrier: lifecycle={}, opId={}, activeOpId={}", lifecycle, opId, activeSplitOperationId);
            return;
        }

        this.snapshotBarrierIndex = index;
        log.info("Applied SnapshotBarrier: opId={}, snapshotBarrierIndex={}", opId, snapshotBarrierIndex);
    }

    public synchronized void applyFinalizeSplit(long index, FinalizeSplitCommandProto cmd) {
        String opId = cmd.getSplitOperationId();
        if (opId.equals(activeSplitOperationId) && lifecycle == ShardLifecycle.ACTIVE) {
            log.info("Idempotent duplicate FinalizeSplit at index={} for opId={}", index, opId);
            return;
        }

        ByteArrayKey finalizeSplitKey = splitKey != null ? splitKey : ByteArrayKey.of(cmd.getSplitKey().toByteArray());

        // Non-destructive finalize: mark keys >= splitKey as unroutable stale data (retained for delayed GC)
        for (String key : store.keySet()) {
            ByteArrayKey k = ByteArrayKey.of(key);
            if (k.compareTo(finalizeSplitKey) >= 0) {
                unroutableStaleKeys.add(key);
            }
        }

        this.lifecycle = ShardLifecycle.ACTIVE;
        this.splitKey = null;
        this.writeFenceIndex = -1L;
        this.snapshotBarrierIndex = -1L;
        this.activeSplitOperationId = null;

        log.info("Applied FinalizeSplit: opId={}, marked {} keys as unroutable stale data for delayed GC",
                opId, unroutableStaleKeys.size());
    }

    public synchronized void applyActivateShard(long index, ActivateShardCommandProto cmd) {
        this.lifecycle = ShardLifecycle.ACTIVE;
        this.activeSplitOperationId = null;
        log.info("Applied ActivateShard: shard activated for opId={}", cmd.getSplitOperationId());
    }

    public synchronized void applyAbortSplit(long index, AbortSplitCommandProto cmd) {
        this.lifecycle = ShardLifecycle.ACTIVE;
        this.splitKey = null;
        this.writeFenceIndex = -1L;
        this.snapshotBarrierIndex = -1L;
        this.activeSplitOperationId = null;
        log.info("Applied AbortSplit: opId={}, reverted parent to ACTIVE", cmd.getSplitOperationId());
    }

    public synchronized void applyAbortBootstrap(long index, AbortBootstrapCommandProto cmd) {
        this.lifecycle = ShardLifecycle.TOMBSTONED;
        this.activeSplitOperationId = null;
        log.info("Applied AbortBootstrap: child TOMBSTONED for opId={}", cmd.getSplitOperationId());
    }

    public synchronized void applyBootstrapComplete(long index, BootstrapCompleteCommandProto cmd) {
        this.lifecycle = ShardLifecycle.READY;
        this.writeFenceIndex = cmd.getPrepareFenceIndex();
        this.snapshotBarrierIndex = cmd.getSnapshotBarrierIndex();
        this.activeSplitOperationId = cmd.getSplitOperationId();
        log.info("Applied BootstrapComplete: child transitioned to READY for opId={}, fenceIndex={}, barrierIndex={}",
                cmd.getSplitOperationId(), writeFenceIndex, snapshotBarrierIndex);
    }

    /**
     * Creates an MVCC-complete snapshot slice strictly containing entries >= splitKey.
     */
    public synchronized byte[] takeSnapshotSlice(ByteArrayKey splitKey, long barrierIndex) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            List<Map.Entry<String, byte[]>> sliceEntries = new ArrayList<>();
            for (Map.Entry<String, byte[]> entry : store.entrySet()) {
                if (ByteArrayKey.of(entry.getKey()).compareTo(splitKey) >= 0) {
                    sliceEntries.add(entry);
                }
            }

            dos.writeInt(sliceEntries.size());
            for (Map.Entry<String, byte[]> entry : sliceEntries) {
                byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                dos.writeInt(keyBytes.length);
                dos.write(keyBytes);

                byte[] valBytes = entry.getValue();
                dos.writeInt(valBytes.length);
                dos.write(valBytes);
            }
            dos.flush();
            byte[] snapshot = baos.toByteArray();
            log.info("Took state machine snapshot slice at barrierIndex={}: {} entries, {} bytes",
                    barrierIndex, sliceEntries.size(), snapshot.length);
            return snapshot;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize snapshot slice", e);
        }
    }

    /**
     * Computes the SHA-256 checksum of snapshot data.
     */
    public static byte[] computeChecksum(byte[] data) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return md.digest(data != null ? data : new byte[0]);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm not available", e);
        }
    }

    /**
     * Asynchronously prunes stale keys that were moved during FinalizeSplit (delayed safe GC).
     */
    public synchronized int pruneStaleKeys() {
        int count = 0;
        for (String staleKey : unroutableStaleKeys) {
            if (store.remove(staleKey) != null) {
                count++;
            }
        }
        unroutableStaleKeys.clear();
        log.info("Delayed GC pruned {} stale keys from state machine", count);
        return count;
    }

    @Override
    public synchronized byte[] takeSnapshot() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            dos.writeInt(store.size());
            for (Map.Entry<String, byte[]> entry : store.entrySet()) {
                byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                dos.writeInt(keyBytes.length);
                dos.write(keyBytes);

                byte[] valBytes = entry.getValue();
                dos.writeInt(valBytes.length);
                dos.write(valBytes);
            }
            dos.flush();
            byte[] snapshot = baos.toByteArray();
            log.info("Took state machine snapshot at lastAppliedIndex={}: {} entries, {} bytes",
                    lastAppliedIndex, store.size(), snapshot.length);
            return snapshot;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize state machine snapshot", e);
        }
    }

    @Override
    public synchronized void restoreSnapshot(long lastIncludedIndex, byte[] snapshotData) {
        store.clear();
        this.lastAppliedIndex = lastIncludedIndex;

        if (snapshotData == null || snapshotData.length == 0) {
            log.info("Restored empty state machine snapshot at lastIncludedIndex={}", lastIncludedIndex);
            return;
        }

        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(snapshotData);
            DataInputStream dis = new DataInputStream(bais);

            int entryCount = dis.readInt();
            for (int i = 0; i < entryCount; i++) {
                int keyLen = dis.readInt();
                byte[] keyBytes = new byte[keyLen];
                dis.readFully(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);

                int valLen = dis.readInt();
                byte[] valBytes = new byte[valLen];
                dis.readFully(valBytes);

                store.put(key, valBytes);
            }
            log.info("Restored state machine snapshot at lastIncludedIndex={}: {} entries restored",
                    lastIncludedIndex, store.size());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to deserialize state machine snapshot", e);
        }
    }

    @Override
    public long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    public byte[] get(String key) {
        if (lifecycle == ShardLifecycle.READY) {
            throw new DatabaseException(ErrorCode.SHARD_NOT_ACTIVATED, "Shard is in READY state, not yet activated");
        }
        if (lifecycle == ShardLifecycle.TOMBSTONED) {
            throw new DatabaseException(ErrorCode.NODE_STOPPED, "Shard is TOMBSTONED and cannot process operations");
        }
        if (unroutableStaleKeys.contains(key)) {
            throw new DatabaseException(ErrorCode.STALE_EPOCH, "Key " + key + " is in unroutable stale range");
        }
        return store.get(key);
    }

    public boolean containsKey(String key) {
        if (unroutableStaleKeys.contains(key)) {
            return false;
        }
        return store.containsKey(key);
    }

    public int size() {
        return store.size();
    }

    public Map<String, byte[]> snapshotMap() {
        return Collections.unmodifiableMap(new ConcurrentSkipListMap<>(store));
    }

    public ShardLifecycle lifecycle() {
        return lifecycle;
    }

    public void setLifecycle(ShardLifecycle lifecycle) {
        this.lifecycle = lifecycle;
    }

    public ByteArrayKey splitKey() {
        return splitKey;
    }

    public long writeFenceIndex() {
        return writeFenceIndex;
    }

    public long snapshotBarrierIndex() {
        return snapshotBarrierIndex;
    }

    public String activeSplitOperationId() {
        return activeSplitOperationId;
    }

    public Set<String> unroutableStaleKeys() {
        return Collections.unmodifiableSet(unroutableStaleKeys);
    }

    public void registerPreparedTransactionKey(String key) {
        inFlightPreparedTxns.add(Objects.requireNonNull(key, "key cannot be null"));
    }

    public Set<String> inFlightPreparedTxns() {
        return Collections.unmodifiableSet(inFlightPreparedTxns);
    }
}
