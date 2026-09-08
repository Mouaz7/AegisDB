package se.mouaz.aegisdb.mvcc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

/**
 * Thread-safe, multi-version key-value store providing Snapshot Isolation (Master Plan §9 & §17).
 * <p>
 * Key Design Properties:
 * <ul>
 *   <li>Lock-free read path: readers never block writers, writers never block readers.</li>
 *   <li>Snapshot Isolation: readers see consistent point-in-time snapshots.</li>
 *   <li>Dirty read and dirty write prevention.</li>
 *   <li>Watermarked garbage collection safety.</li>
 *   <li>Snapshot serialization for Raft state machine integration.</li>
 * </ul>
 */
public class MvccStore {
    private static final Logger log = LoggerFactory.getLogger(MvccStore.class);
    private static final int SNAPSHOT_MAGIC = 0x4D564343; // "MVCC"
    private static final byte SNAPSHOT_VERSION = 1;

    private final TimestampProvider timestampProvider;
    private final ConcurrentMap<String, VersionChain> chains = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, ActiveTx> activeTransactions = new ConcurrentHashMap<>();
    private final ConcurrentMap<Long, Long> activeSnapshots = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Long> keyWriteLocks = new ConcurrentHashMap<>();
    private final Object commitLock = new Object();

    private final AtomicLong txIdGenerator = new AtomicLong(1000);
    private final AtomicLong snapshotIdGenerator = new AtomicLong(1);

    /**
     * Active in-flight transaction descriptor.
     */
    public static class ActiveTx {
        private final long txId;
        private final long startTimestamp;
        private final Set<Long> inFlightTxsAtStart;
        final Set<String> modifiedKeys = ConcurrentHashMap.newKeySet();
        private final ConcurrentMap<String, Long> readVersionTimestamps = new ConcurrentHashMap<>();

        public ActiveTx(long txId, long startTimestamp, Set<Long> inFlightTxsAtStart) {
            this.txId = txId;
            this.startTimestamp = startTimestamp;
            this.inFlightTxsAtStart = Collections.unmodifiableSet(new HashSet<>(inFlightTxsAtStart));
        }

        public long txId() {
            return txId;
        }

        public long startTimestamp() {
            return startTimestamp;
        }

        public Set<Long> inFlightTxsAtStart() {
            return inFlightTxsAtStart;
        }

        public Set<String> modifiedKeys() {
            return Collections.unmodifiableSet(modifiedKeys);
        }

        public void recordRead(String key, long commitTimestamp) {
            readVersionTimestamps.putIfAbsent(key, commitTimestamp);
        }

        public Long getReadVersionCommitTimestamp(String key) {
            return readVersionTimestamps.get(key);
        }
    }

    public MvccStore() {
        this(new TimestampProvider());
    }

    public MvccStore(TimestampProvider timestampProvider) {
        this.timestampProvider = Objects.requireNonNull(timestampProvider, "timestampProvider must not be null");
    }

    public TimestampProvider timestampProvider() {
        return timestampProvider;
    }

    public ConcurrentMap<String, VersionChain> chains() {
        return chains;
    }

    // ==========================================
    // Transaction Lifecycle Management
    // ==========================================

    /**
     * Starts a new transaction, allocating a unique transaction ID and recording its start timestamp.
     */
    public long beginTransaction() {
        synchronized (commitLock) {
            long txId = txIdGenerator.incrementAndGet();
            long startTimestamp = timestampProvider.nextTimestamp();
            Set<Long> inFlight = new HashSet<>(activeTransactions.keySet());
            ActiveTx tx = new ActiveTx(txId, startTimestamp, inFlight);
            activeTransactions.put(txId, tx);
            log.debug("Transaction {} started at logical timestamp {}", txId, startTimestamp);
            return txId;
        }
    }

    /**
     * Checks if a transaction is currently active.
     */
    public boolean isTransactionActive(long txId) {
        return activeTransactions.containsKey(txId);
    }

    /**
     * Retrieves active transaction metadata.
     */
    public Optional<ActiveTx> getActiveTransaction(long txId) {
        return Optional.ofNullable(activeTransactions.get(txId));
    }

    /**
     * Commits all uncommitted writes performed by the given transaction.
     *
     * @param txId transaction ID to commit
     * @return logical commit timestamp
     * @throws MvccException if the transaction is not active
     */
    public long commit(long txId) {
        long commitTimestamp;
        synchronized (commitLock) {
            ActiveTx tx = activeTransactions.get(txId);
            if (tx == null) {
                throw new MvccException("Transaction " + txId + " is not active or already finished");
            }

            commitTimestamp = timestampProvider.nextTimestamp();
            for (String key : tx.modifiedKeys) {
                VersionChain chain = chains.get(key);
                if (chain != null) {
                    chain.commitVersion(txId, commitTimestamp);
                }
            }
            activeTransactions.remove(txId);
            for (String key : tx.modifiedKeys) {
                keyWriteLocks.remove(key, txId);
            }
        }

        log.debug("Transaction {} committed at logical timestamp {}", txId, commitTimestamp);
        return commitTimestamp;
    }

    /**
     * Aborts all uncommitted writes performed by the given transaction.
     *
     * @param txId transaction ID to abort
     * @return true if an active transaction was aborted, false otherwise
     */
    public boolean abort(long txId) {
        synchronized (commitLock) {
            ActiveTx tx = activeTransactions.get(txId);
            if (tx == null) {
                return false;
            }

            for (String key : tx.modifiedKeys) {
                VersionChain chain = chains.get(key);
                if (chain != null) {
                    chain.abortVersion(txId);
                }
            }
            activeTransactions.remove(txId);
            for (String key : tx.modifiedKeys) {
                keyWriteLocks.remove(key, txId);
            }

            log.debug("Transaction {} aborted and uncommitted versions rolled back", txId);
            return true;
        }
    }

    // ==========================================
    // Snapshot Management
    // ==========================================

    /**
     * Creates an immutable point-in-time snapshot of the database at current logical time.
     * The snapshot tracks currently in-flight transactions and registers with the active snapshot tracker.
     */
    public Snapshot createSnapshot() {
        synchronized (commitLock) {
            long snapshotId = snapshotIdGenerator.incrementAndGet();
            long readTimestamp = timestampProvider.currentTimestamp();
            Set<Long> activeTxs = Set.copyOf(activeTransactions.keySet());
            activeSnapshots.put(snapshotId, readTimestamp);

            return new Snapshot(
                    snapshotId,
                    readTimestamp,
                    0L,
                    activeTxs,
                    () -> activeSnapshots.remove(snapshotId)
            );
        }
    }

    /**
     * Creates a snapshot tailored for a specific in-flight transaction.
     * Allows Read-Your-Own-Writes for this transaction while maintaining snapshot isolation for all others.
     */
    public Snapshot createSnapshotForTransaction(long txId) {
        synchronized (commitLock) {
            ActiveTx tx = activeTransactions.get(txId);
            if (tx == null) {
                throw new MvccException("Transaction " + txId + " is not active");
            }

            long snapshotId = snapshotIdGenerator.incrementAndGet();
            long readTimestamp = tx.startTimestamp();
            Set<Long> activeTxs = new HashSet<>(tx.inFlightTxsAtStart());
            activeSnapshots.put(snapshotId, readTimestamp);

            return new Snapshot(
                    snapshotId,
                    readTimestamp,
                    txId,
                    activeTxs,
                    () -> activeSnapshots.remove(snapshotId)
            );
        }
    }

    /**
     * Calculates the minimum read timestamp across all currently active snapshots.
     * Used as the garbage collection watermark.
     */
    public long minActiveSnapshotTimestamp() {
        if (activeSnapshots.isEmpty()) {
            return timestampProvider.currentTimestamp();
        }
        long min = Long.MAX_VALUE;
        for (long ts : activeSnapshots.values()) {
            if (ts < min) {
                min = ts;
            }
        }
        return min;
    }

    public int activeSnapshotCount() {
        return activeSnapshots.size();
    }

    // ==========================================
    // Write Operations
    // ==========================================

    /**
     * Writes a new value under the context of an active transaction.
     *
     * @param key   record key
     * @param value byte payload
     * @param txId  active transaction ID
     * @throws WriteConflictException if the key is already locked by another uncommitted transaction
     */
    public void put(String key, byte[] value, long txId) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        ActiveTx tx = getRequiredActiveTx(txId);

        acquireWriteLock(key, tx);
        tx.modifiedKeys.add(key);

        VersionChain chain = chains.computeIfAbsent(key, VersionChain::new);
        chain.append(new VersionedValue(txId, VersionedValue.UNCOMMITTED, value, false));
    }

    /**
     * Marks a record deleted (tombstone) under the context of an active transaction.
     *
     * @param key  record key
     * @param txId active transaction ID
     * @throws WriteConflictException if the key is already locked by another uncommitted transaction
     */
    public void delete(String key, long txId) {
        Objects.requireNonNull(key, "key must not be null");
        ActiveTx tx = getRequiredActiveTx(txId);

        acquireWriteLock(key, tx);
        tx.modifiedKeys.add(key);

        VersionChain chain = chains.computeIfAbsent(key, VersionChain::new);
        chain.append(new VersionedValue(txId, VersionedValue.UNCOMMITTED, new byte[0], true));
    }

    /**
     * Convenience auto-committing write for standalone updates.
     */
    public long put(String key, byte[] value) {
        long txId = beginTransaction();
        try {
            put(key, value, txId);
            return commit(txId);
        } catch (RuntimeException e) {
            abort(txId);
            throw e;
        }
    }

    /**
     * Convenience auto-committing delete for standalone deletes.
     */
    public long delete(String key) {
        long txId = beginTransaction();
        try {
            delete(key, txId);
            return commit(txId);
        } catch (RuntimeException e) {
            abort(txId);
            throw e;
        }
    }

    private void acquireWriteLock(String key, ActiveTx tx) {
        long txId = tx.txId();
        Long existingLockTx = keyWriteLocks.putIfAbsent(key, txId);
        if (existingLockTx != null && existingLockTx != txId) {
            throw new WriteConflictException(key, txId, existingLockTx);
        }

        if (existingLockTx != null && existingLockTx == txId) {
            // Already holds write lock for this key in this transaction
            return;
        }

        // First-Committer-Wins (Snapshot Isolation conflict detection):
        // Ensure no concurrent transaction committed a modification to this key after this transaction started
        // or since the transaction read this key.
        VersionChain chain = chains.get(key);
        if (chain != null) {
            VersionedValue node = chain.head();
            // Find the most recent committed version (skipping any uncommitted nodes)
            while (node != null && !node.isCommitted()) {
                node = node.next();
            }

            Long readTs = tx.getReadVersionCommitTimestamp(key);
            if (readTs != null) {
                // If the transaction previously read this key, ensure no newer version committed since:
                if (readTs == 0L) {
                    // Key was non-existent when read; conflict if a committed version now exists
                    if (node != null) {
                        keyWriteLocks.remove(key, txId);
                        throw new WriteConflictException(key, txId, node.createTxId());
                    }
                } else if (node == null || node.commitTimestamp() > readTs || tx.inFlightTxsAtStart().contains(node.createTxId())) {
                    keyWriteLocks.remove(key, txId);
                    long conflictingTx = (node != null) ? node.createTxId() : -1L;
                    throw new WriteConflictException(key, txId, conflictingTx);
                }
            } else {
                // Blind write: ensure no concurrent transaction committed after this tx started
                // or was in-flight when this tx started
                if (node != null) {
                    boolean committedAfterStart = node.commitTimestamp() > tx.startTimestamp();
                    boolean committedByInFlight = tx.inFlightTxsAtStart().contains(node.createTxId());
                    if (committedAfterStart || committedByInFlight) {
                        keyWriteLocks.remove(key, txId);
                        throw new WriteConflictException(key, txId, node.createTxId());
                    }
                }
            }
        }
    }

    private ActiveTx getRequiredActiveTx(long txId) {
        ActiveTx tx = activeTransactions.get(txId);
        if (tx == null) {
            throw new MvccException("Cannot write: transaction " + txId + " is not active");
        }
        return tx;
    }

    // ==========================================
    // Read Operations (Lock-Free)
    // ==========================================

    /**
     * Reads the value of a key visible to the given snapshot without blocking.
     */
    public Optional<byte[]> get(String key, Snapshot snapshot) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(snapshot, "snapshot must not be null");

        VersionChain chain = chains.get(key);
        if (chain == null) {
            if (snapshot.readerTxId() > 0) {
                ActiveTx tx = activeTransactions.get(snapshot.readerTxId());
                if (tx != null) {
                    tx.recordRead(key, 0L);
                }
            }
            return Optional.empty();
        }

        Optional<VersionedValue> visibleNode = chain.findVisibleNode(snapshot);
        if (snapshot.readerTxId() > 0) {
            ActiveTx tx = activeTransactions.get(snapshot.readerTxId());
            if (tx != null) {
                if (visibleNode.isPresent()) {
                    tx.recordRead(key, visibleNode.get().commitTimestamp());
                } else {
                    tx.recordRead(key, 0L);
                }
            }
        }

        if (visibleNode.isEmpty() || visibleNode.get().isTombstone()) {
            return Optional.empty();
        }
        return Optional.of(visibleNode.get().value());
    }

    /**
     * Reads the current committed value of a key using a point-in-time snapshot.
     */
    public Optional<byte[]> get(String key) {
        try (Snapshot snapshot = createSnapshot()) {
            return get(key, snapshot);
        }
    }

    /**
     * Scans all key-value entries visible to the given snapshot.
     */
    public Map<String, byte[]> scan(Snapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        Map<String, byte[]> result = new HashMap<>();

        for (Map.Entry<String, VersionChain> entry : chains.entrySet()) {
            Optional<byte[]> val = entry.getValue().findVisible(snapshot);
            val.ifPresent(bytes -> result.put(entry.getKey(), bytes));
        }

        return Collections.unmodifiableMap(result);
    }

    /**
     * Returns total number of registered keys (including keys that might only have deleted versions).
     */
    public int keyCount() {
        return chains.size();
    }

    /**
     * Checks if a key chain exists in the store.
     */
    public boolean containsKey(String key) {
        return get(key).isPresent();
    }

    // ==========================================
    // Garbage Collection Support
    // ==========================================

    /**
     * Removes an obsolete key if its chain only contains a tombstone and has no active transactions.
     */
    public boolean removeIfObsolete(String key) {
        if (keyWriteLocks.containsKey(key)) {
            return false;
        }

        VersionChain chain = chains.get(key);
        if (chain != null) {
            VersionedValue head = chain.head();
            if (head != null && head.isTombstone() && head.isCommitted() && head.next() == null) {
                return chains.remove(key, chain);
            }
        }
        return false;
    }

    // ==========================================
    // State Machine Snapshot Serialization (Raft)
    // ==========================================

    /**
     * Serializes all committed active records into a binary snapshot for Raft log compaction.
     */
    public byte[] serializeSnapshot() {
        try (Snapshot snapshot = createSnapshot();
             ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            Map<String, byte[]> visibleData = scan(snapshot);

            dos.writeInt(SNAPSHOT_MAGIC);
            dos.writeByte(SNAPSHOT_VERSION);
            dos.writeLong(snapshot.readTimestamp());
            dos.writeInt(visibleData.size());

            CRC32 crc = new CRC32();
            ByteArrayOutputStream payloadBaos = new ByteArrayOutputStream();
            DataOutputStream payloadDos = new DataOutputStream(payloadBaos);

            for (Map.Entry<String, byte[]> entry : visibleData.entrySet()) {
                byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                payloadDos.writeShort(keyBytes.length);
                payloadDos.write(keyBytes);
                payloadDos.writeInt(entry.getValue().length);
                payloadDos.write(entry.getValue());
            }
            payloadDos.flush();

            byte[] payload = payloadBaos.toByteArray();
            crc.update(payload);

            dos.writeLong(crc.getValue());
            dos.write(payload);
            dos.flush();

            return baos.toByteArray();
        } catch (IOException e) {
            throw new MvccException("Failed to serialize MVCC snapshot", e);
        }
    }

    /**
     * Restores state from a serialized snapshot, clearing existing in-memory state.
     */
    public void restoreSnapshot(byte[] data) {
        Objects.requireNonNull(data, "snapshot data must not be null");

        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             DataInputStream dis = new DataInputStream(bais)) {

            int magic = dis.readInt();
            if (magic != SNAPSHOT_MAGIC) {
                throw new MvccException("Invalid MVCC snapshot magic: 0x" + Integer.toHexString(magic));
            }

            byte version = dis.readByte();
            if (version != SNAPSHOT_VERSION) {
                throw new MvccException("Unsupported MVCC snapshot version: " + version);
            }

            long snapshotTimestamp = dis.readLong();
            int recordCount = dis.readInt();
            long expectedCrc = dis.readLong();

            byte[] payload = dis.readAllBytes();
            CRC32 crc = new CRC32();
            crc.update(payload);
            if (crc.getValue() != expectedCrc) {
                throw new MvccException("MVCC snapshot CRC mismatch: expected " + expectedCrc + " but was " + crc.getValue());
            }

            // Clear in-memory state
            chains.clear();
            activeTransactions.clear();
            keyWriteLocks.clear();
            activeSnapshots.clear();

            try (DataInputStream payloadDis = new DataInputStream(new ByteArrayInputStream(payload))) {
                for (int i = 0; i < recordCount; i++) {
                    short keyLen = payloadDis.readShort();
                    byte[] keyBytes = new byte[keyLen];
                    payloadDis.readFully(keyBytes);
                    String key = new String(keyBytes, StandardCharsets.UTF_8);

                    int valLen = payloadDis.readInt();
                    byte[] valBytes = new byte[valLen];
                    payloadDis.readFully(valBytes);

                    VersionedValue committedVersion = new VersionedValue(
                            0L,
                            snapshotTimestamp,
                            valBytes,
                            false
                    );
                    chains.put(key, new VersionChain(key, committedVersion));
                }
            }

            // Advance timestamp provider beyond restored snapshot timestamp
            while (timestampProvider.currentTimestamp() <= snapshotTimestamp) {
                timestampProvider.nextTimestamp();
            }

            log.info("Restored MVCC state from snapshot with {} records at timestamp {}", recordCount, snapshotTimestamp);
        } catch (IOException e) {
            throw new MvccException("Failed to restore MVCC snapshot", e);
        }
    }
}
