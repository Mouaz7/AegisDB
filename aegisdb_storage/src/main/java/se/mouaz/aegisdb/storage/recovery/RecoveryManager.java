package se.mouaz.aegisdb.storage.recovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.log.RaftLogEntry;
import se.mouaz.aegisdb.storage.metadata.PersistentRaftMetadata;
import se.mouaz.aegisdb.storage.metadata.RaftMetadataStorage;
import se.mouaz.aegisdb.storage.wal.StorageRecord;
import se.mouaz.aegisdb.storage.wal.WalManager;

import se.mouaz.aegisdb.storage.snapshot.FileSnapshotReader;
import se.mouaz.aegisdb.storage.snapshot.SnapshotReader;

import java.io.IOException;
import java.util.*;

/**
 * Orchestrates the full crash recovery sequence according to Section 8 of the Master Project Plan:
 * Load persistent Raft metadata
 * -> Load latest valid snapshot
 * -> Scan WAL segments
 * -> Validate checksum and record framing
 * -> Stop safely at incomplete tail
 * -> Replay valid committed records
 * -> Ready to start transport and Raft
 */
public class RecoveryManager {
    private static final Logger log = LoggerFactory.getLogger(RecoveryManager.class);

    private final RaftMetadataStorage metadataStorage;
    private final WalRecoveryManager walRecoveryManager;
    private final FileSnapshotReader snapshotReader;

    public RecoveryManager(RaftMetadataStorage metadataStorage, WalManager walManager) {
        this(metadataStorage, walManager, null);
    }

    public RecoveryManager(RaftMetadataStorage metadataStorage, WalManager walManager, FileSnapshotReader snapshotReader) {
        this.metadataStorage = Objects.requireNonNull(metadataStorage, "metadataStorage cannot be null");
        this.walRecoveryManager = new WalRecoveryManager(Objects.requireNonNull(walManager, "walManager cannot be null"));
        this.snapshotReader = snapshotReader;
    }

    public WalRecoveryManager walRecoveryManager() {
        return walRecoveryManager;
    }

    public FileSnapshotReader snapshotReader() {
        return snapshotReader;
    }

    /**
     * Executes the recovery sequence and returns the recovered state.
     */
    public RecoveryResult recover() throws IOException {
        long startTime = System.currentTimeMillis();
        log.info("Beginning node crash recovery sequence...");

        // 1. Load persistent Raft metadata (term, votedFor)
        Optional<PersistentRaftMetadata> metaOpt = metadataStorage.load();
        long recoveredTerm = metaOpt.map(PersistentRaftMetadata::currentTerm).orElse(0L);
        NodeId recoveredVotedFor = metaOpt.map(PersistentRaftMetadata::votedFor).orElse(null);
        log.info("1/5: Loaded persistent Raft metadata: term={}, votedFor={}", recoveredTerm, recoveredVotedFor);

        // 2. Load latest valid snapshot (Sprint 5)
        long snapshotIndex = 0L;
        long snapshotTerm = 0L;
        byte[] snapshotData = null;
        if (snapshotReader != null) {
            Optional<SnapshotReader.SnapshotReadResult> snapOpt = snapshotReader.readLatestSnapshot();
            if (snapOpt.isPresent()) {
                SnapshotReader.SnapshotReadResult snap = snapOpt.get();
                snapshotIndex = snap.metadata().lastIncludedIndex();
                snapshotTerm = snap.metadata().lastIncludedTerm();
                snapshotData = snap.data();
                log.info("2/5: Loaded latest valid snapshot: index={}, term={}, dataLen={}",
                        snapshotIndex, snapshotTerm, snapshotData.length);
            } else {
                log.info("2/5: No valid snapshot found on disk.");
            }
        } else {
            log.info("2/5: Snapshot reader not configured.");
        }

        // 3 & 4. Scan WAL segments, validate checksums, and stop safely at incomplete tail
        log.info("3/5: Scanning WAL segments and validating record framing...");
        WalRecoveryManager.WalScanResult scanResult = walRecoveryManager.scanAndRecover();

        // 5. Replay valid records to reconstruct Raft log entries starting after snapshotIndex
        log.info("4/5: Replaying valid records to reconstruct consensus log state...");
        List<RaftLogEntry> replayedEntries = new ArrayList<>();
        long lastIndex = snapshotIndex;
        long lastTerm = snapshotTerm;

        for (StorageRecord record : scanResult.records()) {
            if (record.sequenceNumber() <= snapshotIndex) {
                // Entry already incorporated in snapshot
                continue;
            }
            if (record.recordType() == StorageRecord.TYPE_DATA) {
                // If value exists, use value; else if key exists, use key
                byte[] data = record.value();
                RaftLogEntry entry = new RaftLogEntry(record.sequenceNumber(), record.term(), data);
                replayedEntries.add(entry);
                lastIndex = record.sequenceNumber();
                lastTerm = record.term();
            } else if (record.recordType() == StorageRecord.TYPE_TRUNCATE) {
                // Truncate in-memory replay list from specified index
                long truncateFromIndex = record.sequenceNumber();
                replayedEntries.removeIf(e -> e.index() >= truncateFromIndex);
                if (!replayedEntries.isEmpty()) {
                    RaftLogEntry last = replayedEntries.get(replayedEntries.size() - 1);
                    lastIndex = last.index();
                    lastTerm = last.term();
                } else {
                    lastIndex = snapshotIndex;
                    lastTerm = snapshotTerm;
                }
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        log.info("5/5: Recovery sequence completed in {} ms. LastLogIndex={}, LastLogTerm={}, Entries={}, SnapshotIndex={}",
                duration, lastIndex, lastTerm, replayedEntries.size(), snapshotIndex);

        return new RecoveryResult(
                recoveredTerm,
                recoveredVotedFor,
                lastIndex,
                lastTerm,
                Collections.unmodifiableList(replayedEntries),
                scanResult.tornTailsRepairedCount(),
                duration,
                snapshotIndex,
                snapshotTerm,
                snapshotData,
                scanResult.storageIndex()
        );
    }
}
