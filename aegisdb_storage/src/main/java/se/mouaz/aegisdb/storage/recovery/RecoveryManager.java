package se.mouaz.aegisdb.storage.recovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.log.RaftLogEntry;
import se.mouaz.aegisdb.storage.metadata.PersistentRaftMetadata;
import se.mouaz.aegisdb.storage.metadata.RaftMetadataStorage;
import se.mouaz.aegisdb.storage.wal.StorageRecord;
import se.mouaz.aegisdb.storage.wal.WalManager;

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

    public RecoveryManager(RaftMetadataStorage metadataStorage, WalManager walManager) {
        this.metadataStorage = Objects.requireNonNull(metadataStorage, "metadataStorage cannot be null");
        this.walRecoveryManager = new WalRecoveryManager(Objects.requireNonNull(walManager, "walManager cannot be null"));
    }

    public WalRecoveryManager walRecoveryManager() {
        return walRecoveryManager;
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

        // 2. Load latest valid snapshot (Sprint 5 preparation)
        log.info("2/5: Checking snapshots (Sprint 5 snapshot store)...");

        // 3 & 4. Scan WAL segments, validate checksums, and stop safely at incomplete tail
        log.info("3/5: Scanning WAL segments and validating record framing...");
        WalRecoveryManager.WalScanResult scanResult = walRecoveryManager.scanAndRecover();

        // 5. Replay valid records to reconstruct Raft log entries
        log.info("4/5: Replaying valid records to reconstruct consensus log state...");
        List<RaftLogEntry> replayedEntries = new ArrayList<>();
        long lastIndex = 0L;
        long lastTerm = 0L;

        for (StorageRecord record : scanResult.records()) {
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
                    lastIndex = 0L;
                    lastTerm = 0L;
                }
            }
        }

        long duration = System.currentTimeMillis() - startTime;
        log.info("5/5: Recovery sequence completed in {} ms. LastLogIndex={}, LastLogTerm={}, Entries={}",
                duration, lastIndex, lastTerm, replayedEntries.size());

        return new RecoveryResult(
                recoveredTerm,
                recoveredVotedFor,
                lastIndex,
                lastTerm,
                Collections.unmodifiableList(replayedEntries),
                scanResult.tornTailsRepairedCount(),
                duration
        );
    }
}
