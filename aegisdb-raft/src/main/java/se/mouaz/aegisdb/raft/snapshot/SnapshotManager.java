package se.mouaz.aegisdb.raft.snapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.protocol.InstallSnapshotRequest;
import se.mouaz.aegisdb.protocol.InstallSnapshotResponse;
import se.mouaz.aegisdb.raft.log.RaftLogRepository;
import se.mouaz.aegisdb.raft.statemachine.StateMachine;
import se.mouaz.aegisdb.transport.RaftTransport;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Manages snapshot creation, chunking, transmission, and restoration (Raft Section 7; US009, US010).
 */
public class SnapshotManager {
    private static final Logger log = LoggerFactory.getLogger(SnapshotManager.class);
    public static final int CHUNK_SIZE = 64 * 1024; // 64 KB per AC2

    private final StateMachine stateMachine;
    private final RaftLogRepository raftLog;
    private final BiConsumer<Long, byte[]> persistentSaveCallback;
    private final BiConsumer<Long, Long> onSnapshotInstalledCallback; // (lastIncludedIndex, lastIncludedTerm) -> RaftNode state

    // Follower state for assembling incoming chunked snapshots
    private final ByteArrayOutputStream incomingBuffer = new ByteArrayOutputStream();
    private long incomingLastIndex = 0;
    private long incomingLastTerm = 0;

    // Cached latest snapshot on leader
    private volatile byte[] latestSnapshotData;
    private volatile long latestSnapshotIndex = 0;
    private volatile long latestSnapshotTerm = 0;

    public SnapshotManager(
            StateMachine stateMachine,
            RaftLogRepository raftLog,
            BiConsumer<Long, byte[]> persistentSaveCallback,
            BiConsumer<Long, Long> onSnapshotInstalledCallback) {
        this.stateMachine = Objects.requireNonNull(stateMachine, "stateMachine cannot be null");
        this.raftLog = Objects.requireNonNull(raftLog, "raftLog cannot be null");
        this.persistentSaveCallback = persistentSaveCallback;
        this.onSnapshotInstalledCallback = onSnapshotInstalledCallback;
    }

    public SnapshotManager(StateMachine stateMachine, RaftLogRepository raftLog) {
        this(stateMachine, raftLog, null, null);
    }

    public StateMachine stateMachine() {
        return stateMachine;
    }

    public RaftLogRepository raftLog() {
        return raftLog;
    }

    public long latestSnapshotIndex() {
        return latestSnapshotIndex;
    }

    public long latestSnapshotTerm() {
        return latestSnapshotTerm;
    }

    public byte[] latestSnapshotData() {
        return latestSnapshotData;
    }

    /**
     * Creates a new point-in-time snapshot, compacts the Raft log, and invokes persistence.
     */
    public synchronized byte[] takeSnapshot(long lastIncludedIndex, long lastIncludedTerm) {
        byte[] data = stateMachine.takeSnapshot();
        this.latestSnapshotData = data;
        this.latestSnapshotIndex = lastIncludedIndex;
        this.latestSnapshotTerm = lastIncludedTerm;

        if (persistentSaveCallback != null) {
            persistentSaveCallback.accept(lastIncludedIndex, data);
        }

        raftLog.compactUpTo(lastIncludedIndex, lastIncludedTerm);
        log.info("Snapshot taken and log compacted up to index={}, term={}, dataLen={}",
                lastIncludedIndex, lastIncludedTerm, data.length);
        return data;
    }

    /**
     * Handles an incoming InstallSnapshotRequest chunk on a follower.
     */
    public synchronized InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request, long currentTerm) {
        if (request.term() < currentTerm) {
            return InstallSnapshotResponse.failure(currentTerm);
        }

        if (request.offset() == 0) {
            incomingBuffer.reset();
            incomingLastIndex = request.lastIncludedIndex();
            incomingLastTerm = request.lastIncludedTerm();
            log.info("Starting reception of incoming snapshot: lastIncludedIndex={}, lastIncludedTerm={}",
                    incomingLastIndex, incomingLastTerm);
        }

        if (request.data() != null && request.data().length > 0) {
            incomingBuffer.write(request.data(), 0, request.data().length);
        }

        if (request.done()) {
            byte[] fullSnapshot = incomingBuffer.toByteArray();
            log.info("Completed reception of incoming snapshot: {} bytes. Restoring state machine...",
                    fullSnapshot.length);

            stateMachine.restoreSnapshot(request.lastIncludedIndex(), fullSnapshot);
            raftLog.compactUpTo(request.lastIncludedIndex(), request.lastIncludedTerm());

            if (onSnapshotInstalledCallback != null) {
                onSnapshotInstalledCallback.accept(request.lastIncludedIndex(), request.lastIncludedTerm());
            }

            if (persistentSaveCallback != null) {
                persistentSaveCallback.accept(request.lastIncludedIndex(), fullSnapshot);
            }

            this.latestSnapshotData = fullSnapshot;
            this.latestSnapshotIndex = request.lastIncludedIndex();
            this.latestSnapshotTerm = request.lastIncludedTerm();
            incomingBuffer.reset();
        }

        return InstallSnapshotResponse.success(currentTerm);
    }

    /**
     * Sends snapshot to a peer in chunked RPCs (Raft §7, 64 KB chunks).
     */
    public CompletableFuture<InstallSnapshotResponse> sendSnapshot(
            NodeId peer,
            long term,
            NodeId leaderId,
            RaftTransport transport) {

        byte[] data = latestSnapshotData;
        long lastIndex = latestSnapshotIndex;
        long lastTerm = latestSnapshotTerm;

        if (data == null || lastIndex == 0) {
            lastIndex = raftLog.snapshotIndex();
            lastTerm = raftLog.snapshotTerm();
            data = stateMachine.takeSnapshot();
            this.latestSnapshotData = data;
            this.latestSnapshotIndex = lastIndex;
            this.latestSnapshotTerm = lastTerm;
        }

        return sendChunk(peer, term, leaderId, lastIndex, lastTerm, data, 0, transport);
    }

    private CompletableFuture<InstallSnapshotResponse> sendChunk(
            NodeId peer,
            long term,
            NodeId leaderId,
            long lastIncludedIndex,
            long lastIncludedTerm,
            byte[] data,
            int offset,
            RaftTransport transport) {

        int remaining = data.length - offset;
        int currentChunkLen = Math.min(remaining, CHUNK_SIZE);
        boolean done = (offset + currentChunkLen >= data.length);

        byte[] chunk = (currentChunkLen > 0) ? Arrays.copyOfRange(data, offset, offset + currentChunkLen) : new byte[0];

        InstallSnapshotRequest request = new InstallSnapshotRequest(
                term,
                leaderId,
                lastIncludedIndex,
                lastIncludedTerm,
                offset,
                chunk,
                done
        );

        return transport.installSnapshot(peer, request).thenCompose(resp -> {
            if (resp == null || !resp.success() || done) {
                return CompletableFuture.completedFuture(resp != null ? resp : InstallSnapshotResponse.failure(term));
            }
            // Send next chunk recursively
            return sendChunk(peer, term, leaderId, lastIncludedIndex, lastIncludedTerm, data, offset + currentChunkLen, transport);
        });
    }
}
