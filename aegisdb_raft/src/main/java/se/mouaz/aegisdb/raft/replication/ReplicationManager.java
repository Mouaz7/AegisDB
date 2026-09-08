package se.mouaz.aegisdb.raft.replication;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.protocol.AppendEntriesRequest;
import se.mouaz.aegisdb.protocol.AppendEntriesResponse;
import se.mouaz.aegisdb.protocol.InstallSnapshotResponse;
import se.mouaz.aegisdb.raft.NotLeaderException;
import se.mouaz.aegisdb.raft.event.AppendEntriesResponseEvent;
import se.mouaz.aegisdb.raft.event.InstallSnapshotResponseEvent;
import se.mouaz.aegisdb.raft.event.RaftEvent;
import se.mouaz.aegisdb.raft.log.RaftLog;
import se.mouaz.aegisdb.raft.log.RaftLogEntry;
import se.mouaz.aegisdb.raft.snapshot.SnapshotManager;
import se.mouaz.aegisdb.raft.state.RaftInvariants;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.raft.state.RaftState;
import se.mouaz.aegisdb.transport.RaftTransport;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Coordinates Raft log replication from the leader to cluster followers (Section 83; Ongaro §5.3).
 */
public class ReplicationManager {
    private static final Logger log = LoggerFactory.getLogger(ReplicationManager.class);

    private final NodeId localNodeId;
    private final RaftState state;
    private final RaftLog raftLog;
    private final ClusterConfiguration clusterConfig;
    private final RaftTransport transport;
    private final CommitIndexManager commitIndexManager;
    private final Consumer<RaftEvent> eventDispatcher;

    private final Map<Long, CompletableFuture<Long>> pendingClientFutures = new ConcurrentHashMap<>();
    private final Map<NodeId, FollowerReplicationState> followers = new ConcurrentHashMap<>();
    private SnapshotManager snapshotManager;

    public ReplicationManager(NodeId localNodeId,
                              RaftState state,
                              RaftLog raftLog,
                              ClusterConfiguration clusterConfig,
                              RaftTransport transport,
                              CommitIndexManager commitIndexManager,
                              Consumer<RaftEvent> eventDispatcher) {
        this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId cannot be null");
        this.state = Objects.requireNonNull(state, "state cannot be null");
        this.raftLog = Objects.requireNonNull(raftLog, "raftLog cannot be null");
        this.clusterConfig = Objects.requireNonNull(clusterConfig, "clusterConfig cannot be null");
        this.transport = Objects.requireNonNull(transport, "transport cannot be null");
        this.commitIndexManager = commitIndexManager != null ? commitIndexManager : new CommitIndexManager();
        this.eventDispatcher = Objects.requireNonNull(eventDispatcher, "eventDispatcher cannot be null");
    }

    public void setSnapshotManager(SnapshotManager snapshotManager) {
        this.snapshotManager = snapshotManager;
    }

    /**
     * Re-initializes replication states for all cluster followers when this node is elected leader.
     */
    public void initialize(Iterable<NodeId> peers, long lastLogIndex) {
        followers.clear();
        for (NodeId peer : peers) {
            if (!peer.equals(localNodeId)) {
                followers.put(peer, new FollowerReplicationState(peer, lastLogIndex + 1));
            }
        }
        state.leaderState().initialize(peers, lastLogIndex);
        log.info("Initialized ReplicationManager on leader {} with {} followers, nextIndex={}",
                localNodeId, followers.size(), lastLogIndex + 1);
    }

    /**
     * Proposes a new command from a client. Must be called on node event loop.
     */
    public void propose(byte[] command, CompletableFuture<Long> future) {
        if (state.role() != RaftRole.LEADER) {
            future.completeExceptionally(new NotLeaderException(state.currentLeader().orElse(null)));
            return;
        }

        long entryIndex = raftLog.lastLogIndex() + 1;
        long term = state.currentTerm();
        RaftLogEntry entry = new RaftLogEntry(entryIndex, term, command);
        raftLog.append(entry);

        pendingClientFutures.put(entryIndex, future);

        // In a 1-node cluster, leader is the entire majority
        if (followers.isEmpty() || clusterConfig.clusterSize() <= 1) {
            state.volatileState().setCommitIndex(entryIndex);
            future.complete(entryIndex);
            pendingClientFutures.remove(entryIndex);
            return;
        }

        // Broadcast replication to all followers
        broadcastReplication();
    }

    /**
     * Triggers replication to all followers.
     */
    public void broadcastReplication() {
        if (state.role() != RaftRole.LEADER) {
            return;
        }
        for (NodeId peer : followers.keySet()) {
            replicateTo(peer);
        }
    }

    /**
     * Replicates log entries to a specific follower if no RPC is currently in flight.
     */
    public void replicateTo(NodeId peer) {
        if (state.role() != RaftRole.LEADER) {
            return;
        }

        FollowerReplicationState follower = followers.get(peer);
        if (follower == null) {
            return;
        }

        if (!follower.tryStartRpc()) {
            return;
        }

        long next = follower.nextIndex();
        if (next <= raftLog.snapshotIndex()) {
            log.info("Leader {}: follower {} nextIndex {} <= snapshotIndex {}. Triggering InstallSnapshot catch-up.",
                    localNodeId, peer, next, raftLog.snapshotIndex());
            if (snapshotManager != null) {
                snapshotManager.sendSnapshot(peer, state.currentTerm(), localNodeId, transport)
                        .whenComplete((response, ex) -> {
                            follower.finishRpc();
                            if (ex == null && response != null) {
                                eventDispatcher.accept(new InstallSnapshotResponseEvent(peer, state.currentTerm(), response, raftLog.snapshotIndex()));
                            } else {
                                log.debug("InstallSnapshot to peer {} failed: {}", peer, ex != null ? ex.getMessage() : "null");
                            }
                        });
            } else {
                follower.finishRpc();
            }
            return;
        }

        long prevLogIndex = next - 1;
        long prevLogTerm = raftLog.getTerm(prevLogIndex);
        List<RaftLogEntry> entries = raftLog.getEntriesFrom(next);
        byte[] serialized = RaftLogEntry.serializeList(entries);
        long leaderCommit = state.volatileState().commitIndex();
        long term = state.currentTerm();

        AppendEntriesRequest request = new AppendEntriesRequest(
                term,
                localNodeId,
                prevLogIndex,
                prevLogTerm,
                serialized,
                leaderCommit
        );

        transport.appendEntries(peer, request).whenComplete((response, ex) -> {
            follower.finishRpc();
            if (ex == null && response != null) {
                eventDispatcher.accept(new AppendEntriesResponseEvent(peer, term, response));
            } else {
                log.debug("Replication to peer {} failed or timed out: {}",
                        peer, ex != null ? ex.getMessage() : "null response");
            }
        });
    }

    /**
     * Handles InstallSnapshotResponse on the leader's event loop.
     */
    public void handleInstallSnapshotResponse(NodeId peer, InstallSnapshotResponse response, long snapshotIndex) {
        if (state.role() != RaftRole.LEADER) {
            return;
        }
        FollowerReplicationState follower = followers.get(peer);
        if (follower == null) {
            return;
        }

        if (response.term() > state.currentTerm()) {
            log.info("Leader {} stepping down: peer {} returned higher term {} on InstallSnapshot",
                    localNodeId, peer, response.term());
            state.becomeFollower(response.term(), null);
            failPendingFutures(new NotLeaderException(null));
            return;
        }

        if (response.success()) {
            follower.recordSuccess(snapshotIndex);
            follower.setNextIndex(snapshotIndex + 1);
            follower.setMatchIndex(snapshotIndex);
            state.leaderState().setMatchIndex(peer, snapshotIndex);
            state.leaderState().setNextIndex(peer, snapshotIndex + 1);
            log.info("Follower {} caught up via InstallSnapshot to index {}", peer, snapshotIndex);

            if (follower.matchIndex() < raftLog.lastLogIndex()) {
                replicateTo(peer);
            }
        }
    }

    /**
     * Handles AppendEntriesResponse on the node's single-threaded event loop.
     */
    public void handleAppendEntriesResponse(NodeId peer, AppendEntriesResponse response) {
        if (state.role() != RaftRole.LEADER) {
            return;
        }

        FollowerReplicationState follower = followers.get(peer);
        if (follower == null) {
            return;
        }

        if (response.term() > state.currentTerm()) {
            log.info("Leader {} stepping down: peer {} returned higher term {}",
                    localNodeId, peer, response.term());
            state.becomeFollower(response.term(), null);
            failPendingFutures(new NotLeaderException(null));
            return;
        }

        if (response.success()) {
            follower.recordSuccess(response.matchIndex());
            state.leaderState().setMatchIndex(peer, response.matchIndex());
            state.leaderState().setNextIndex(peer, response.matchIndex() + 1);

            long oldCommit = state.volatileState().commitIndex();
            long newCommit = commitIndexManager.computeNewCommitIndex(
                    raftLog,
                    state.currentTerm(),
                    oldCommit,
                    followers.values(),
                    clusterConfig.clusterSize()
            );

            if (newCommit > oldCommit) {
                state.volatileState().setCommitIndex(newCommit);
                log.info("Leader {} advanced commitIndex from {} to {}", localNodeId, oldCommit, newCommit);
                RaftInvariants.assertCommittedEntriesNeverOverwritten(newCommit, raftLog);

                for (long idx = oldCommit + 1; idx <= newCommit; idx++) {
                    CompletableFuture<Long> fut = pendingClientFutures.remove(idx);
                    if (fut != null) {
                        fut.complete(idx);
                    }
                }
                broadcastReplication();
            }

            // If follower is still behind, continue replicating immediately
            if (follower.matchIndex() < raftLog.lastLogIndex()) {
                replicateTo(peer);
            }
        } else {
            // Rejection due to log mismatch: decrement nextIndex and retry immediately
            log.debug("Peer {} rejected AppendEntries, decrementing nextIndex (matchHint={})",
                    peer, response.matchIndex());
            follower.decrementNextIndex(response.matchIndex());
            state.leaderState().setNextIndex(peer, follower.nextIndex());
            replicateTo(peer);
        }
    }

    /**
     * Fails all uncommitted client futures (e.g. when stepping down or stopping).
     */
    public void failPendingFutures(Throwable ex) {
        for (CompletableFuture<Long> fut : pendingClientFutures.values()) {
            fut.completeExceptionally(ex);
        }
        pendingClientFutures.clear();
    }

    public Optional<FollowerReplicationState> getFollowerState(NodeId peer) {
        return Optional.ofNullable(followers.get(peer));
    }

    public Collection<FollowerReplicationState> allFollowerStates() {
        return Collections.unmodifiableCollection(followers.values());
    }

    public int pendingFuturesCount() {
        return pendingClientFutures.size();
    }
}
