package se.mouaz.aegisdb.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.protocol.AppendEntriesRequest;
import se.mouaz.aegisdb.protocol.AppendEntriesResponse;
import se.mouaz.aegisdb.protocol.RequestVoteRequest;
import se.mouaz.aegisdb.protocol.RequestVoteResponse;
import se.mouaz.aegisdb.raft.election.ElectionManager;
import se.mouaz.aegisdb.raft.election.ElectionTimer;
import se.mouaz.aegisdb.raft.election.RequestVoteHandler;
import se.mouaz.aegisdb.raft.event.*;
import se.mouaz.aegisdb.raft.heartbeat.AppendEntriesHandler;
import se.mouaz.aegisdb.raft.heartbeat.HeartbeatManager;
import se.mouaz.aegisdb.raft.log.RaftLog;
import se.mouaz.aegisdb.raft.replication.CommitIndexManager;
import se.mouaz.aegisdb.raft.replication.LogConflictResolver;
import se.mouaz.aegisdb.raft.replication.ReplicationManager;
import se.mouaz.aegisdb.raft.state.PersistentRaftState;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.raft.state.RaftState;
import se.mouaz.aegisdb.raft.time.Clock;
import se.mouaz.aegisdb.raft.time.Scheduler;
import se.mouaz.aegisdb.raft.time.SystemClock;
import se.mouaz.aegisdb.raft.time.SystemScheduler;
import se.mouaz.aegisdb.transport.RaftRequestHandler;
import se.mouaz.aegisdb.transport.RaftTransport;
import se.mouaz.aegisdb.transport.TransportException;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RaftNode coordinates the Raft consensus protocol for a single node (Sections 15, 82, 83).
 * Operates on a single ordered executor to guarantee sequential state mutations (Section 107).
 */
public class RaftNode implements RaftRequestHandler, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    private final NodeId nodeId;
    private final ClusterConfiguration clusterConfig;
    private final RaftTransport transport;
    private final Clock clock;
    private final Scheduler scheduler;
    private final boolean ownsScheduler;

    private final RaftState state;
    private final RaftLog raftLog;
    private final LogConflictResolver conflictResolver;
    private final CommitIndexManager commitIndexManager;
    private final ReplicationManager replicationManager;

    private final ElectionTimer electionTimer;
    private final ElectionManager electionManager;
    private final RequestVoteHandler requestVoteHandler;
    private final HeartbeatManager heartbeatManager;
    private final AppendEntriesHandler appendEntriesHandler;

    // Single-ordered event queue executor per Raft node (Section 107)
    private final ExecutorService eventExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public RaftNode(NodeId nodeId,
                    ClusterConfiguration clusterConfig,
                    RaftTransport transport,
                    PersistentRaftState persistentState,
                    RaftLog raftLog,
                    Clock clock,
                    Scheduler scheduler,
                    Duration minElectionTimeout,
                    Duration maxElectionTimeout,
                    Duration heartbeatInterval,
                    Random random) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId cannot be null");
        this.clusterConfig = Objects.requireNonNull(clusterConfig, "clusterConfig cannot be null");
        this.transport = Objects.requireNonNull(transport, "transport cannot be null");
        this.clock = clock != null ? clock : new SystemClock();
        this.ownsScheduler = scheduler == null;
        this.scheduler = scheduler != null ? scheduler : new SystemScheduler();

        this.state = new RaftState(nodeId, persistentState);
        this.raftLog = raftLog != null ? raftLog : new RaftLog();
        this.conflictResolver = new LogConflictResolver();
        this.commitIndexManager = new CommitIndexManager();

        this.eventExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "raft-event-loop-" + nodeId.value());
            t.setDaemon(true);
            return t;
        });

        // 1. Election Timer
        this.electionTimer = new ElectionTimer(
                this.scheduler,
                this.clock,
                minElectionTimeout,
                maxElectionTimeout,
                random,
                () -> postEvent(new ElectionTimeoutEvent(state.currentTerm()))
        );

        // 2. Replication Manager
        this.replicationManager = new ReplicationManager(
                nodeId,
                state,
                this.raftLog,
                clusterConfig,
                transport,
                commitIndexManager,
                this::postEvent
        );

        // 3. Heartbeat Manager
        this.heartbeatManager = new HeartbeatManager(
                state,
                transport,
                clusterConfig,
                this.scheduler,
                heartbeatInterval,
                heartbeat -> {
                    for (NodeId peer : clusterConfig.members().keySet()) {
                        if (!peer.equals(nodeId)) {
                            transport.appendEntries(peer, heartbeat).whenComplete((resp, ex) -> {
                                if (ex == null && resp != null) {
                                    postEvent(new AppendEntriesResponseEvent(peer, heartbeat.term(), resp));
                                }
                            });
                        }
                    }
                }
        );

        // 4. Election Manager
        this.electionManager = new ElectionManager(
                state,
                transport,
                clusterConfig,
                electionTimer,
                election -> {
                    // Leader elected callback (§5.2, §5.3)
                    heartbeatManager.startHeartbeats();
                    replicationManager.initialize(clusterConfig.members().keySet(), this.raftLog.lastLogIndex());
                },
                (peer, req) -> {
                    transport.requestVote(peer, req).whenComplete((resp, ex) -> {
                        if (ex == null && resp != null) {
                            postEvent(new VoteResponseEvent(peer, req.term(), resp));
                        }
                    });
                }
        );

        // 5. RequestVote Handler
        this.requestVoteHandler = new RequestVoteHandler(state, electionTimer);

        // 6. AppendEntries Handler
        this.appendEntriesHandler = new AppendEntriesHandler(state, electionTimer, this.raftLog, conflictResolver);
    }

    public RaftNode(NodeId nodeId,
                    ClusterConfiguration clusterConfig,
                    RaftTransport transport,
                    PersistentRaftState persistentState,
                    Clock clock,
                    Scheduler scheduler,
                    Duration minElectionTimeout,
                    Duration maxElectionTimeout,
                    Duration heartbeatInterval,
                    Random random) {
        this(nodeId, clusterConfig, transport, persistentState, null, clock, scheduler,
                minElectionTimeout, maxElectionTimeout, heartbeatInterval, random);
    }

    public static Builder builder() {
        return new Builder();
    }

    public synchronized void start() {
        if (running.compareAndSet(false, true)) {
            log.info("Starting RaftNode {}", nodeId);
            transport.registerHandler(this);
            electionTimer.reset();
        }
    }

    public synchronized void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Stopping RaftNode {}", nodeId);
            electionTimer.cancel();
            heartbeatManager.stopHeartbeats();
            replicationManager.failPendingFutures(TransportException.nodeStopped("Node " + nodeId + " is stopped"));
            eventExecutor.shutdown();
            try {
                if (!eventExecutor.awaitTermination(2, TimeUnit.SECONDS)) {
                    eventExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                eventExecutor.shutdownNow();
            }
            if (ownsScheduler) {
                scheduler.close();
            }
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public NodeId nodeId() {
        return nodeId;
    }

    public RaftRole role() {
        return state.role();
    }

    public long currentTerm() {
        return state.currentTerm();
    }

    public Optional<NodeId> currentLeader() {
        return state.currentLeader();
    }

    public RaftState state() {
        return state;
    }

    public RaftLog log() {
        return raftLog;
    }

    public ReplicationManager replicationManager() {
        return replicationManager;
    }

    public long commitIndex() {
        return state.volatileState().commitIndex();
    }

    public long lastApplied() {
        return state.volatileState().lastApplied();
    }

    public ElectionTimer electionTimer() {
        return electionTimer;
    }

    public Scheduler scheduler() {
        return scheduler;
    }

    public Clock clock() {
        return clock;
    }

    /**
     * Proposes a new command to be replicated by Raft (Section 83; US006).
     * If this node is the leader, appends entry and replicates to majority before completing future.
     */
    public CompletableFuture<Long> propose(byte[] command) {
        if (!running.get()) {
            return CompletableFuture.failedFuture(TransportException.nodeStopped("Node " + nodeId + " is stopped"));
        }
        if (state.role() != RaftRole.LEADER) {
            return CompletableFuture.failedFuture(new NotLeaderException(state.currentLeader().orElse(null)));
        }
        CompletableFuture<Long> future = new CompletableFuture<>();
        postEvent(new ClientWriteEvent(command, future));
        return future;
    }

    /**
     * Replicates a client command (alias for propose).
     */
    public CompletableFuture<Long> replicate(byte[] command) {
        return propose(command);
    }

    // --- RaftRequestHandler Overrides (Inbound RPCs from Transport) ---

    @Override
    public CompletableFuture<RequestVoteResponse> handleRequestVote(RequestVoteRequest request) {
        if (!running.get()) {
            return CompletableFuture.failedFuture(TransportException.nodeStopped("Node " + nodeId + " is stopped"));
        }
        CompletableFuture<RequestVoteResponse> future = new CompletableFuture<>();
        postEvent(new VoteRequestEvent(request, future));
        return future;
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> handleAppendEntries(AppendEntriesRequest request) {
        if (!running.get()) {
            return CompletableFuture.failedFuture(TransportException.nodeStopped("Node " + nodeId + " is stopped"));
        }
        CompletableFuture<AppendEntriesResponse> future = new CompletableFuture<>();
        postEvent(new AppendEntriesEvent(request, future));
        return future;
    }

    // --- Event Queue Dispatcher (Section 107) ---

    public void postEvent(RaftEvent event) {
        if (!running.get()) {
            if (event instanceof VoteRequestEvent req) {
                req.future().completeExceptionally(TransportException.nodeStopped("Node is stopped"));
            } else if (event instanceof AppendEntriesEvent app) {
                app.future().completeExceptionally(TransportException.nodeStopped("Node is stopped"));
            } else if (event instanceof ClientWriteEvent write) {
                write.future().completeExceptionally(TransportException.nodeStopped("Node is stopped"));
            }
            return;
        }

        eventExecutor.execute(() -> {
            try {
                processEvent(event);
            } catch (Exception ex) {
                log.error("Error processing RaftEvent on node {}", nodeId, ex);
            }
        });
    }

    private void processEvent(RaftEvent event) {
        if (event instanceof VoteRequestEvent req) {
            RaftRole roleBefore = state.role();
            RequestVoteResponse response = requestVoteHandler.handleRequestVote(req.request());
            if (roleBefore == RaftRole.LEADER && state.role() == RaftRole.FOLLOWER) {
                heartbeatManager.stopHeartbeats();
                replicationManager.failPendingFutures(new NotLeaderException(state.currentLeader().orElse(null)));
            }
            req.future().complete(response);
        } else if (event instanceof VoteResponseEvent voteResp) {
            electionManager.handleVoteResponse(voteResp.fromNode(), voteResp.term(), voteResp.response());
        } else if (event instanceof AppendEntriesEvent app) {
            RaftRole roleBefore = state.role();
            AppendEntriesResponse response = appendEntriesHandler.handleAppendEntries(app.request());
            if (roleBefore == RaftRole.LEADER && state.role() == RaftRole.FOLLOWER) {
                heartbeatManager.stopHeartbeats();
                replicationManager.failPendingFutures(new NotLeaderException(state.currentLeader().orElse(null)));
            }
            app.future().complete(response);
        } else if (event instanceof AppendEntriesResponseEvent appResp) {
            if (appResp.response().term() > state.currentTerm()) {
                log.info("Leader {} stepping down: discovered higher term {} from peer {}",
                        nodeId, appResp.response().term(), appResp.fromNode());
                state.becomeFollower(appResp.response().term(), null);
                heartbeatManager.stopHeartbeats();
                replicationManager.failPendingFutures(new NotLeaderException(null));
                electionTimer.reset();
            } else if (state.role() == RaftRole.LEADER) {
                replicationManager.handleAppendEntriesResponse(appResp.fromNode(), appResp.response());
            }
        } else if (event instanceof ClientWriteEvent writeEvent) {
            replicationManager.propose(writeEvent.command(), writeEvent.future());
        } else if (event instanceof ElectionTimeoutEvent timeout) {
            if (state.role() != RaftRole.LEADER) {
                log.info("Node {} triggered election timeout for term {}", nodeId, state.currentTerm());
                electionManager.startElection();
            }
        } else if (event instanceof HeartbeatTimeoutEvent) {
            if (state.role() == RaftRole.LEADER) {
                heartbeatManager.sendHeartbeats();
            }
        }
    }

    @Override
    public void close() {
        stop();
    }

    // --- Builder ---

    public static class Builder {
        private NodeId nodeId;
        private ClusterConfiguration clusterConfig;
        private RaftTransport transport;
        private PersistentRaftState persistentState;
        private RaftLog raftLog;
        private Clock clock;
        private Scheduler scheduler;
        private Duration minElectionTimeout = Duration.ofMillis(150);
        private Duration maxElectionTimeout = Duration.ofMillis(300);
        private Duration heartbeatInterval = Duration.ofMillis(50);
        private Random random;

        public Builder nodeId(NodeId nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder clusterConfig(ClusterConfiguration clusterConfig) {
            this.clusterConfig = clusterConfig;
            return this;
        }

        public Builder transport(RaftTransport transport) {
            this.transport = transport;
            return this;
        }

        public Builder persistentState(PersistentRaftState persistentState) {
            this.persistentState = persistentState;
            return this;
        }

        public Builder raftLog(RaftLog raftLog) {
            this.raftLog = raftLog;
            return this;
        }

        public Builder clock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder scheduler(Scheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        public Builder minElectionTimeout(Duration timeout) {
            this.minElectionTimeout = timeout;
            return this;
        }

        public Builder maxElectionTimeout(Duration timeout) {
            this.maxElectionTimeout = timeout;
            return this;
        }

        public Builder heartbeatInterval(Duration interval) {
            this.heartbeatInterval = interval;
            return this;
        }

        public Builder random(Random random) {
            this.random = random;
            return this;
        }

        public RaftNode build() {
            return new RaftNode(
                    nodeId, clusterConfig, transport, persistentState, raftLog,
                    clock, scheduler, minElectionTimeout, maxElectionTimeout,
                    heartbeatInterval, random
            );
        }
    }
}
