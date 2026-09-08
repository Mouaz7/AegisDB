package se.mouaz.aegisdb.node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.NodeConfiguration;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.common.NodeStatus;
import se.mouaz.aegisdb.protocol.AppendEntriesRequest;
import se.mouaz.aegisdb.protocol.AppendEntriesResponse;
import se.mouaz.aegisdb.protocol.InstallSnapshotRequest;
import se.mouaz.aegisdb.protocol.InstallSnapshotResponse;
import se.mouaz.aegisdb.protocol.RequestVoteRequest;
import se.mouaz.aegisdb.protocol.RequestVoteResponse;
import se.mouaz.aegisdb.raft.RaftNode;
import se.mouaz.aegisdb.storage.StorageEngine;
import se.mouaz.aegisdb.transport.RaftRequestHandler;
import se.mouaz.aegisdb.transport.RaftTransport;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class DatabaseNode implements NodeLifecycle, RaftRequestHandler {
    private static final Logger log = LoggerFactory.getLogger(DatabaseNode.class);

    private final NodeContext context;
    private final StorageEngine storageEngine;
    private final RaftNode raftNode;
    private final AtomicReference<NodeStatus> status = new AtomicReference<>(NodeStatus.STOPPED);

    public DatabaseNode(NodeContext context, StorageEngine storageEngine, RaftNode raftNode) {
        this.context = Objects.requireNonNull(context, "context cannot be null");
        this.storageEngine = storageEngine;
        this.raftNode = raftNode;
        context.transport().registerHandler(this);
    }

    public DatabaseNode(NodeContext context) {
        this(context, null, null);
    }

    public DatabaseNode(NodeConfiguration nodeConfig, ClusterConfiguration clusterConfig, RaftTransport transport) {
        this(new NodeContext(nodeConfig, clusterConfig, transport), null, null);
    }

    public DatabaseNode(NodeConfiguration nodeConfig, ClusterConfiguration clusterConfig, RaftTransport transport,
                        StorageEngine storageEngine, RaftNode raftNode) {
        this(new NodeContext(nodeConfig, clusterConfig, transport), storageEngine, raftNode);
    }

    public NodeId nodeId() {
        return context.nodeId();
    }

    public NodeConfiguration config() {
        return context.config();
    }

    public ClusterConfiguration clusterConfig() {
        return context.clusterConfig();
    }

    public RaftTransport transport() {
        return context.transport();
    }

    public Optional<StorageEngine> storageEngine() {
        return Optional.ofNullable(storageEngine);
    }

    public Optional<RaftNode> raftNode() {
        return Optional.ofNullable(raftNode);
    }

    @Override
    public NodeStatus status() {
        return status.get();
    }

    @Override
    public synchronized void start() throws Exception {
        if (!status.compareAndSet(NodeStatus.STOPPED, NodeStatus.STARTING)) {
            log.warn("Node {} is already in state {}", nodeId(), status.get());
            return;
        }

        log.info("Starting node {}", nodeId());
        try {
            context.transport().start();
            if (raftNode != null) {
                raftNode.start();
            }
            status.set(NodeStatus.RUNNING);
            log.info("Node {} is now RUNNING", nodeId());
        } catch (Exception e) {
            status.set(NodeStatus.STOPPED);
            log.error("Failed to start node {}", nodeId(), e);
            throw e;
        }
    }

    @Override
    public synchronized void stop() {
        NodeStatus current = status.get();
        if (current == NodeStatus.STOPPED || current == NodeStatus.STOPPING) {
            return;
        }

        status.set(NodeStatus.STOPPING);
        log.info("Stopping node {}", nodeId());
        try {
            if (raftNode != null) {
                raftNode.stop();
            }
            context.transport().stop();
            if (storageEngine != null) {
                try {
                    storageEngine.close();
                } catch (Exception e) {
                    log.error("Failed to close StorageEngine for node {}", nodeId(), e);
                }
            }
        } finally {
            status.set(NodeStatus.STOPPED);
            log.info("Node {} is now STOPPED", nodeId());
        }
    }

    public CompletableFuture<RequestVoteResponse> sendRequestVote(NodeId destination, RequestVoteRequest request) {
        return context.transport().requestVote(destination, request);
    }

    public CompletableFuture<AppendEntriesResponse> sendAppendEntries(NodeId destination, AppendEntriesRequest request) {
        return context.transport().appendEntries(destination, request);
    }

    public CompletableFuture<byte[]> executeClientCommand(byte[] command) {
        if (raftNode != null) {
            return raftNode.executeClientCommand(command);
        }
        return CompletableFuture.failedFuture(new IllegalStateException("RaftNode is not initialized on node " + nodeId()));
    }

    public byte[] takeSnapshot() {
        if (raftNode != null) {
            return raftNode.takeSnapshot();
        }
        throw new IllegalStateException("RaftNode is not initialized on node " + nodeId());
    }

    public byte[] takeSnapshot(long lastIncludedIndex, long lastIncludedTerm) {
        if (raftNode != null) {
            return raftNode.takeSnapshot(lastIncludedIndex, lastIncludedTerm);
        }
        throw new IllegalStateException("RaftNode is not initialized on node " + nodeId());
    }

    @Override
    public CompletableFuture<RequestVoteResponse> handleRequestVote(RequestVoteRequest request) {
        if (raftNode != null) {
            return raftNode.handleRequestVote(request);
        }
        log.debug("Node {} received RequestVote from {}", nodeId(), request.candidateId());
        return CompletableFuture.completedFuture(RequestVoteResponse.granted(request.term()));
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> handleAppendEntries(AppendEntriesRequest request) {
        if (raftNode != null) {
            return raftNode.handleAppendEntries(request);
        }
        log.debug("Node {} received AppendEntries from leader {}", nodeId(), request.leaderId());
        return CompletableFuture.completedFuture(AppendEntriesResponse.success(request.term(), 1));
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> handleInstallSnapshot(InstallSnapshotRequest request) {
        if (raftNode != null) {
            return raftNode.handleInstallSnapshot(request);
        }
        log.debug("Node {} received InstallSnapshot from leader {}", nodeId(), request.leaderId());
        return CompletableFuture.completedFuture(new InstallSnapshotResponse(request.term(), true));
    }
}
