package se.mouaz.jdistdb.node;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.jdistdb.common.ClusterConfiguration;
import se.mouaz.jdistdb.common.NodeConfiguration;
import se.mouaz.jdistdb.common.NodeId;
import se.mouaz.jdistdb.common.NodeStatus;
import se.mouaz.jdistdb.protocol.AppendEntriesRequest;
import se.mouaz.jdistdb.protocol.AppendEntriesResponse;
import se.mouaz.jdistdb.protocol.RequestVoteRequest;
import se.mouaz.jdistdb.protocol.RequestVoteResponse;
import se.mouaz.jdistdb.transport.RaftRequestHandler;
import se.mouaz.jdistdb.transport.RaftTransport;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class DatabaseNode implements NodeLifecycle, RaftRequestHandler {
    private static final Logger log = LoggerFactory.getLogger(DatabaseNode.class);

    private final NodeContext context;
    private final AtomicReference<NodeStatus> status = new AtomicReference<>(NodeStatus.STOPPED);

    public DatabaseNode(NodeContext context) {
        this.context = Objects.requireNonNull(context, "context cannot be null");
        context.transport().registerHandler(this);
    }

    public DatabaseNode(NodeConfiguration nodeConfig, ClusterConfiguration clusterConfig, RaftTransport transport) {
        this(new NodeContext(nodeConfig, clusterConfig, transport));
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
            context.transport().stop();
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

    @Override
    public CompletableFuture<RequestVoteResponse> handleRequestVote(RequestVoteRequest request) {
        log.debug("Node {} received RequestVote from {}", nodeId(), request.candidateId());
        return CompletableFuture.completedFuture(RequestVoteResponse.granted(request.term()));
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> handleAppendEntries(AppendEntriesRequest request) {
        log.debug("Node {} received AppendEntries from leader {}", nodeId(), request.leaderId());
        return CompletableFuture.completedFuture(AppendEntriesResponse.success(request.term(), 1));
    }
}
