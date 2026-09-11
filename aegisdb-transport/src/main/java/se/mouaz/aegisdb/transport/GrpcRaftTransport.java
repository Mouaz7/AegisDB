package se.mouaz.aegisdb.transport;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.protocol.AppendEntriesRequest;
import se.mouaz.aegisdb.protocol.AppendEntriesResponse;
import se.mouaz.aegisdb.protocol.InstallSnapshotRequest;
import se.mouaz.aegisdb.protocol.InstallSnapshotResponse;
import se.mouaz.aegisdb.protocol.ProtocolAdapter;
import se.mouaz.aegisdb.protocol.RequestVoteRequest;
import se.mouaz.aegisdb.protocol.RequestVoteResponse;
import se.mouaz.aegisdb.protocol.pb.AppendEntriesArgs;
import se.mouaz.aegisdb.protocol.pb.AppendEntriesReply;
import se.mouaz.aegisdb.protocol.pb.InstallSnapshotArgs;
import se.mouaz.aegisdb.protocol.pb.InstallSnapshotReply;
import se.mouaz.aegisdb.protocol.pb.RaftRpcServiceGrpc;
import se.mouaz.aegisdb.protocol.pb.RequestVoteArgs;
import se.mouaz.aegisdb.protocol.pb.RequestVoteReply;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class GrpcRaftTransport implements RaftTransport {
    private static final Logger log = LoggerFactory.getLogger(GrpcRaftTransport.class);

    private final NodeId nodeId;
    private final Endpoint endpoint;
    private final ClusterConfiguration clusterConfig;
    private final GrpcChannelManager channelManager;
    private final Duration requestTimeout;

    private Server server;
    private volatile RaftRequestHandler handler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public GrpcRaftTransport(NodeId nodeId,
                             Endpoint endpoint,
                             ClusterConfiguration clusterConfig,
                             Duration requestTimeout) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId cannot be null");
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint cannot be null");
        this.clusterConfig = Objects.requireNonNull(clusterConfig, "clusterConfig cannot be null");
        this.channelManager = new GrpcChannelManager();
        this.requestTimeout = requestTimeout != null ? requestTimeout : Duration.ofSeconds(2);
    }

    @Override
    public NodeId localNodeId() {
        return nodeId;
    }

    @Override
    public void registerHandler(RaftRequestHandler handler) {
        this.handler = Objects.requireNonNull(handler, "handler cannot be null");
    }

    @Override
    public void start() throws IOException {
        if (running.compareAndSet(false, true)) {
            server = ServerBuilder.forPort(endpoint.port())
                    .addService(new RaftRpcServiceImpl())
                    .build()
                    .start();
            log.info("gRPC Raft transport server started on port {} for node {}", endpoint.port(), nodeId);
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            log.info("Stopping gRPC Raft transport for node {}", nodeId);
            if (server != null) {
                server.shutdown();
                try {
                    if (!server.awaitTermination(2, TimeUnit.SECONDS)) {
                        server.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    server.shutdownNow();
                }
            }
            channelManager.close();
            log.info("gRPC Raft transport stopped for node {}", nodeId);
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    @Override
    public CompletableFuture<RequestVoteResponse> requestVote(NodeId destination, RequestVoteRequest request) {
        if (!running.get()) {
            return CompletableFuture.failedFuture(
                    TransportException.nodeStopped("Transport is stopped on node " + nodeId));
        }

        Endpoint targetEndpoint = clusterConfig.getEndpoint(destination)
                .orElse(null);
        if (targetEndpoint == null) {
            return CompletableFuture.failedFuture(
                    TransportException.nodeNotFound("Unknown destination node: " + destination));
        }

        CompletableFuture<RequestVoteResponse> future = new CompletableFuture<>();
        try {
            ManagedChannel channel = channelManager.getOrCreateChannel(destination, targetEndpoint);
            RaftRpcServiceGrpc.RaftRpcServiceStub stub = RaftRpcServiceGrpc.newStub(channel)
                    .withDeadlineAfter(requestTimeout.toMillis(), TimeUnit.MILLISECONDS);

            RequestVoteArgs args = ProtocolAdapter.toProto(request);
            stub.requestVote(args, new StreamObserver<>() {
                @Override
                public void onNext(RequestVoteReply reply) {
                    future.complete(ProtocolAdapter.fromProto(reply));
                }

                @Override
                public void onError(Throwable t) {
                    future.completeExceptionally(mapGrpcException("RequestVote", destination, t));
                }

                @Override
                public void onCompleted() {
                    // Handled in onNext
                }
            });
        } catch (Exception ex) {
            future.completeExceptionally(TransportException.networkError("Failed to dispatch RequestVote to " + destination, ex));
        }

        return future;
    }

    @Override
    public CompletableFuture<AppendEntriesResponse> appendEntries(NodeId destination, AppendEntriesRequest request) {
        if (!running.get()) {
            return CompletableFuture.failedFuture(
                    TransportException.nodeStopped("Transport is stopped on node " + nodeId));
        }

        Endpoint targetEndpoint = clusterConfig.getEndpoint(destination)
                .orElse(null);
        if (targetEndpoint == null) {
            return CompletableFuture.failedFuture(
                    TransportException.nodeNotFound("Unknown destination node: " + destination));
        }

        CompletableFuture<AppendEntriesResponse> future = new CompletableFuture<>();
        try {
            ManagedChannel channel = channelManager.getOrCreateChannel(destination, targetEndpoint);
            RaftRpcServiceGrpc.RaftRpcServiceStub stub = RaftRpcServiceGrpc.newStub(channel)
                    .withDeadlineAfter(requestTimeout.toMillis(), TimeUnit.MILLISECONDS);

            AppendEntriesArgs args = ProtocolAdapter.toProto(request);
            stub.appendEntries(args, new StreamObserver<>() {
                @Override
                public void onNext(AppendEntriesReply reply) {
                    future.complete(ProtocolAdapter.fromProto(reply));
                }

                @Override
                public void onError(Throwable t) {
                    future.completeExceptionally(mapGrpcException("AppendEntries", destination, t));
                }

                @Override
                public void onCompleted() {
                    // Handled in onNext
                }
            });
        } catch (Exception ex) {
            future.completeExceptionally(TransportException.networkError("Failed to dispatch AppendEntries to " + destination, ex));
        }

        return future;
    }

    @Override
    public CompletableFuture<InstallSnapshotResponse> installSnapshot(NodeId destination, InstallSnapshotRequest request) {
        if (!running.get()) {
            return CompletableFuture.failedFuture(
                    TransportException.nodeStopped("Transport is stopped on node " + nodeId));
        }

        Endpoint targetEndpoint = clusterConfig.getEndpoint(destination)
                .orElse(null);
        if (targetEndpoint == null) {
            return CompletableFuture.failedFuture(
                    TransportException.nodeNotFound("Unknown destination node: " + destination));
        }

        CompletableFuture<InstallSnapshotResponse> future = new CompletableFuture<>();
        try {
            ManagedChannel channel = channelManager.getOrCreateChannel(destination, targetEndpoint);
            RaftRpcServiceGrpc.RaftRpcServiceStub stub = RaftRpcServiceGrpc.newStub(channel)
                    .withDeadlineAfter(requestTimeout.toMillis(), TimeUnit.MILLISECONDS);

            InstallSnapshotArgs args = ProtocolAdapter.toProto(request);
            stub.installSnapshot(args, new StreamObserver<>() {
                @Override
                public void onNext(InstallSnapshotReply reply) {
                    future.complete(ProtocolAdapter.fromProto(reply));
                }

                @Override
                public void onError(Throwable t) {
                    future.completeExceptionally(mapGrpcException("InstallSnapshot", destination, t));
                }

                @Override
                public void onCompleted() {
                    // Handled in onNext
                }
            });
        } catch (Exception ex) {
            future.completeExceptionally(TransportException.networkError("Failed to dispatch InstallSnapshot to " + destination, ex));
        }

        return future;
    }

    private Throwable mapGrpcException(String rpc, NodeId dest, Throwable t) {
        Status status = Status.fromThrowable(t);
        if (status.getCode() == Status.Code.DEADLINE_EXCEEDED) {
            return TransportException.timeout("gRPC " + rpc + " to " + dest + " timed out after " + requestTimeout.toMillis() + " ms");
        }
        if (status.getCode() == Status.Code.UNAVAILABLE) {
            return TransportException.networkError("gRPC node " + dest + " is unavailable", t);
        }
        return TransportException.networkError("gRPC " + rpc + " failed with status " + status, t);
    }

    private class RaftRpcServiceImpl extends RaftRpcServiceGrpc.RaftRpcServiceImplBase {
        @Override
        public void requestVote(RequestVoteArgs request, StreamObserver<RequestVoteReply> responseObserver) {
            if (!running.get() || handler == null) {
                responseObserver.onError(Status.UNAVAILABLE
                        .withDescription("Node " + nodeId + " is not ready or stopped")
                        .asRuntimeException());
                return;
            }

            RequestVoteRequest domainReq = ProtocolAdapter.fromProto(request);
            handler.handleRequestVote(domainReq).whenComplete((domainResp, ex) -> {
                if (ex != null) {
                    responseObserver.onError(Status.INTERNAL
                            .withDescription(ex.getMessage())
                            .asRuntimeException());
                } else {
                    responseObserver.onNext(ProtocolAdapter.toProto(domainResp));
                    responseObserver.onCompleted();
                }
            });
        }

        @Override
        public void appendEntries(AppendEntriesArgs request, StreamObserver<AppendEntriesReply> responseObserver) {
            if (!running.get() || handler == null) {
                responseObserver.onError(Status.UNAVAILABLE
                        .withDescription("Node " + nodeId + " is not ready or stopped")
                        .asRuntimeException());
                return;
            }

            AppendEntriesRequest domainReq = ProtocolAdapter.fromProto(request);
            handler.handleAppendEntries(domainReq).whenComplete((domainResp, ex) -> {
                if (ex != null) {
                    responseObserver.onError(Status.INTERNAL
                            .withDescription(ex.getMessage())
                            .asRuntimeException());
                } else {
                    responseObserver.onNext(ProtocolAdapter.toProto(domainResp));
                    responseObserver.onCompleted();
                }
            });
        }

        @Override
        public void installSnapshot(InstallSnapshotArgs request, StreamObserver<InstallSnapshotReply> responseObserver) {
            if (!running.get() || handler == null) {
                responseObserver.onError(Status.UNAVAILABLE
                        .withDescription("Node " + nodeId + " is not ready or stopped")
                        .asRuntimeException());
                return;
            }

            InstallSnapshotRequest domainReq = ProtocolAdapter.fromProto(request);
            handler.handleInstallSnapshot(domainReq).whenComplete((domainResp, ex) -> {
                if (ex != null) {
                    responseObserver.onError(Status.INTERNAL
                            .withDescription(ex.getMessage())
                            .asRuntimeException());
                } else {
                    responseObserver.onNext(ProtocolAdapter.toProto(domainResp));
                    responseObserver.onCompleted();
                }
            });
        }
    }
}
