package se.mouaz.jdistdb.protocol.pb;

import io.grpc.BindableService;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.MethodDescriptor;
import io.grpc.ServerServiceDefinition;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.AbstractAsyncStub;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;

import static io.grpc.MethodDescriptor.generateFullMethodName;

public final class RaftRpcServiceGrpc {

    private RaftRpcServiceGrpc() {}

    public static final String SERVICE_NAME = "se.mouaz.jdistdb.protocol.RaftRpcService";

    private static volatile MethodDescriptor<RequestVoteArgs, RequestVoteReply> getRequestVoteMethod;

    public static MethodDescriptor<RequestVoteArgs, RequestVoteReply> getRequestVoteMethod() {
        MethodDescriptor<RequestVoteArgs, RequestVoteReply> method = getRequestVoteMethod;
        if (method == null) {
            synchronized (RaftRpcServiceGrpc.class) {
                method = getRequestVoteMethod;
                if (method == null) {
                    getRequestVoteMethod = method = MethodDescriptor.<RequestVoteArgs, RequestVoteReply>newBuilder()
                            .setType(MethodDescriptor.MethodType.UNARY)
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "RequestVote"))
                            .setSampledToLocalTracing(true)
                            .setRequestMarshaller(ProtoUtils.marshaller(RequestVoteArgs.getDefaultInstance()))
                            .setResponseMarshaller(ProtoUtils.marshaller(RequestVoteReply.getDefaultInstance()))
                            .build();
                }
            }
        }
        return method;
    }

    private static volatile MethodDescriptor<AppendEntriesArgs, AppendEntriesReply> getAppendEntriesMethod;

    public static MethodDescriptor<AppendEntriesArgs, AppendEntriesReply> getAppendEntriesMethod() {
        MethodDescriptor<AppendEntriesArgs, AppendEntriesReply> method = getAppendEntriesMethod;
        if (method == null) {
            synchronized (RaftRpcServiceGrpc.class) {
                method = getAppendEntriesMethod;
                if (method == null) {
                    getAppendEntriesMethod = method = MethodDescriptor.<AppendEntriesArgs, AppendEntriesReply>newBuilder()
                            .setType(MethodDescriptor.MethodType.UNARY)
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AppendEntries"))
                            .setSampledToLocalTracing(true)
                            .setRequestMarshaller(ProtoUtils.marshaller(AppendEntriesArgs.getDefaultInstance()))
                            .setResponseMarshaller(ProtoUtils.marshaller(AppendEntriesReply.getDefaultInstance()))
                            .build();
                }
            }
        }
        return method;
    }

    public static RaftRpcServiceStub newStub(Channel channel) {
        return new RaftRpcServiceStub(channel);
    }

    public static abstract class RaftRpcServiceImplBase implements BindableService {
        public void requestVote(RequestVoteArgs request, StreamObserver<RequestVoteReply> responseObserver) {
            ServerCalls.asyncUnimplementedUnaryCall(getRequestVoteMethod(), responseObserver);
        }

        public void appendEntries(AppendEntriesArgs request, StreamObserver<AppendEntriesReply> responseObserver) {
            ServerCalls.asyncUnimplementedUnaryCall(getAppendEntriesMethod(), responseObserver);
        }

        @Override
        public ServerServiceDefinition bindService() {
            return ServerServiceDefinition.builder(SERVICE_NAME)
                    .addMethod(
                            getRequestVoteMethod(),
                            ServerCalls.asyncUnaryCall(
                                    (req, observer) -> requestVote(req, observer)))
                    .addMethod(
                            getAppendEntriesMethod(),
                            ServerCalls.asyncUnaryCall(
                                    (req, observer) -> appendEntries(req, observer)))
                    .build();
        }
    }

    public static final class RaftRpcServiceStub extends AbstractAsyncStub<RaftRpcServiceStub> {
        private RaftRpcServiceStub(Channel channel) {
            super(channel, CallOptions.DEFAULT);
        }

        private RaftRpcServiceStub(Channel channel, CallOptions callOptions) {
            super(channel, callOptions);
        }

        @Override
        protected RaftRpcServiceStub build(Channel channel, CallOptions callOptions) {
            return new RaftRpcServiceStub(channel, callOptions);
        }

        public void requestVote(RequestVoteArgs request, StreamObserver<RequestVoteReply> responseObserver) {
            ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getRequestVoteMethod(), getCallOptions()), request, responseObserver);
        }

        public void appendEntries(AppendEntriesArgs request, StreamObserver<AppendEntriesReply> responseObserver) {
            ClientCalls.asyncUnaryCall(
                    getChannel().newCall(getAppendEntriesMethod(), getCallOptions()), request, responseObserver);
        }
    }
}
