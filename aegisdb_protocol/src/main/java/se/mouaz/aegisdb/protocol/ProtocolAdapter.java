package se.mouaz.aegisdb.protocol;

import com.google.protobuf.ByteString;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.protocol.pb.AppendEntriesArgs;
import se.mouaz.aegisdb.protocol.pb.AppendEntriesReply;
import se.mouaz.aegisdb.protocol.pb.InstallSnapshotArgs;
import se.mouaz.aegisdb.protocol.pb.InstallSnapshotReply;
import se.mouaz.aegisdb.protocol.pb.RequestVoteArgs;
import se.mouaz.aegisdb.protocol.pb.RequestVoteReply;

public final class ProtocolAdapter {

    private ProtocolAdapter() {}

    public static RequestVoteArgs toProto(RequestVoteRequest request) {
        return RequestVoteArgs.newBuilder()
                .setCandidateId(request.candidateId().value())
                .setTerm(request.term())
                .setLastLogIndex(request.lastLogIndex())
                .setLastLogTerm(request.lastLogTerm())
                .build();
    }

    public static RequestVoteRequest fromProto(RequestVoteArgs args) {
        return new RequestVoteRequest(
                NodeId.of(args.getCandidateId()),
                args.getTerm(),
                args.getLastLogIndex(),
                args.getLastLogTerm()
        );
    }

    public static RequestVoteReply toProto(RequestVoteResponse response) {
        return RequestVoteReply.newBuilder()
                .setTerm(response.term())
                .setVoteGranted(response.voteGranted())
                .setReason(response.reason() == null ? "" : response.reason())
                .build();
    }

    public static RequestVoteResponse fromProto(RequestVoteReply reply) {
        return new RequestVoteResponse(
                reply.getTerm(),
                reply.getVoteGranted(),
                reply.getReason()
        );
    }

    public static AppendEntriesArgs toProto(AppendEntriesRequest request) {
        return AppendEntriesArgs.newBuilder()
                .setTerm(request.term())
                .setLeaderId(request.leaderId().value())
                .setPrevLogIndex(request.prevLogIndex())
                .setPrevLogTerm(request.prevLogTerm())
                .setEntries(ByteString.copyFrom(request.entries()))
                .setLeaderCommit(request.leaderCommit())
                .build();
    }

    public static AppendEntriesRequest fromProto(AppendEntriesArgs args) {
        return new AppendEntriesRequest(
                args.getTerm(),
                NodeId.of(args.getLeaderId()),
                args.getPrevLogIndex(),
                args.getPrevLogTerm(),
                args.getEntries().toByteArray(),
                args.getLeaderCommit()
        );
    }

    public static AppendEntriesReply toProto(AppendEntriesResponse response) {
        return AppendEntriesReply.newBuilder()
                .setTerm(response.term())
                .setSuccess(response.success())
                .setMatchIndex(response.matchIndex())
                .setReason(response.reason() == null ? "" : response.reason())
                .build();
    }

    public static AppendEntriesResponse fromProto(AppendEntriesReply reply) {
        return new AppendEntriesResponse(
                reply.getTerm(),
                reply.getSuccess(),
                reply.getMatchIndex(),
                reply.getReason()
        );
    }

    public static InstallSnapshotArgs toProto(InstallSnapshotRequest request) {
        return InstallSnapshotArgs.newBuilder()
                .setTerm(request.term())
                .setLeaderId(request.leaderId().value())
                .setLastIncludedIndex(request.lastIncludedIndex())
                .setLastIncludedTerm(request.lastIncludedTerm())
                .setOffset(request.offset())
                .setData(ByteString.copyFrom(request.data()))
                .setDone(request.done())
                .build();
    }

    public static InstallSnapshotRequest fromProto(InstallSnapshotArgs args) {
        return new InstallSnapshotRequest(
                args.getTerm(),
                NodeId.of(args.getLeaderId()),
                args.getLastIncludedIndex(),
                args.getLastIncludedTerm(),
                args.getOffset(),
                args.getData().toByteArray(),
                args.getDone()
        );
    }

    public static InstallSnapshotReply toProto(InstallSnapshotResponse response) {
        return InstallSnapshotReply.newBuilder()
                .setTerm(response.term())
                .setSuccess(response.success())
                .build();
    }

    public static InstallSnapshotResponse fromProto(InstallSnapshotReply reply) {
        return new InstallSnapshotResponse(
                reply.getTerm(),
                reply.getSuccess()
        );
    }
}
