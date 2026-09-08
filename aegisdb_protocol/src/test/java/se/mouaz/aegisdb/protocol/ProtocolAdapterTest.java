package se.mouaz.aegisdb.protocol;

import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.protocol.pb.AppendEntriesArgs;
import se.mouaz.aegisdb.protocol.pb.AppendEntriesReply;
import se.mouaz.aegisdb.protocol.pb.RequestVoteArgs;
import se.mouaz.aegisdb.protocol.pb.RequestVoteReply;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class ProtocolAdapterTest {

    @Test
    void testRequestVoteConversion() {
        RequestVoteRequest domainReq = new RequestVoteRequest(NodeId.of("candidate-1"), 3, 42, 2);
        RequestVoteArgs protoArgs = ProtocolAdapter.toProto(domainReq);

        assertThat(protoArgs.getCandidateId()).isEqualTo("candidate-1");
        assertThat(protoArgs.getTerm()).isEqualTo(3);
        assertThat(protoArgs.getLastLogIndex()).isEqualTo(42);
        assertThat(protoArgs.getLastLogTerm()).isEqualTo(2);

        RequestVoteRequest roundtripReq = ProtocolAdapter.fromProto(protoArgs);
        assertThat(roundtripReq).isEqualTo(domainReq);

        RequestVoteResponse domainResp = RequestVoteResponse.granted(3);
        RequestVoteReply protoReply = ProtocolAdapter.toProto(domainResp);

        assertThat(protoReply.getTerm()).isEqualTo(3);
        assertThat(protoReply.getVoteGranted()).isTrue();

        RequestVoteResponse roundtripResp = ProtocolAdapter.fromProto(protoReply);
        assertThat(roundtripResp).isEqualTo(domainResp);
    }

    @Test
    void testAppendEntriesConversion() {
        byte[] payload = "test-log-entry".getBytes(StandardCharsets.UTF_8);
        AppendEntriesRequest domainReq = new AppendEntriesRequest(5, NodeId.of("leader-1"), 10, 4, payload, 9);
        AppendEntriesArgs protoArgs = ProtocolAdapter.toProto(domainReq);

        assertThat(protoArgs.getTerm()).isEqualTo(5);
        assertThat(protoArgs.getLeaderId()).isEqualTo("leader-1");
        assertThat(protoArgs.getEntries().toByteArray()).isEqualTo(payload);

        AppendEntriesRequest roundtripReq = ProtocolAdapter.fromProto(protoArgs);
        assertThat(roundtripReq).isEqualTo(domainReq);

        AppendEntriesResponse domainResp = AppendEntriesResponse.success(5, 11);
        AppendEntriesReply protoReply = ProtocolAdapter.toProto(domainResp);

        assertThat(protoReply.getSuccess()).isTrue();
        assertThat(protoReply.getMatchIndex()).isEqualTo(11);

        AppendEntriesResponse roundtripResp = ProtocolAdapter.fromProto(protoReply);
        assertThat(roundtripResp).isEqualTo(domainResp);
    }

    @Test
    void testInstallSnapshotConversion() {
        byte[] snapshotData = "snapshot-state-machine-bytes".getBytes(StandardCharsets.UTF_8);
        InstallSnapshotRequest domainReq = new InstallSnapshotRequest(
                4, NodeId.of("leader-1"), 100, 3, 0, snapshotData, true);
        se.mouaz.aegisdb.protocol.pb.InstallSnapshotArgs protoArgs = ProtocolAdapter.toProto(domainReq);

        assertThat(protoArgs.getTerm()).isEqualTo(4);
        assertThat(protoArgs.getLeaderId()).isEqualTo("leader-1");
        assertThat(protoArgs.getLastIncludedIndex()).isEqualTo(100);
        assertThat(protoArgs.getLastIncludedTerm()).isEqualTo(3);
        assertThat(protoArgs.getOffset()).isEqualTo(0);
        assertThat(protoArgs.getData().toByteArray()).isEqualTo(snapshotData);
        assertThat(protoArgs.getDone()).isTrue();

        InstallSnapshotRequest roundtripReq = ProtocolAdapter.fromProto(protoArgs);
        assertThat(roundtripReq).isEqualTo(domainReq);

        InstallSnapshotResponse domainResp = InstallSnapshotResponse.success(4);
        se.mouaz.aegisdb.protocol.pb.InstallSnapshotReply protoReply = ProtocolAdapter.toProto(domainResp);

        assertThat(protoReply.getTerm()).isEqualTo(4);
        assertThat(protoReply.getSuccess()).isTrue();

        InstallSnapshotResponse roundtripResp = ProtocolAdapter.fromProto(protoReply);
        assertThat(roundtripResp).isEqualTo(domainResp);
    }
}
