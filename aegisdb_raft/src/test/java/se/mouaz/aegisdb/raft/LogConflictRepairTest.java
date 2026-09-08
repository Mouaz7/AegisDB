package se.mouaz.aegisdb.raft;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.log.RaftLog;
import se.mouaz.aegisdb.raft.log.RaftLogEntry;
import se.mouaz.aegisdb.raft.state.PersistentRaftState;
import se.mouaz.aegisdb.raft.state.RaftInvariants;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@DisplayName("Log Conflict Repair Test - Acceptance Criterion 5 (US006)")
class LogConflictRepairTest {

    private RaftNode leaderNode;
    private RaftNode followerNode;
    private RaftNode conflictingNode;

    private NodeId leaderId;
    private NodeId followerId;
    private NodeId conflictId;

    private InMemoryTransport leaderTransport;
    private InMemoryTransport followerTransport;
    private InMemoryTransport conflictTransport;

    @BeforeEach
    void setUp() {
        RaftInvariants.clearInvariantTracking();
        InMemoryTransport.clearRegistry();

        leaderId = NodeId.of("repair-leader-1");
        followerId = NodeId.of("repair-follower-2");
        conflictId = NodeId.of("repair-conflict-3");

        Endpoint ep1 = Endpoint.of("127.0.0.1", 7401);
        Endpoint ep2 = Endpoint.of("127.0.0.1", 7402);
        Endpoint ep3 = Endpoint.of("127.0.0.1", 7403);

        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .clusterId("repair-cluster")
                .addMember(leaderId, ep1)
                .addMember(followerId, ep2)
                .addMember(conflictId, ep3)
                .build();

        leaderTransport = new InMemoryTransport(leaderId);
        followerTransport = new InMemoryTransport(followerId);
        conflictTransport = new InMemoryTransport(conflictId);

        leaderTransport.start();
        followerTransport.start();
        conflictTransport.start();

        // 1. Setup conflicting node with divergent uncommitted log:
        // Entry 1 (term 1), Entry 2 (term 1, conflicting payload), Entry 3 (term 1, conflicting payload)
        RaftLog conflictLog = new RaftLog();
        conflictLog.append(new RaftLogEntry(1, 1, "shared_cmd".getBytes(StandardCharsets.UTF_8)));
        conflictLog.append(new RaftLogEntry(2, 1, "stale_cmd_2".getBytes(StandardCharsets.UTF_8)));
        conflictLog.append(new RaftLogEntry(3, 1, "stale_cmd_3".getBytes(StandardCharsets.UTF_8)));

        PersistentRaftState pConflict = new PersistentRaftState(1, conflictId);

        conflictingNode = RaftNode.builder()
                .nodeId(conflictId).clusterConfig(clusterConfig).transport(conflictTransport)
                .persistentState(pConflict)
                .raftLog(conflictLog)
                .minElectionTimeout(Duration.ofMillis(800))
                .maxElectionTimeout(Duration.ofMillis(900))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(44))
                .build();

        // Leader starts in term 2
        PersistentRaftState pLeader = new PersistentRaftState(2, leaderId);
        RaftLog leaderLog = new RaftLog();
        leaderLog.append(new RaftLogEntry(1, 1, "shared_cmd".getBytes(StandardCharsets.UTF_8)));

        leaderNode = RaftNode.builder()
                .nodeId(leaderId).clusterConfig(clusterConfig).transport(leaderTransport)
                .persistentState(pLeader)
                .raftLog(leaderLog)
                .minElectionTimeout(Duration.ofMillis(80))
                .maxElectionTimeout(Duration.ofMillis(120))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(42))
                .build();

        PersistentRaftState pFollower = new PersistentRaftState(2, null);
        RaftLog followerLog = new RaftLog();
        followerLog.append(new RaftLogEntry(1, 1, "shared_cmd".getBytes(StandardCharsets.UTF_8)));

        followerNode = RaftNode.builder()
                .nodeId(followerId).clusterConfig(clusterConfig).transport(followerTransport)
                .persistentState(pFollower)
                .raftLog(followerLog)
                .minElectionTimeout(Duration.ofMillis(800))
                .maxElectionTimeout(Duration.ofMillis(900))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(43))
                .build();

        leaderNode.start();
        followerNode.start();
        conflictingNode.start();
    }

    @AfterEach
    void tearDown() {
        if (leaderNode != null) leaderNode.stop();
        if (followerNode != null) followerNode.stop();
        if (conflictingNode != null) conflictingNode.stop();

        if (leaderTransport != null) leaderTransport.stop();
        if (followerTransport != null) followerTransport.stop();
        if (conflictTransport != null) conflictTransport.stop();

        InMemoryTransport.clearRegistry();
    }

    @Test
    @DisplayName("Leader detects log divergence, repairs conflict and overwrites stale follower entries")
    void repairsConflictingFollowerEntries() throws Exception {
        // 1. Leader gets elected in term 3 (or wins election)
        await().atMost(Duration.ofSeconds(3)).until(() -> leaderNode.role() == RaftRole.LEADER);

        // 2. Leader writes authoritative entries in new term
        CompletableFuture<Long> fut2 = leaderNode.propose("authoritative_cmd_2".getBytes(StandardCharsets.UTF_8));
        Long c2 = fut2.get(3, TimeUnit.SECONDS);
        assertThat(c2).isGreaterThanOrEqualTo(2L);

        CompletableFuture<Long> fut3 = leaderNode.propose("authoritative_cmd_3".getBytes(StandardCharsets.UTF_8));
        Long c3 = fut3.get(3, TimeUnit.SECONDS);
        assertThat(c3).isGreaterThanOrEqualTo(3L);

        // 3. Verify conflicting node repaired its divergence
        await().atMost(Duration.ofSeconds(4)).until(() -> conflictingNode.commitIndex() >= 3L);

        // 4. Assert conflicting node's log now has the authoritative entries instead of stale entries
        assertThat(conflictingNode.log().lastLogIndex()).isEqualTo(leaderNode.log().lastLogIndex());

        RaftLogEntry rep2 = conflictingNode.log().getEntry(2).orElseThrow();
        assertThat(new String(rep2.data(), StandardCharsets.UTF_8)).isEqualTo("authoritative_cmd_2");
        assertThat(rep2.term()).isEqualTo(leaderNode.log().getEntry(2).orElseThrow().term());

        RaftLogEntry rep3 = conflictingNode.log().getEntry(3).orElseThrow();
        assertThat(new String(rep3.data(), StandardCharsets.UTF_8)).isEqualTo("authoritative_cmd_3");
        assertThat(rep3.term()).isEqualTo(leaderNode.log().getEntry(3).orElseThrow().term());

        // 5. Verify Raft Invariant: Committed entries appear in identical order
        RaftInvariants.assertIdenticalOrderOfCommittedEntries(
                leaderNode.log(), conflictingNode.log(), conflictingNode.commitIndex()
        );
    }
}
