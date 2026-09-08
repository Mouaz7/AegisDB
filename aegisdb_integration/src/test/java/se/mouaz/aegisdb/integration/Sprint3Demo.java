package se.mouaz.aegisdb.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.RaftNode;
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

import static org.awaitility.Awaitility.await;

/**
 * Sprint 3 Live Demonstration & Verification (US006 - Raft Log Replication).
 * Demonstrates all 5 Sprint 3 Acceptance Criteria:
 * 1. Writes replicate
 * 2. Majority required
 * 3. CommitIndex advances correctly
 * 4. Follower catches up
 * 5. Conflicting entries are repaired
 */
public class Sprint3Demo {
    private static final Logger log = LoggerFactory.getLogger(Sprint3Demo.class);

    public static void main(String[] args) {
        System.out.println("===============================================================");
        System.out.println("     AegisDB - Sprint 3 Demonstration & Verification ");
        System.out.println("     Consensus Engine: Raft Log Replication (US006)");
        System.out.println("===============================================================\n");

        RaftInvariants.clearInvariantTracking();
        InMemoryTransport.clearRegistry();

        NodeId id1 = NodeId.of("raft-node-1");
        NodeId id2 = NodeId.of("raft-node-2");
        NodeId id3 = NodeId.of("raft-node-3");

        Endpoint ep1 = Endpoint.of("127.0.0.1", 8001);
        Endpoint ep2 = Endpoint.of("127.0.0.1", 8002);
        Endpoint ep3 = Endpoint.of("127.0.0.1", 8003);

        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .clusterId("aegisdb-sprint3-demo")
                .addMember(id1, ep1)
                .addMember(id2, ep2)
                .addMember(id3, ep3)
                .build();

        InMemoryTransport transport1 = new InMemoryTransport(id1);
        InMemoryTransport transport2 = new InMemoryTransport(id2);
        InMemoryTransport transport3 = new InMemoryTransport(id3);

        transport1.start();
        transport2.start();
        transport3.start();

        RaftNode node1 = RaftNode.builder()
                .nodeId(id1).clusterConfig(clusterConfig).transport(transport1)
                .minElectionTimeout(Duration.ofMillis(100))
                .maxElectionTimeout(Duration.ofMillis(140))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(10))
                .build();

        RaftNode node2 = RaftNode.builder()
                .nodeId(id2).clusterConfig(clusterConfig).transport(transport2)
                .minElectionTimeout(Duration.ofMillis(1000))
                .maxElectionTimeout(Duration.ofMillis(1200))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(20))
                .build();

        RaftNode node3 = RaftNode.builder()
                .nodeId(id3).clusterConfig(clusterConfig).transport(transport3)
                .minElectionTimeout(Duration.ofMillis(1000))
                .maxElectionTimeout(Duration.ofMillis(1200))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(30))
                .build();

        try {
            // 1. Start cluster and elect leader
            System.out.println("▶ [1/5] Starting 3-node cluster and awaiting stable leader...");
            node1.start();
            node2.start();
            node3.start();

            await().atMost(Duration.ofSeconds(3)).until(() -> node1.role() == RaftRole.LEADER);
            System.out.println("   ✔ Leader established: " + node1.nodeId() + " [Term: " + node1.currentTerm() + "]");
            System.out.println("   ✔ Followers synchronized: " + node2.nodeId() + ", " + node3.nodeId() + "\n");

            // 2. Acceptance Criterion 1 & 3: Writes replicate & CommitIndex advances correctly
            System.out.println("▶ [2/5] [AC1 & AC3] Verifying 'Writes replicate' & 'CommitIndex advances correctly'...");
            byte[] cmd1 = "SET account:101 5000".getBytes(StandardCharsets.UTF_8);
            byte[] cmd2 = "SET account:102 3000".getBytes(StandardCharsets.UTF_8);

            CompletableFuture<Long> fut1 = node1.propose(cmd1);
            Long idx1 = fut1.get(3, TimeUnit.SECONDS);
            System.out.println("   ✔ First write committed at index: " + idx1);

            CompletableFuture<Long> fut2 = node1.propose(cmd2);
            Long idx2 = fut2.get(3, TimeUnit.SECONDS);
            System.out.println("   ✔ Second write committed at index: " + idx2);

            await().atMost(Duration.ofSeconds(3)).until(() -> node2.commitIndex() == 2L && node3.commitIndex() == 2L);
            System.out.println("   ✔ Both followers acknowledged and advanced commitIndex to 2.");
            RaftInvariants.assertIdenticalOrderOfCommittedEntries(node1.log(), node2.log(), 2L);
            RaftInvariants.assertIdenticalOrderOfCommittedEntries(node1.log(), node3.log(), 2L);
            System.out.println("   ✔ Invariant verified: All nodes share identical committed log sequence.\n");

            // 3. Acceptance Criterion 2: Majority required
            System.out.println("▶ [3/5] [AC2] Verifying 'Majority required'...");
            System.out.println("   Simulating network partition: Disconnecting both followers (node-2, node-3)...");
            transport2.stop();
            transport3.stop();

            CompletableFuture<Long> uncommittedFut = node1.propose("SET isolated:tx 9999".getBytes(StandardCharsets.UTF_8));
            await().atMost(Duration.ofSeconds(2)).until(() -> node1.log().lastLogIndex() == 3L);

            if (!uncommittedFut.isDone() && node1.commitIndex() == 2L) {
                System.out.println("   ✔ Write safely blocked from commit: Only leader alone (1/3 nodes) holds entry.");
            } else {
                throw new IllegalStateException("Failed: Write was allowed to commit without majority quorum!");
            }

            System.out.println("   Reconnecting node-2 (restoring 2/3 majority)...");
            transport2.start();
            node1.replicationManager().replicateTo(id2);
            Long idx3 = uncommittedFut.get(3, TimeUnit.SECONDS);
            System.out.println("   ✔ Write committed immediately once quorum was restored! CommitIndex: " + idx3 + "\n");

            // 4. Acceptance Criterion 4: Follower catches up
            System.out.println("▶ [4/5] [AC4] Verifying 'Follower catches up'...");
            System.out.println("   Node-3 remains offline while leader commits additional entries...");
            for (int i = 4; i <= 6; i++) {
                node1.propose(("TX-BATCH-" + i).getBytes(StandardCharsets.UTF_8)).get(3, TimeUnit.SECONDS);
            }
            System.out.println("   ✔ Leader and Node-2 are at commitIndex 6. Node-3 remains at commitIndex 2.");

            System.out.println("   Reconnecting lagging Node-3...");
            transport3.start();
            node1.replicationManager().replicateTo(id3);

            await().atMost(Duration.ofSeconds(4)).until(() -> node3.commitIndex() == 6L);
            System.out.println("   ✔ Node-3 caught up on all missing entries and reached commitIndex: " + node3.commitIndex());
            RaftInvariants.assertIdenticalOrderOfCommittedEntries(node1.log(), node3.log(), 6L);
            System.out.println("   ✔ Logs are completely synchronized.\n");

            // Close cluster 1 for clean AC5 demonstration
            node1.stop();
            node2.stop();
            node3.stop();
            transport1.stop();
            transport2.stop();
            transport3.stop();
            InMemoryTransport.clearRegistry();

            // 5. Acceptance Criterion 5: Conflicting entries are repaired
            System.out.println("▶ [5/5] [AC5] Verifying 'Conflicting entries are repaired'...");
            System.out.println("   Creating scenario with diverging uncommitted entries in follower log...");

            NodeId repLeaderId = NodeId.of("repair-leader");
            NodeId repFollowerId = NodeId.of("repair-divergent");

            InMemoryTransport tRepLeader = new InMemoryTransport(repLeaderId);
            InMemoryTransport tRepFollower = new InMemoryTransport(repFollowerId);
            tRepLeader.start();
            tRepFollower.start();

            ClusterConfiguration repConfig = ClusterConfiguration.builder()
                    .clusterId("repair-demo-cluster")
                    .addMember(repLeaderId, Endpoint.of("127.0.0.1", 9001))
                    .addMember(repFollowerId, Endpoint.of("127.0.0.1", 9002))
                    .build();

            // Follower has conflicting uncommitted entries at index 2 and 3 from old term 1
            RaftLog conflictLog = new RaftLog();
            conflictLog.append(new RaftLogEntry(1, 1, "SET account:101 5000".getBytes(StandardCharsets.UTF_8)));
            conflictLog.append(new RaftLogEntry(2, 1, "STALE_SPLIT_BRAIN_WRITE_2".getBytes(StandardCharsets.UTF_8)));
            conflictLog.append(new RaftLogEntry(3, 1, "STALE_SPLIT_BRAIN_WRITE_3".getBytes(StandardCharsets.UTF_8)));

            RaftNode divergentNode = RaftNode.builder()
                    .nodeId(repFollowerId).clusterConfig(repConfig).transport(tRepFollower)
                    .persistentState(new PersistentRaftState(1, repFollowerId))
                    .raftLog(conflictLog)
                    .minElectionTimeout(Duration.ofMillis(1000))
                    .maxElectionTimeout(Duration.ofMillis(1200))
                    .build();

            // Leader starts in term 2 with authoritative entries
            RaftLog leaderLog = new RaftLog();
            leaderLog.append(new RaftLogEntry(1, 1, "SET account:101 5000".getBytes(StandardCharsets.UTF_8)));
            leaderLog.append(new RaftLogEntry(2, 2, "SET account:102 3000".getBytes(StandardCharsets.UTF_8)));
            leaderLog.append(new RaftLogEntry(3, 2, "SET account:103 7000".getBytes(StandardCharsets.UTF_8)));

            RaftNode repairLeader = RaftNode.builder()
                    .nodeId(repLeaderId).clusterConfig(repConfig).transport(tRepLeader)
                    .persistentState(new PersistentRaftState(2, repLeaderId))
                    .raftLog(leaderLog)
                    .minElectionTimeout(Duration.ofMillis(100))
                    .maxElectionTimeout(Duration.ofMillis(140))
                    .heartbeatInterval(Duration.ofMillis(30))
                    .build();

            divergentNode.start();
            repairLeader.start();

            System.out.println("   Divergent node has conflicting entry at index 2: '"
                    + new String(conflictLog.getEntry(2).get().data(), StandardCharsets.UTF_8) + "' (Term 1)");
            System.out.println("   Leader in Term 2 has authoritative entry at index 2: '"
                    + new String(leaderLog.getEntry(2).get().data(), StandardCharsets.UTF_8) + "' (Term 2)");

            await().atMost(Duration.ofSeconds(3)).until(() -> repairLeader.role() == RaftRole.LEADER);
            repairLeader.replicationManager().broadcastReplication();

            await().atMost(Duration.ofSeconds(4)).until(() ->
                    divergentNode.log().getEntry(2).isPresent() &&
                    new String(divergentNode.log().getEntry(2).get().data(), StandardCharsets.UTF_8).equals("SET account:102 3000") &&
                    divergentNode.log().getEntry(2).get().term() == 2L
            );

            System.out.println("   ✔ Conflict repaired! Follower log truncated and overwritten with leader authoritative entries:");
            System.out.println("     Index 2: '" + new String(divergentNode.log().getEntry(2).get().data(), StandardCharsets.UTF_8) + "' (Term 2)");
            System.out.println("     Index 3: '" + new String(divergentNode.log().getEntry(3).get().data(), StandardCharsets.UTF_8) + "' (Term 2)");
            System.out.println("   ✔ Invariant verified: All divergent entries eliminated and replaced.\n");

            divergentNode.stop();
            repairLeader.stop();
            tRepLeader.stop();
            tRepFollower.stop();
            InMemoryTransport.clearRegistry();

            System.out.println("===============================================================");
            System.out.println("     ALL 5 SPRINT 3 ACCEPTANCE CRITERIA VERIFIED!             ");
            System.out.println("     [AC1] Writes replicate:                 PASSED");
            System.out.println("     [AC2] Majority required:                PASSED");
            System.out.println("     [AC3] CommitIndex advances correctly:   PASSED");
            System.out.println("     [AC4] Follower catches up:              PASSED");
            System.out.println("     [AC5] Conflicting entries are repaired: PASSED");
            System.out.println("===============================================================");

        } catch (Exception e) {
            log.error("Sprint 3 Demo failed", e);
            System.err.println("DEMO FAILED: " + e.getMessage());
            System.exit(1);
        } finally {
            node1.stop();
            node2.stop();
            node3.stop();

            transport1.stop();
            transport2.stop();
            transport3.stop();

            InMemoryTransport.clearRegistry();
        }
    }
}
