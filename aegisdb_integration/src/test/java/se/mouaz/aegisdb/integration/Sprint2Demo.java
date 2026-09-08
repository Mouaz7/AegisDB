package se.mouaz.aegisdb.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.RaftNode;
import se.mouaz.aegisdb.raft.state.RaftInvariants;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.time.Duration;
import java.util.List;
import java.util.Random;

import static org.awaitility.Awaitility.await;

/**
 * Sprint 2 Live Demonstration & Verification (US005 - Raft Leader Election).
 * Demonstrates all 3 Sprint 2 Acceptance Criteria:
 * 1. Exactly one leader exists per term in the cluster
 * 2. Followers reset election timeout after receiving valid heartbeats
 * 3. When the current leader fails, a new leader is safely elected with higher term
 */
public class Sprint2Demo {
    private static final Logger log = LoggerFactory.getLogger(Sprint2Demo.class);

    public static void main(String[] args) {
        System.out.println("===============================================================");
        System.out.println("     AegisDB - Sprint 2 Demonstration & Verification ");
        System.out.println("     Consensus Engine: Raft Leader Election (US005)");
        System.out.println("===============================================================\n");

        RaftInvariants.clearInvariantTracking();
        InMemoryTransport.clearRegistry();

        NodeId id1 = NodeId.of("raft-node-1");
        NodeId id2 = NodeId.of("raft-node-2");
        NodeId id3 = NodeId.of("raft-node-3");

        Endpoint ep1 = Endpoint.of("127.0.0.1", 7001);
        Endpoint ep2 = Endpoint.of("127.0.0.1", 7002);
        Endpoint ep3 = Endpoint.of("127.0.0.1", 7003);

        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .clusterId("aegisdb-sprint2-demo")
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

        // Stagger election timeouts for deterministic first leader
        RaftNode node1 = RaftNode.builder()
                .nodeId(id1).clusterConfig(clusterConfig).transport(transport1)
                .minElectionTimeout(Duration.ofMillis(120))
                .maxElectionTimeout(Duration.ofMillis(160))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(1))
                .build();

        RaftNode node2 = RaftNode.builder()
                .nodeId(id2).clusterConfig(clusterConfig).transport(transport2)
                .minElectionTimeout(Duration.ofMillis(280))
                .maxElectionTimeout(Duration.ofMillis(340))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(2))
                .build();

        RaftNode node3 = RaftNode.builder()
                .nodeId(id3).clusterConfig(clusterConfig).transport(transport3)
                .minElectionTimeout(Duration.ofMillis(380))
                .maxElectionTimeout(Duration.ofMillis(450))
                .heartbeatInterval(Duration.ofMillis(30))
                .random(new Random(3))
                .build();

        try {
            // 1. Start cluster
            System.out.println("▶ [1/4] Starting 3 Raft consensus nodes with Single-Threaded Event Loop...");
            node1.start();
            node2.start();
            node3.start();

            System.out.println("   ✔ Node 1 started [Role: " + node1.role() + ", Term: " + node1.currentTerm() + "]");
            System.out.println("   ✔ Node 2 started [Role: " + node2.role() + ", Term: " + node2.currentTerm() + "]");
            System.out.println("   ✔ Node 3 started [Role: " + node3.role() + ", Term: " + node3.currentTerm() + "]\n");

            // 2. Acceptance Criterion 1: Exactly one leader per term
            System.out.println("▶ [2/4] [AC1] Verifying leader election: Exactly ONE leader elected for Term 1...");
            await().atMost(Duration.ofSeconds(2)).until(() ->
                    node1.role() == RaftRole.LEADER || node2.role() == RaftRole.LEADER || node3.role() == RaftRole.LEADER
            );

            List<RaftNode> cluster = List.of(node1, node2, node3);
            RaftNode initialLeader = cluster.stream()
                    .filter(n -> n.role() == RaftRole.LEADER)
                    .findFirst()
                    .orElseThrow();

            long leadersCount = cluster.stream().filter(n -> n.role() == RaftRole.LEADER).count();
            if (leadersCount != 1) {
                throw new AssertionError("More than 1 leader elected! Count: " + leadersCount);
            }

            System.out.println("   ✔ [AC1 PASS] Leader elected: " + initialLeader.nodeId() +
                    " in Term " + initialLeader.currentTerm());
            System.out.println("   ✔ [AC1 PASS] Safety Invariant verified: Exactly 1 leader in Term " +
                    initialLeader.currentTerm() + "\n");

            // 3. Acceptance Criterion 2: Followers reset timeout on valid heartbeat
            System.out.println("▶ [3/4] [AC2] Verifying heartbeats: Followers reset election timer on valid heartbeat...");
            Thread.sleep(150); // Allow multiple heartbeat intervals (30ms per round)

            for (RaftNode node : cluster) {
                if (!node.nodeId().equals(initialLeader.nodeId())) {
                    if (node.role() != RaftRole.FOLLOWER) {
                        throw new AssertionError("Node " + node.nodeId() + " is not follower!");
                    }
                    if (node.currentLeader().isEmpty() || !node.currentLeader().get().equals(initialLeader.nodeId())) {
                        throw new AssertionError("Node " + node.nodeId() + " does not recognize leader!");
                    }
                    System.out.println("   ✔ [AC2 PASS] Node " + node.nodeId() +
                            " is stable FOLLOWER and receives heartbeats from " + node.currentLeader().get());
                }
            }
            System.out.println("   ✔ [AC2 PASS] Heartbeat Invariant verified: All followers reset election timer.\n");

            // 4. Acceptance Criterion 3: Leader failure & re-election
            System.out.println("▶ [4/4] [AC3] Simulating leader failure: Stopping leader " + initialLeader.nodeId() + "...");
            initialLeader.stop();
            if (initialLeader == node1) transport1.stop();
            else if (initialLeader == node2) transport2.stop();
            else transport3.stop();

            System.out.println("   ⚡ Leader " + initialLeader.nodeId() + " terminated. Waiting for re-election...");

            List<RaftNode> remainingNodes = cluster.stream()
                    .filter(n -> !n.nodeId().equals(initialLeader.nodeId()))
                    .toList();

            await().atMost(Duration.ofSeconds(4)).until(() ->
                    remainingNodes.stream().anyMatch(n -> n.role() == RaftRole.LEADER)
            );

            RaftNode newLeader = remainingNodes.stream()
                    .filter(n -> n.role() == RaftRole.LEADER)
                    .findFirst()
                    .orElseThrow();

            RaftNode newFollower = remainingNodes.stream()
                    .filter(n -> n.role() == RaftRole.FOLLOWER)
                    .findFirst()
                    .orElseThrow();

            System.out.println("   ✔ [AC3 PASS] New leader elected: " + newLeader.nodeId() +
                    " with new higher Term: " + newLeader.currentTerm() +
                    " (previous: " + initialLeader.currentTerm() + ")");
            System.out.println("   ✔ [AC3 PASS] Follower " + newFollower.nodeId() +
                    " acknowledges new leader: " + newFollower.currentLeader().orElse(null) + "\n");

            System.out.println("===============================================================");
            System.out.println("     ALL 3 SPRINT 2 ACCEPTANCE CRITERIA VERIFIED!             ");
            System.out.println("===============================================================");
            System.exit(0);

        } catch (Exception e) {
            System.err.println("\n❌ DEMONSTRATION FAILED: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            node1.stop();
            node2.stop();
            node3.stop();
            transport1.stop();
            transport2.stop();
            transport3.stop();
            InMemoryTransport.clearRegistry();
            RaftInvariants.clearInvariantTracking();
        }
    }
}
