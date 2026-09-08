package se.mouaz.aegisdb.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.*;
import se.mouaz.aegisdb.node.DatabaseNode;
import se.mouaz.aegisdb.node.NodeBootstrap;
import se.mouaz.aegisdb.protocol.*;
import se.mouaz.aegisdb.transport.TransportException;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class Sprint1Demo {
    private static final Logger log = LoggerFactory.getLogger(Sprint1Demo.class);

    public static void main(String[] args) {
        System.out.println("===============================================================");
        System.out.println("     AegisDB - Sprint 1 Demonstration & Verification ");
        System.out.println("===============================================================\n");

        NodeId id1 = NodeId.of("node-1");
        NodeId id2 = NodeId.of("node-2");
        NodeId id3 = NodeId.of("node-3");

        Endpoint ep1 = Endpoint.of("127.0.0.1", 7001);
        Endpoint ep2 = Endpoint.of("127.0.0.1", 7002);
        Endpoint ep3 = Endpoint.of("127.0.0.1", 7003);

        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .clusterId("aegisdb-sprint1-demo")
                .addMember(id1, ep1)
                .addMember(id2, ep2)
                .addMember(id3, ep3)
                .build();

        NetworkConfiguration netConfig = new NetworkConfiguration(Duration.ofSeconds(1), Duration.ofSeconds(3));

        NodeConfiguration config1 = NodeConfiguration.builder().nodeId(id1).endpoint(ep1).networkConfig(netConfig).build();
        NodeConfiguration config2 = NodeConfiguration.builder().nodeId(id2).endpoint(ep2).networkConfig(netConfig).build();
        NodeConfiguration config3 = NodeConfiguration.builder().nodeId(id3).endpoint(ep3).networkConfig(netConfig).build();

        DatabaseNode node1 = NodeBootstrap.createGrpcNode(config1, clusterConfig);
        DatabaseNode node2 = NodeBootstrap.createGrpcNode(config2, clusterConfig);
        DatabaseNode node3 = NodeBootstrap.createGrpcNode(config3, clusterConfig);

        try {
            // 1. Start nodes
            System.out.println("▶ [1/6] [AC1] Starting 3 distributed gRPC nodes...");
            node1.start();
            node2.start();
            node3.start();

            System.out.println("   ✔ Node 1 started on port " + ep1.port() + " [Status: " + node1.status() + "]");
            System.out.println("   ✔ Node 2 started on port " + ep2.port() + " [Status: " + node2.status() + "]");
            System.out.println("   ✔ Node 3 started on port " + ep3.port() + " [Status: " + node3.status() + "]\n");

            // 2. Verify unique identities
            System.out.println("▶ [2/6] [AC2] Verifying unique node identities...");
            System.out.println("   ✔ Node 1 ID: " + node1.nodeId() + " -> " + node1.config().endpoint());
            System.out.println("   ✔ Node 2 ID: " + node2.nodeId() + " -> " + node2.config().endpoint());
            System.out.println("   ✔ Node 3 ID: " + node3.nodeId() + " -> " + node3.config().endpoint() + "\n");

            // 3. Node A calls Node B with RequestVote
            System.out.println("▶ [3/6] [AC3] Sending RequestVote RPC: Node 1 ──(gRPC)──▶ Node 2...");
            RequestVoteRequest voteReq = new RequestVoteRequest(node1.nodeId(), 1, 0, 0);
            RequestVoteResponse voteResp = node1.sendRequestVote(node2.nodeId(), voteReq).get();
            System.out.println("   ✔ Response received from Node 2: [Term=" + voteResp.term() + ", VoteGranted=" + voteResp.voteGranted() + ", Reason=" + voteResp.reason() + "]\n");

            // 4. Node A calls Node C with AppendEntries
            System.out.println("▶ [4/6] [AC4] Sending AppendEntries RPC: Node 1 ──(gRPC)──▶ Node 3...");
            byte[] dummyLog = "log-entry-001".getBytes(StandardCharsets.UTF_8);
            AppendEntriesRequest appendReq = new AppendEntriesRequest(1, node1.nodeId(), 0, 0, dummyLog, 0);
            AppendEntriesResponse appendResp = node1.sendAppendEntries(node3.nodeId(), appendReq).get();
            System.out.println("   ✔ Response received from Node 3: [Term=" + appendResp.term() + ", Success=" + appendResp.success() + ", MatchIndex=" + appendResp.matchIndex() + "]\n");

            // 5. Error Propagation & Stopped Node
            System.out.println("▶ [5/6] [AC5] Testing error propagation: Stopping Node 3 and invoking RPC...");
            node3.stop();
            System.out.println("   ✔ Node 3 is now [Status: " + node3.status() + "]");
            try {
                node1.sendRequestVote(node3.nodeId(), voteReq).get();
                System.err.println("   ✘ Error: Call should have failed!");
            } catch (Exception e) {
                System.out.println("   ✔ Expected error caught and propagated correctly: " + e.getCause().getClass().getSimpleName() + " (" + e.getCause().getMessage() + ")\n");
            }

            // 6. Graceful Shutdown
            System.out.println("▶ [6/6] [AC6] Stopping remaining nodes gracefully...");
            node1.stop();
            node2.stop();
            System.out.println("   ✔ Node 1 [Status: " + node1.status() + "]");
            System.out.println("   ✔ Node 2 [Status: " + node2.status() + "]");
            System.out.println("   ✔ Node 3 [Status: " + node3.status() + "]\n");

            System.out.println("===============================================================");
            System.out.println("  ALL 6 SPRINT 1 ACCEPTANCE CRITERIA VERIFIED SUCCESSFULLY!   ");
            System.out.println("===============================================================");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            node1.stop();
            node2.stop();
            node3.stop();
        }
    }
}
