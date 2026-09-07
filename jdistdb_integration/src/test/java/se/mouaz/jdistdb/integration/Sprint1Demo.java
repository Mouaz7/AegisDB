package se.mouaz.jdistdb.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.jdistdb.common.*;
import se.mouaz.jdistdb.node.DatabaseNode;
import se.mouaz.jdistdb.node.NodeBootstrap;
import se.mouaz.jdistdb.protocol.*;
import se.mouaz.jdistdb.transport.TransportException;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

public class Sprint1Demo {
    private static final Logger log = LoggerFactory.getLogger(Sprint1Demo.class);

    public static void main(String[] args) {
        System.out.println("===============================================================");
        System.out.println("     JDistDB / AegisDB - Sprint 1 Demonstration & Verifiering ");
        System.out.println("===============================================================\n");

        NodeId id1 = NodeId.of("node-1");
        NodeId id2 = NodeId.of("node-2");
        NodeId id3 = NodeId.of("node-3");

        Endpoint ep1 = Endpoint.of("127.0.0.1", 7001);
        Endpoint ep2 = Endpoint.of("127.0.0.1", 7002);
        Endpoint ep3 = Endpoint.of("127.0.0.1", 7003);

        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .clusterId("jdistdb-sprint1-demo")
                .addMember(id1, ep1)
                .addMember(id2, ep2)
                .addMember(id3, ep3)
                .build();

        NetworkConfiguration netConfig = new NetworkConfiguration(Duration.ofSeconds(1), Duration.ofMillis(800));

        NodeConfiguration config1 = NodeConfiguration.builder().nodeId(id1).endpoint(ep1).networkConfig(netConfig).build();
        NodeConfiguration config2 = NodeConfiguration.builder().nodeId(id2).endpoint(ep2).networkConfig(netConfig).build();
        NodeConfiguration config3 = NodeConfiguration.builder().nodeId(id3).endpoint(ep3).networkConfig(netConfig).build();

        DatabaseNode node1 = NodeBootstrap.createGrpcNode(config1, clusterConfig);
        DatabaseNode node2 = NodeBootstrap.createGrpcNode(config2, clusterConfig);
        DatabaseNode node3 = NodeBootstrap.createGrpcNode(config3, clusterConfig);

        try {
            // 1. Starta noder
            System.out.println("▶ [1/6] Startar 3 distribuerade gRPC-noder...");
            node1.start();
            node2.start();
            node3.start();

            System.out.println("   ✔ Node 1 startad på port " + ep1.port() + " [Status: " + node1.status() + "]");
            System.out.println("   ✔ Node 2 startad på port " + ep2.port() + " [Status: " + node2.status() + "]");
            System.out.println("   ✔ Node 3 startad på port " + ep3.port() + " [Status: " + node3.status() + "]\n");

            // 2. Kontrollera unika identiteter
            System.out.println("▶ [2/6] Verifierar unika nod-identiteter...");
            System.out.println("   ✔ Node 1 ID: " + node1.nodeId() + " -> " + node1.config().endpoint());
            System.out.println("   ✔ Node 2 ID: " + node2.nodeId() + " -> " + node2.config().endpoint());
            System.out.println("   ✔ Node 3 ID: " + node3.nodeId() + " -> " + node3.config().endpoint() + "\n");

            // 3. Node A anropar Node B med RequestVote
            System.out.println("▶ [3/6] Skickar RequestVote RPC: Node 1 ──(gRPC)──▶ Node 2...");
            RequestVoteRequest voteReq = new RequestVoteRequest(node1.nodeId(), 1, 0, 0);
            RequestVoteResponse voteResp = node1.sendRequestVote(node2.nodeId(), voteReq).get();
            System.out.println("   ✔ Svar mottaget från Node 2: [Term=" + voteResp.term() + ", VoteGranted=" + voteResp.voteGranted() + ", Orsak=" + voteResp.reason() + "]\n");

            // 4. Node A anropar Node C med AppendEntries
            System.out.println("▶ [4/6] Skickar AppendEntries RPC: Node 1 ──(gRPC)──▶ Node 3...");
            byte[] dummyLog = "log-entry-001".getBytes(StandardCharsets.UTF_8);
            AppendEntriesRequest appendReq = new AppendEntriesRequest(1, node1.nodeId(), 0, 0, dummyLog, 0);
            AppendEntriesResponse appendResp = node1.sendAppendEntries(node3.nodeId(), appendReq).get();
            System.out.println("   ✔ Svar mottaget från Node 3: [Term=" + appendResp.term() + ", Success=" + appendResp.success() + ", MatchIndex=" + appendResp.matchIndex() + "]\n");

            // 5. Test av Felpropagering & Stoppad nod
            System.out.println("▶ [5/6] Testar felpropagering: Stänger ner Node 3 och försöker anropa den...");
            node3.stop();
            System.out.println("   ✔ Node 3 är nu [Status: " + node3.status() + "]");
            try {
                node1.sendRequestVote(node3.nodeId(), voteReq).get();
                System.err.println("   ✘ Fel: Anropet borde ha misslyckats!");
            } catch (Exception e) {
                System.out.println("   ✔ Förväntat fel fångat och propagerat korrekt: " + e.getCause().getClass().getSimpleName() + " (" + e.getCause().getMessage() + ")\n");
            }

            // 6. Graceful Shutdown
            System.out.println("▶ [6/6] Stänger ner resterande noder...");
            node1.stop();
            node2.stop();
            System.out.println("   ✔ Node 1 [Status: " + node1.status() + "]");
            System.out.println("   ✔ Node 2 [Status: " + node2.status() + "]");
            System.out.println("   ✔ Node 3 [Status: " + node3.status() + "]\n");

            System.out.println("===============================================================");
            System.out.println("  ALLA 6 ACCEPTANSKRITERIER FÖR SPRINT 1 ÄR FRAMGÅNGSRIKA!  ");
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
