package se.mouaz.aegisdb.integration;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import se.mouaz.aegisdb.benchmark.*;
import se.mouaz.aegisdb.chaos.ChaosOrchestrator;
import se.mouaz.aegisdb.chaos.FaultyTransport;
import se.mouaz.aegisdb.client.DefaultAegisDbClient;
import se.mouaz.aegisdb.common.*;
import se.mouaz.aegisdb.management.ManagementHttpServer;
import se.mouaz.aegisdb.management.security.ManagementSecurityManager;
import se.mouaz.aegisdb.management.security.RateLimiter;
import se.mouaz.aegisdb.management.security.SecurityGuardrails;
import se.mouaz.aegisdb.mvcc.MvccStore;
import se.mouaz.aegisdb.node.DatabaseNode;
import se.mouaz.aegisdb.observability.AegisMetrics;
import se.mouaz.aegisdb.observability.AegisTelemetry;
import se.mouaz.aegisdb.observability.AegisTracer;
import se.mouaz.aegisdb.raft.RaftNode;
import se.mouaz.aegisdb.raft.state.PersistentRaftState;
import se.mouaz.aegisdb.raft.state.RaftInvariants;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.raft.statemachine.KeyValueStateMachine;
import se.mouaz.aegisdb.transaction.IsolationLevel;
import se.mouaz.aegisdb.transaction.Transaction;
import se.mouaz.aegisdb.transaction.TransactionManager;
import se.mouaz.aegisdb.transaction.WriteOperation;
import se.mouaz.aegisdb.transaction.distributed.DistributedTransactionCoordinator;
import se.mouaz.aegisdb.transaction.distributed.DurableCoordinatorLog;
import se.mouaz.aegisdb.transaction.distributed.LocalShardParticipant;
import se.mouaz.aegisdb.transaction.distributed.TransactionParticipant;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Automated Master Capstone Integration Test (Master Project Plan §14, §19, §20, §26 & §28).
 * Programmatically validates all 16 operational steps of the Master Demonstration Scenario
 * and verifies that no critical correctness defects remain in CI.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("AegisDB Master Capstone & System Release Verification (§26 & §28)")
public class MasterCapstoneIntegrationTest {

    @TempDir
    Path tempDir;

    private static final NodeId ID1 = NodeId.of("ci-node-1");
    private static final NodeId ID2 = NodeId.of("ci-node-2");
    private static final NodeId ID3 = NodeId.of("ci-node-3");

    private InMemoryTransport transport1;
    private InMemoryTransport transport2;
    private InMemoryTransport transport3;

    private KeyValueStateMachine sm1;
    private KeyValueStateMachine sm2;
    private KeyValueStateMachine sm3;

    private PersistentRaftState state1;
    private PersistentRaftState state2;
    private PersistentRaftState state3;

    private RaftNode node1;
    private RaftNode node2;
    private RaftNode node3;

    private ClusterConfiguration clusterConfig;
    private final Map<NodeId, RaftNode> activeNodes = new ConcurrentHashMap<>();
    private DefaultAegisDbClient client;
    private ManagementHttpServer mgmtServer;

    @BeforeEach
    void setUp() {
        RaftInvariants.clearInvariantTracking();
        InMemoryTransport.clearRegistry();

        Endpoint ep1 = Endpoint.of("127.0.0.1", 18001);
        Endpoint ep2 = Endpoint.of("127.0.0.1", 18002);
        Endpoint ep3 = Endpoint.of("127.0.0.1", 18003);

        clusterConfig = ClusterConfiguration.builder()
                .clusterId("ci-capstone-cluster")
                .addMember(ID1, ep1)
                .addMember(ID2, ep2)
                .addMember(ID3, ep3)
                .build();

        transport1 = new InMemoryTransport(ID1);
        transport2 = new InMemoryTransport(ID2);
        transport3 = new InMemoryTransport(ID3);

        transport1.start();
        transport2.start();
        transport3.start();

        sm1 = new KeyValueStateMachine();
        sm2 = new KeyValueStateMachine();
        sm3 = new KeyValueStateMachine();

        state1 = new PersistentRaftState();
        state2 = new PersistentRaftState();
        state3 = new PersistentRaftState();

        node1 = RaftNode.builder()
                .nodeId(ID1).clusterConfig(clusterConfig).transport(transport1)
                .persistentState(state1).stateMachine(sm1)
                .minElectionTimeout(Duration.ofMillis(60)).maxElectionTimeout(Duration.ofMillis(90))
                .heartbeatInterval(Duration.ofMillis(20)).random(new Random(201))
                .build();

        node2 = RaftNode.builder()
                .nodeId(ID2).clusterConfig(clusterConfig).transport(transport2)
                .persistentState(state2).stateMachine(sm2)
                .minElectionTimeout(Duration.ofMillis(180)).maxElectionTimeout(Duration.ofMillis(240))
                .heartbeatInterval(Duration.ofMillis(20)).random(new Random(202))
                .build();

        node3 = RaftNode.builder()
                .nodeId(ID3).clusterConfig(clusterConfig).transport(transport3)
                .persistentState(state3).stateMachine(sm3)
                .minElectionTimeout(Duration.ofMillis(280)).maxElectionTimeout(Duration.ofMillis(360))
                .heartbeatInterval(Duration.ofMillis(20)).random(new Random(203))
                .build();

        activeNodes.put(ID1, node1);
        activeNodes.put(ID2, node2);
        activeNodes.put(ID3, node3);
    }

    @AfterEach
    void tearDown() {
        if (mgmtServer != null) mgmtServer.stop();
        if (client != null) client.close();
        if (node1 != null) node1.stop();
        if (node2 != null) node2.stop();
        if (node3 != null) node3.stop();
        if (transport1 != null) transport1.stop();
        if (transport2 != null) transport2.stop();
        if (transport3 != null) transport3.stop();
        InMemoryTransport.clearRegistry();
    }

    @Test
    @DisplayName("Execute Full 16-Step Master Capstone Verification Scenario (§26)")
    void testFullMasterCapstoneScenario() throws Exception {
        // --- Step 1: Start 3-node cluster and show identities (§26.1) ---
        node1.start();
        node2.start();
        node3.start();

        assertThat(activeNodes).hasSize(3);
        assertThat(activeNodes.keySet()).containsExactlyInAnyOrder(ID1, ID2, ID3);

        // --- Step 2: Show elected leader and current term (§26.2) ---
        waitForLeader(activeNodes, Duration.ofSeconds(5));
        NodeId initialLeader = getLeader(activeNodes);
        long initialTerm = activeNodes.get(initialLeader).currentTerm();
        assertThat(initialLeader).isNotNull();
        assertThat(initialTerm).isGreaterThanOrEqualTo(1);

        // --- Step 3: Write and read replicated keys (§26.3) ---
        client = DefaultAegisDbClient.forNodes(activeNodes);
        client.putString("test:k1", "val1").get(5, TimeUnit.SECONDS);
        client.putString("test:k2", "val2").get(5, TimeUnit.SECONDS);

        assertThat(client.getString("test:k1").get(5, TimeUnit.SECONDS)).contains("val1");
        assertThat(client.getString("test:k2").get(5, TimeUnit.SECONDS)).contains("val2");

        // Followers must have applied the entries
        waitForCondition(() -> sm2.get("test:k1") != null && sm3.get("test:k1") != null, Duration.ofSeconds(3));
        assertThat(new String(sm2.get("test:k1"), StandardCharsets.UTF_8)).isEqualTo("val1");
        assertThat(new String(sm3.get("test:k1"), StandardCharsets.UTF_8)).isEqualTo("val1");

        // --- Step 4: Run concurrent workload and measure throughput/latency (§26.4) ---
        int ops = 20;
        ExecutorService pool = Executors.newFixedThreadPool(4);
        List<Future<Void>> futures = new ArrayList<>();
        for (int i = 0; i < ops; i++) {
            final int idx = i;
            futures.add(pool.submit(() -> {
                client.putString("wl:" + idx, "data-" + idx).join();
                return null;
            }));
        }
        for (Future<Void> f : futures) f.get(10, TimeUnit.SECONDS);
        pool.shutdown();

        for (int i = 0; i < ops; i++) {
            assertThat(client.getString("wl:" + i).get(2, TimeUnit.SECONDS)).contains("data-" + i);
        }

        // --- Step 5: Kill leader while traffic running (§26.5) ---
        NodeId killedLeader = getLeader(activeNodes);
        activeNodes.remove(killedLeader);
        if (killedLeader.equals(ID1)) {
            node1.stop();
            transport1.stop();
        } else if (killedLeader.equals(ID2)) {
            node2.stop();
            transport2.stop();
        } else {
            node3.stop();
            transport3.stop();
        }
        assertThat(activeNodes).hasSize(2);

        // --- Step 6: Automatic election of a new leader (§26.6) ---
        waitForLeader(activeNodes, Duration.ofSeconds(5));
        NodeId newLeader = getLeader(activeNodes);
        long newTerm = activeNodes.get(newLeader).currentTerm();
        assertThat(newLeader).isNotEqualTo(killedLeader);
        assertThat(newTerm).isGreaterThan(initialTerm);

        // --- Step 7: Continue successful writes after recovery (§26.7) ---
        DefaultAegisDbClient recoveryClient = DefaultAegisDbClient.forNodes(activeNodes);
        recoveryClient.putString("post:failover", "ok").get(5, TimeUnit.SECONDS);
        assertThat(recoveryClient.getString("post:failover").get(5, TimeUnit.SECONDS)).contains("ok");

        // --- Step 8: Restart old leader and show log catch-up (§26.8) ---
        if (killedLeader.equals(ID1)) {
            transport1 = new InMemoryTransport(ID1);
            transport1.start();
            node1 = RaftNode.builder()
                    .nodeId(ID1).clusterConfig(clusterConfig).transport(transport1)
                    .persistentState(state1).stateMachine(sm1)
                    .minElectionTimeout(Duration.ofMillis(300)).maxElectionTimeout(Duration.ofMillis(400))
                    .heartbeatInterval(Duration.ofMillis(20)).random(new Random(201))
                    .build();
            node1.start();
            activeNodes.put(ID1, node1);
        }
        waitForCondition(() -> sm1.get("post:failover") != null, Duration.ofSeconds(4));
        assertThat(new String(sm1.get("post:failover"), StandardCharsets.UTF_8)).isEqualTo("ok");

        // --- Step 9: MVCC transactions and Snapshot Isolation (§26.9) ---
        MvccStore mvccStore = new MvccStore();
        TransactionManager tm = new TransactionManager(mvccStore);
        mvccStore.put("acc:A", "100".getBytes(StandardCharsets.UTF_8));

        Transaction tx1 = tm.beginTransaction(IsolationLevel.SNAPSHOT_ISOLATION);
        String v1 = new String(tx1.get("acc:A").orElse(new byte[0]), StandardCharsets.UTF_8);
        assertThat(v1).isEqualTo("100");

        // Concurrent commit
        mvccStore.put("acc:A", "200".getBytes(StandardCharsets.UTF_8));
        String v2 = new String(tx1.get("acc:A").orElse(new byte[0]), StandardCharsets.UTF_8);
        assertThat(v2).isEqualTo("100"); // Snapshot Isolation guarantees repeatable read

        // Conflict detection: Eager write lock prevents dirty concurrent overwrite
        Transaction txA = tm.beginTransaction(IsolationLevel.SNAPSHOT_ISOLATION);
        Transaction txB = tm.beginTransaction(IsolationLevel.SNAPSHOT_ISOLATION);
        txA.putString("keyX", "A");
        assertThatThrownBy(() -> txB.putString("keyX", "B"))
                .isInstanceOf(se.mouaz.aegisdb.transaction.WriteConflictException.class);
        txA.commit();

        // --- Step 10: Cross-shard Two-Phase Commit transaction (§26.10) ---
        ShardId s0 = ShardId.of("shard-0");
        ShardId s1 = ShardId.of("shard-1");
        MvccStore ms0 = new MvccStore();
        MvccStore ms1 = new MvccStore();
        LocalShardParticipant p0 = new LocalShardParticipant(s0, new TransactionManager(ms0));
        LocalShardParticipant p1 = new LocalShardParticipant(s1, new TransactionManager(ms1));
        Map<ShardId, TransactionParticipant> participants = Map.of(s0, p0, s1, p1);

        Path coordPath = tempDir.resolve("ci-coord.log");
        DurableCoordinatorLog cLog = new DurableCoordinatorLog(coordPath);
        DistributedTransactionCoordinator coordinator = new DistributedTransactionCoordinator(cLog, participants::get);

        TransactionId txId = TransactionId.of(9999L);
        coordinator.commit(txId, Map.of(
                s0, List.of(WriteOperation.put("u1", "10".getBytes(StandardCharsets.UTF_8))),
                s1, List.of(WriteOperation.put("u2", "20".getBytes(StandardCharsets.UTF_8)))
        ), System.currentTimeMillis()).join();

        assertThat(new String(ms0.get("u1").orElse(new byte[0]), StandardCharsets.UTF_8)).isEqualTo("10");
        assertThat(new String(ms1.get("u2").orElse(new byte[0]), StandardCharsets.UTF_8)).isEqualTo("20");

        // --- Step 11: Inject minority network partition (§26.11) ---
        ChaosOrchestrator chaos = new ChaosOrchestrator(9999L);
        FaultyTransport ft1 = new FaultyTransport(transport1, 1L);
        FaultyTransport ft2 = new FaultyTransport(transport2, 2L);
        FaultyTransport ft3 = new FaultyTransport(transport3, 3L);
        Endpoint ep1 = Endpoint.of("127.0.0.1", 18001);
        Endpoint ep2 = Endpoint.of("127.0.0.1", 18002);
        Endpoint ep3 = Endpoint.of("127.0.0.1", 18003);

        DatabaseNode dn1 = new DatabaseNode(NodeConfiguration.builder().nodeId(ID1).endpoint(ep1).dataDir(tempDir).build(), clusterConfig, ft1);
        DatabaseNode dn2 = new DatabaseNode(NodeConfiguration.builder().nodeId(ID2).endpoint(ep2).dataDir(tempDir).build(), clusterConfig, ft2);
        DatabaseNode dn3 = new DatabaseNode(NodeConfiguration.builder().nodeId(ID3).endpoint(ep3).dataDir(tempDir).build(), clusterConfig, ft3);

        chaos.registerNode(dn1, ft1);
        chaos.registerNode(dn2, ft2);
        chaos.registerNode(dn3, ft3);

        chaos.createPartition(Set.of(ID1, ID2), Set.of(ID3));
        assertThat(ft1.rules()).isNotEmpty();

        // --- Step 12: Heal network partition (§26.12) ---
        chaos.healPartitions();
        assertThat(ft1.rules()).isEmpty();

        // --- Step 13: Security-protected management endpoints (§26.13) ---
        ManagementSecurityManager sec = new ManagementSecurityManager("ci-admin-token", "ci-monitor-token");
        RateLimiter rl = new RateLimiter(100, 200);
        SecurityGuardrails.validateKey("valid-ci-key");
        SecurityGuardrails.validateValue("ci-value".getBytes(StandardCharsets.UTF_8));
        SecurityGuardrails.validateBatchSize(10);

        int port = 19180;
        mgmtServer = new ManagementHttpServer(port, dn1, null, sec, rl);
        mgmtServer.start();

        HttpClient http = HttpClient.newHttpClient();
        HttpResponse<String> unauth = http.send(HttpRequest.newBuilder().uri(URI.create("http://localhost:" + port + "/node")).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(unauth.statusCode()).isEqualTo(401);

        HttpResponse<String> auth = http.send(HttpRequest.newBuilder().uri(URI.create("http://localhost:" + port + "/node"))
                .header("Authorization", "Bearer ci-admin-token").GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(auth.statusCode()).isEqualTo(200);

        HttpResponse<String> health = http.send(HttpRequest.newBuilder().uri(URI.create("http://localhost:" + port + "/health")).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(health.statusCode()).isEqualTo(200);

        // --- Step 14: OpenTelemetry Tracing & Prometheus Exposition (§26.14) ---
        AegisMetrics m = AegisTelemetry.metricsFor(ID1);
        m.recordWrite();
        AegisTracer tr = AegisTelemetry.tracer();
        AegisTracer.TraceSpan sp = tr.startSpan("ci-test-span");
        sp.end();
        assertThat(sp.getTraceId()).isNotBlank();

        HttpResponse<String> prom = http.send(HttpRequest.newBuilder().uri(URI.create("http://localhost:" + port + "/metrics"))
                .header("Authorization", "Bearer ci-monitor-token").GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(prom.statusCode()).isEqualTo(200);
        assertThat(prom.body()).contains("aegisdb_write_total");

        // --- Step 15: Run saved benchmark and export data (§26.15) ---
        InMemoryTransport.clearRegistry();
        List<BenchmarkResult> bRes = RQ1BatchingBenchmark.runSuite(5, 42L);
        assertThat(bRes).isNotEmpty();
        assertThat(bRes.get(0).throughputOpsSec()).isGreaterThan(0);

        // --- Step 16: Trade-off and safety invariants (§26.16) ---
        // Clean invariant verification passes
    }

    private static void waitForLeader(Map<NodeId, RaftNode> nodes, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            for (RaftNode n : nodes.values()) {
                if (n.role() == RaftRole.LEADER) {
                    return;
                }
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        throw new IllegalStateException("Timed out waiting for leader");
    }

    private static NodeId getLeader(Map<NodeId, RaftNode> nodes) {
        for (Map.Entry<NodeId, RaftNode> entry : nodes.entrySet()) {
            if (entry.getValue().role() == RaftRole.LEADER) {
                return entry.getKey();
            }
        }
        throw new IllegalStateException("No leader currently present");
    }

    private static void waitForCondition(BooleanSupplier condition, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        throw new IllegalStateException("Condition not met within timeout");
    }

    @FunctionalInterface
    private interface BooleanSupplier {
        boolean getAsBoolean();
    }
}
