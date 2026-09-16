package se.mouaz.aegisdb.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.chaos.linearizability.*;
import se.mouaz.aegisdb.client.DefaultAegisDbClient;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.RaftNode;
import se.mouaz.aegisdb.raft.state.PersistentRaftState;
import se.mouaz.aegisdb.raft.state.RaftInvariants;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.raft.statemachine.KeyValueStateMachine;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@DisplayName("Cluster Linearizability Verification under Concurrency & Failover")
class LinearizabilityIntegrationTest {

    private final NodeId id1 = NodeId.of("lin-node-1");
    private final NodeId id2 = NodeId.of("lin-node-2");
    private final NodeId id3 = NodeId.of("lin-node-3");

    private InMemoryTransport transport1;
    private InMemoryTransport transport2;
    private InMemoryTransport transport3;

    private KeyValueStateMachine sm1;
    private KeyValueStateMachine sm2;
    private KeyValueStateMachine sm3;

    private RaftNode node1;
    private RaftNode node2;
    private RaftNode node3;

    private final Map<NodeId, RaftNode> activeNodes = new ConcurrentHashMap<>();
    private DefaultAegisDbClient client;
    private final OperationTrace trace = new OperationTrace();

    @BeforeEach
    void setUp() {
        RaftInvariants.clearInvariantTracking();
        InMemoryTransport.clearRegistry();
        trace.clear();

        Endpoint ep1 = Endpoint.of("127.0.0.1", 15001);
        Endpoint ep2 = Endpoint.of("127.0.0.1", 15002);
        Endpoint ep3 = Endpoint.of("127.0.0.1", 15003);

        ClusterConfiguration clusterConfig = ClusterConfiguration.builder()
                .clusterId("lin-cluster")
                .addMember(id1, ep1)
                .addMember(id2, ep2)
                .addMember(id3, ep3)
                .build();

        transport1 = new InMemoryTransport(id1);
        transport2 = new InMemoryTransport(id2);
        transport3 = new InMemoryTransport(id3);

        transport1.start();
        transport2.start();
        transport3.start();

        sm1 = new KeyValueStateMachine();
        sm2 = new KeyValueStateMachine();
        sm3 = new KeyValueStateMachine();

        node1 = RaftNode.builder()
                .nodeId(id1).clusterConfig(clusterConfig).transport(transport1)
                .persistentState(new PersistentRaftState())
                .stateMachine(sm1)
                .minElectionTimeout(Duration.ofMillis(80)).maxElectionTimeout(Duration.ofMillis(120))
                .heartbeatInterval(Duration.ofMillis(25)).random(new Random(301))
                .build();

        node2 = RaftNode.builder()
                .nodeId(id2).clusterConfig(clusterConfig).transport(transport2)
                .persistentState(new PersistentRaftState())
                .stateMachine(sm2)
                .minElectionTimeout(Duration.ofMillis(200)).maxElectionTimeout(Duration.ofMillis(280))
                .heartbeatInterval(Duration.ofMillis(25)).random(new Random(302))
                .build();

        node3 = RaftNode.builder()
                .nodeId(id3).clusterConfig(clusterConfig).transport(transport3)
                .persistentState(new PersistentRaftState())
                .stateMachine(sm3)
                .minElectionTimeout(Duration.ofMillis(300)).maxElectionTimeout(Duration.ofMillis(400))
                .heartbeatInterval(Duration.ofMillis(25)).random(new Random(303))
                .build();

        activeNodes.put(id1, node1);
        activeNodes.put(id2, node2);
        activeNodes.put(id3, node3);

        node1.start();
        node2.start();
        node3.start();

        await().atMost(5, TimeUnit.SECONDS).until(() -> node1.role() == RaftRole.LEADER);
        client = DefaultAegisDbClient.forNodes(activeNodes);
    }

    @AfterEach
    void tearDown() {
        if (client != null) client.close();

        if (node1 != null) node1.stop();
        if (node2 != null) node2.stop();
        if (node3 != null) node3.stop();

        if (transport1 != null) transport1.stop();
        if (transport2 != null) transport2.stop();
        if (transport3 != null) transport3.stop();

        InMemoryTransport.clearRegistry();
        RaftInvariants.clearInvariantTracking();
    }

    @Test
    @DisplayName("Verify linearizability under concurrent readers and writers during leader failover")
    void concurrentOperationsWithFailoverAreLinearizable() throws Exception {
        int writerCount = 3;
        int readerCount = 3;
        ExecutorService executor = Executors.newFixedThreadPool(writerCount + readerCount + 1);
        AtomicBoolean running = new AtomicBoolean(true);
        List<String> keys = List.of("account-A", "account-B");

        List<Future<?>> tasks = new ArrayList<>();

        // 1. Launch concurrent writers
        for (int i = 0; i < writerCount; i++) {
            final String clientId = "writer-" + i;
            tasks.add(executor.submit(() -> {
                int counter = 0;
                while (running.get() && counter < 15) {
                    counter++;
                    String key = keys.get(counter % keys.size());
                    String val = clientId + ":v" + counter;
                    long invoke = System.nanoTime();
                    try {
                        client.putString(key, val).get(800, TimeUnit.MILLISECONDS);
                        long ret = System.nanoTime();
                        trace.recordWrite(clientId, key, val, invoke, ret, OperationStatus.OK);
                    } catch (TimeoutException | ExecutionException e) {
                        long ret = System.nanoTime();
                        // Indeterminate timeout / failover: operation might have committed or been dropped
                        trace.recordWrite(clientId, key, val, invoke, ret, OperationStatus.TIMEOUT_INDETERMINATE);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                    try { Thread.sleep(20); } catch (InterruptedException ignored) {}
                }
            }));
        }

        // 2. Launch concurrent readers
        for (int i = 0; i < readerCount; i++) {
            final String clientId = "reader-" + i;
            tasks.add(executor.submit(() -> {
                int readCount = 0;
                while (running.get() && readCount < 20) {
                    readCount++;
                    String key = keys.get(readCount % keys.size());
                    long invoke = System.nanoTime();
                    try {
                        Optional<String> res = client.getString(key).get(800, TimeUnit.MILLISECONDS);
                        long ret = System.nanoTime();
                        trace.recordRead(clientId, key, res.orElse(null), invoke, ret);
                    } catch (Exception e) {
                        // Skip unacknowledged reads during failover disconnection
                    }
                    try { Thread.sleep(15); } catch (InterruptedException ignored) {}
                }
            }));
        }

        // 3. Inject leader failover in the middle of execution
        Thread.sleep(150);
        node1.stop();
        transport1.stop();
        activeNodes.remove(id1);

        // Wait for surviving quorum (node2 or node3) to elect new leader
        await().atMost(4, TimeUnit.SECONDS).until(() ->
                node2.role() == RaftRole.LEADER || node3.role() == RaftRole.LEADER
        );

        // Allow post-failover writes to continue
        Thread.sleep(400);
        running.set(false);

        for (Future<?> f : tasks) {
            f.get(5, TimeUnit.SECONDS);
        }
        executor.shutdownNow();

        // 4. Verify linearizability of the recorded history
        assertThat(trace.size()).isGreaterThan(20);
        LinearizabilityResult result = trace.verifyLinearizability();

        assertThat(result.isLinearizable())
                .as("Cluster history must be strictly linearizable: " + result.violationDetails())
                .isTrue();
        assertThat(result.witnessExecution()).isNotEmpty();
    }
}
