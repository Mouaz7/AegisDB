package se.mouaz.aegisdb.integration;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.chaos.linearizability.LinearizabilityResult;
import se.mouaz.aegisdb.chaos.linearizability.OperationStatus;
import se.mouaz.aegisdb.chaos.linearizability.OperationTrace;
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
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Tag;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Tag("stress")
@DisplayName("Configurable Long-Running Stress and Soak Testing Suite")
class LongRunningStressTest {

    private static final Logger log = LoggerFactory.getLogger(LongRunningStressTest.class);

    private final NodeId id1 = NodeId.of("stress-node-1");
    private final NodeId id2 = NodeId.of("stress-node-2");
    private final NodeId id3 = NodeId.of("stress-node-3");

    private InMemoryTransport transport1;
    private InMemoryTransport transport2;
    private InMemoryTransport transport3;

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

        ClusterConfiguration config = ClusterConfiguration.builder()
                .clusterId("stress-cluster")
                .addMember(id1, Endpoint.of("127.0.0.1", 18001))
                .addMember(id2, Endpoint.of("127.0.0.1", 18002))
                .addMember(id3, Endpoint.of("127.0.0.1", 18003))
                .build();

        transport1 = new InMemoryTransport(id1);
        transport2 = new InMemoryTransport(id2);
        transport3 = new InMemoryTransport(id3);

        transport1.start();
        transport2.start();
        transport3.start();

        node1 = RaftNode.builder().nodeId(id1).clusterConfig(config).transport(transport1)
                .persistentState(new PersistentRaftState()).stateMachine(new KeyValueStateMachine())
                .minElectionTimeout(Duration.ofMillis(80)).maxElectionTimeout(Duration.ofMillis(120))
                .heartbeatInterval(Duration.ofMillis(25)).random(new Random(601)).build();

        node2 = RaftNode.builder().nodeId(id2).clusterConfig(config).transport(transport2)
                .persistentState(new PersistentRaftState()).stateMachine(new KeyValueStateMachine())
                .minElectionTimeout(Duration.ofMillis(200)).maxElectionTimeout(Duration.ofMillis(280))
                .heartbeatInterval(Duration.ofMillis(25)).random(new Random(602)).build();

        node3 = RaftNode.builder().nodeId(id3).clusterConfig(config).transport(transport3)
                .persistentState(new PersistentRaftState()).stateMachine(new KeyValueStateMachine())
                .minElectionTimeout(Duration.ofMillis(300)).maxElectionTimeout(Duration.ofMillis(400))
                .heartbeatInterval(Duration.ofMillis(25)).random(new Random(603)).build();

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
        for (RaftNode n : List.of(node1, node2, node3)) {
            if (n != null) {
                try { n.stop(); } catch (Exception ignored) {}
            }
        }
        for (InMemoryTransport t : List.of(transport1, transport2, transport3)) {
            if (t != null) {
                try { t.stop(); } catch (Exception ignored) {}
            }
        }
        InMemoryTransport.clearRegistry();
        RaftInvariants.clearInvariantTracking();
    }

    @Test
    @DisplayName("Configurable soak load test: verifies zero corruption, forward progress and linearizability")
    void configurableSoakTest() throws Exception {
        int durationSec = Integer.getInteger("aegisdb.stress.durationSeconds", 4);
        int targetOpsPerSec = Integer.getInteger("aegisdb.stress.targetOpsPerSec", 200);

        log.info("Starting Soak Test: duration={}s, targetRate={} ops/sec", durationSec, targetOpsPerSec);

        int clientThreads = 4;
        ExecutorService executor = Executors.newFixedThreadPool(clientThreads);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong successfulOps = new AtomicLong(0);
        AtomicLong indeterminateOps = new AtomicLong(0);

        List<String> keyPool = List.of("hot-key-1", "hot-key-2", "hot-key-3");
        List<Future<?>> clientTasks = new ArrayList<>();

        for (int t = 0; t < clientThreads; t++) {
            final String clientId = "soak-client-" + t;
            final int threadIdx = t;
            clientTasks.add(executor.submit(() -> {
                int opIndex = 0;
                while (running.get()) {
                    opIndex++;
                    String key = keyPool.get(opIndex % keyPool.size());
                    String value = "val-" + threadIdx + "-" + opIndex;

                    long invoke = System.nanoTime();
                    boolean isRead = (opIndex % 3 == 0);
                    try {
                        if (isRead) {
                            Optional<String> observed = client.getString(key).get(1, TimeUnit.SECONDS);
                            long ret = System.nanoTime();
                            trace.recordRead(clientId, key, observed.orElse(null), invoke, ret);
                        } else {
                            client.putString(key, value).get(1, TimeUnit.SECONDS);
                            long ret = System.nanoTime();
                            trace.recordWrite(clientId, key, value, invoke, ret, OperationStatus.OK);
                        }
                        successfulOps.incrementAndGet();
                    } catch (TimeoutException | ExecutionException e) {
                        long ret = System.nanoTime();
                        if (!isRead) {
                            trace.recordWrite(clientId, key, value, invoke, ret, OperationStatus.TIMEOUT_INDETERMINATE);
                        }
                        indeterminateOps.incrementAndGet();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }

                    // Rate limiting throttle
                    try {
                        Thread.sleep(Math.max(1, 1000 / (targetOpsPerSec / clientThreads)));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }));
        }

        // Run for configured duration
        Thread.sleep(durationSec * 1000L);
        running.set(false);

        for (Future<?> f : clientTasks) {
            f.get(5, TimeUnit.SECONDS);
        }
        executor.shutdownNow();

        log.info("Soak test finished: completedOps={}, indeterminateOps={}, totalRecorded={}",
                successfulOps.get(), indeterminateOps.get(), trace.size());

        // Assertions evaluate progress and invariants
        assertThat(successfulOps.get())
                .as("Cluster must maintain forward progress during stress load")
                .isGreaterThan(10);

        LinearizabilityResult result = trace.verifyLinearizability();
        assertThat(result.isLinearizable())
                .as("Execution history under stress must be linearizable: " + result.violationDetails())
                .isTrue();
    }
}
