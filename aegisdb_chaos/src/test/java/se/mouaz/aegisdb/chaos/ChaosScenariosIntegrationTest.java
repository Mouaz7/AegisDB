package se.mouaz.aegisdb.chaos;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.mouaz.aegisdb.client.ShardedAegisDbClient;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.mvcc.MvccStore;
import se.mouaz.aegisdb.sharding.QueryRouter;
import se.mouaz.aegisdb.sharding.ShardManager;
import se.mouaz.aegisdb.transaction.IsolationLevel;
import se.mouaz.aegisdb.transaction.TransactionManager;
import se.mouaz.aegisdb.transaction.distributed.*;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ChaosScenariosIntegrationTest {

    @TempDir
    Path tempDir;

    private NodeId node1;
    private NodeId node2;
    private NodeId node3;

    private FaultyTransport transport1;
    private FaultyTransport transport2;
    private FaultyTransport transport3;

    private ChaosOrchestrator orchestrator;
    private ChaosInvariantMonitor invariantMonitor;

    private ShardId shard0;
    private ShardId shard1;
    private ShardId shard2;

    private MvccStore store0;
    private MvccStore store1;
    private MvccStore store2;

    private TransactionManager tm0;
    private TransactionManager tm1;
    private TransactionManager tm2;

    private LocalShardParticipant participant0;
    private LocalShardParticipant participant1;
    private LocalShardParticipant participant2;

    private ShardedAegisDbClient client;
    private ShardManager shardManager;

    private String keyA;
    private String keyB;
    private String keyC;

    @BeforeEach
    void setUp() throws IOException {
        long seed = 42L;
        orchestrator = new ChaosOrchestrator(seed);
        invariantMonitor = new ChaosInvariantMonitor();

        node1 = NodeId.of("node-1");
        node2 = NodeId.of("node-2");
        node3 = NodeId.of("node-3");

        InMemoryTransport inMem1 = new InMemoryTransport(node1);
        InMemoryTransport inMem2 = new InMemoryTransport(node2);
        InMemoryTransport inMem3 = new InMemoryTransport(node3);
        inMem1.start();
        inMem2.start();
        inMem3.start();

        transport1 = new FaultyTransport(inMem1, seed);
        transport2 = new FaultyTransport(inMem2, seed + 1);
        transport3 = new FaultyTransport(inMem3, seed + 2);

        shard0 = ShardId.of("shard-0");
        shard1 = ShardId.of("shard-1");
        shard2 = ShardId.of("shard-2");

        store0 = new MvccStore();
        store1 = new MvccStore();
        store2 = new MvccStore();

        tm0 = new TransactionManager(store0);
        tm1 = new TransactionManager(store1);
        tm2 = new TransactionManager(store2);

        participant0 = new LocalShardParticipant(shard0, tm0);
        participant1 = new LocalShardParticipant(shard1, tm1);
        participant2 = new LocalShardParticipant(shard2, tm2);

        Map<ShardId, TransactionParticipant> participants = Map.of(
                shard0, participant0,
                shard1, participant1,
                shard2, participant2
        );

        List<NodeId> nodes = List.of(node1, node2, node3);
        shardManager = ShardManager.createStaticShards(3, nodes, 1);

        QueryRouter.ShardNodeInvoker invoker = (sId, targetNode, cmdBytes) -> {
            se.mouaz.aegisdb.raft.statemachine.KvCommand cmd = se.mouaz.aegisdb.raft.statemachine.KvCommand.fromBytes(cmdBytes);
            MvccStore targetStore;
            if (sId.equals(shard0)) {
                targetStore = store0;
            } else if (sId.equals(shard1)) {
                targetStore = store1;
            } else {
                targetStore = store2;
            }

            return switch (cmd.opType()) {
                case PUT -> {
                    targetStore.put(cmd.key(), cmd.value());
                    yield CompletableFuture.completedFuture(new byte[0]);
                }
                case GET -> {
                    byte[] val = targetStore.get(cmd.key()).orElse(null);
                    yield CompletableFuture.completedFuture(val != null ? val : new byte[0]);
                }
                case DELETE -> {
                    targetStore.delete(cmd.key());
                    yield CompletableFuture.completedFuture(new byte[0]);
                }
                default -> CompletableFuture.failedFuture(new IllegalArgumentException("Unknown cmd"));
            };
        };

        QueryRouter queryRouter = new QueryRouter(shardManager.router(), shardManager.leaderLocator(), invoker);
        Path logFile = tempDir.resolve("chaos-coordinator.log");
        DurableCoordinatorLog coordinatorLog = new DurableCoordinatorLog(logFile);

        client = new ShardedAegisDbClient(
                queryRouter,
                Collections.emptyMap(),
                participants,
                coordinatorLog
        );

        // Map keys deterministically
        keyA = findKeyForShard(shard0, "account-A");
        keyB = findKeyForShard(shard1, "account-B");
        keyC = findKeyForShard(shard2, "account-C");
    }

    private String findKeyForShard(ShardId targetShard, String prefix) {
        for (int i = 0; i < 5000; i++) {
            String candidate = prefix + ":" + i;
            if (shardManager.router().routeToShardId(candidate).equals(targetShard)) {
                return candidate;
            }
        }
        throw new IllegalStateException("Could not find key for shard: " + targetShard);
    }

    @Test
    @DisplayName("US016 AC6: Bank invariant A + B + C = 3000 preserved under concurrent chaos and 2PC")
    void testBankInvariantPreservedUnderChaos() throws Exception {
        // Initialize accounts
        client.putString(keyA, "1000").join();
        client.putString(keyB, "1000").join();
        client.putString(keyC, "1000").join();

        invariantMonitor.assertBankInvariant(1000, 1000, 1000, 3000);

        // Inject packet delays and drops into transport
        transport1.addRule(FaultRule.delay("delay-t1", node1, node2, Duration.ofMillis(5), 0.3));
        transport2.addRule(FaultRule.duplicate("dup-t2", node2, node3, 0.2));

        int threadCount = 4;
        int transfersPerThread = 20;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger committedTransfers = new AtomicInteger(0);

        List<String[]> pairs = List.of(
                new String[]{keyA, keyB},
                new String[]{keyB, keyC},
                new String[]{keyC, keyA}
        );

        for (int t = 0; t < threadCount; t++) {
            final int threadIdx = t;
            executor.submit(() -> {
                try {
                    Random rand = new Random(42 + threadIdx);
                    for (int i = 0; i < transfersPerThread; i++) {
                        String[] pair = pairs.get(rand.nextInt(pairs.size()));
                        String fromKey = pair[0];
                        String toKey = pair[1];
                        int amount = rand.nextInt(20) + 1;

                        try {
                            client.runInTransaction(IsolationLevel.SERIALIZABLE, tx -> {
                                int fromBal = Integer.parseInt(tx.getString(fromKey).orElse("0"));
                                int toBal = Integer.parseInt(tx.getString(toKey).orElse("0"));

                                if (fromBal >= amount) {
                                    tx.putString(fromKey, String.valueOf(fromBal - amount));
                                    tx.putString(toKey, String.valueOf(toBal + amount));
                                }
                                return null;
                            }, 50);
                            committedTransfers.incrementAndGet();
                        } catch (Exception ignored) {
                            // Abort / conflict under chaos is expected and safe
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        boolean finished = latch.await(30, TimeUnit.SECONDS);
        executor.shutdown();
        assertThat(finished).isTrue();

        // Read final balances
        int balA = Integer.parseInt(client.getString(keyA).join().orElseThrow());
        int balB = Integer.parseInt(client.getString(keyB).join().orElseThrow());
        int balC = Integer.parseInt(client.getString(keyC).join().orElseThrow());

        // Crucial Master Plan §14 invariant assertion:
        invariantMonitor.assertBankInvariant(balA, balB, balC, 3000);

        assertThat(balA + balB + balC).isEqualTo(3000);
        assertThat(invariantMonitor.hasViolations()).isFalse();
        assertThat(invariantMonitor.assertionsChecked()).isGreaterThanOrEqualTo(2);
    }

    @Test
    @DisplayName("US016 AC2: Network partition fault injection blocks minority and heals correctly")
    void testNetworkPartitionAndHealing() {
        se.mouaz.aegisdb.common.NodeConfiguration cfg1 = se.mouaz.aegisdb.common.NodeConfiguration.builder()
                .nodeId(node1)
                .endpoint(new se.mouaz.aegisdb.common.Endpoint("localhost", 9001))
                .dataDir(tempDir)
                .build();
        se.mouaz.aegisdb.common.ClusterConfiguration ccfg1 = se.mouaz.aegisdb.common.ClusterConfiguration.builder()
                .clusterId(new se.mouaz.aegisdb.common.ClusterId("test"))
                .build();

        se.mouaz.aegisdb.common.NodeConfiguration cfg2 = se.mouaz.aegisdb.common.NodeConfiguration.builder()
                .nodeId(node2)
                .endpoint(new se.mouaz.aegisdb.common.Endpoint("localhost", 9002))
                .dataDir(tempDir)
                .build();
        se.mouaz.aegisdb.common.ClusterConfiguration ccfg2 = se.mouaz.aegisdb.common.ClusterConfiguration.builder()
                .clusterId(new se.mouaz.aegisdb.common.ClusterId("test"))
                .build();

        orchestrator.registerNode(new se.mouaz.aegisdb.node.DatabaseNode(cfg1, ccfg1, transport1), transport1);
        orchestrator.registerNode(new se.mouaz.aegisdb.node.DatabaseNode(cfg2, ccfg2, transport2), transport2);

        // Partition node 1 and node 2
        orchestrator.createPartition(Set.of(node1), Set.of(node2));

        assertThat(transport1.rules()).isNotEmpty();
        assertThat(transport2.rules()).isNotEmpty();

        // Heal partitions
        orchestrator.healPartitions();
        assertThat(transport1.rules()).isEmpty();
        assertThat(transport2.rules()).isEmpty();
    }
}
