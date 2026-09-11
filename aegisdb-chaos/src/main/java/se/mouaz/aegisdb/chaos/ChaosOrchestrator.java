package se.mouaz.aegisdb.chaos;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.common.NodeStatus;
import se.mouaz.aegisdb.node.DatabaseNode;
import se.mouaz.aegisdb.raft.state.RaftRole;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Coordinates failure injection across an active AegisDB cluster.
 * Enables reproducible research experiments (US016) by combining
 * node lifecycle faults (leader/follower kills) with network anomalies (partition, drop, delay, duplicate).
 */
public class ChaosOrchestrator {
    private static final Logger log = LoggerFactory.getLogger(ChaosOrchestrator.class);

    private final Map<NodeId, DatabaseNode> activeNodes = new ConcurrentHashMap<>();
    private final Map<NodeId, FaultyTransport> transports = new ConcurrentHashMap<>();
    private final long seed;
    private final Random random;

    public ChaosOrchestrator(long seed) {
        this.seed = seed;
        this.random = new Random(seed);
    }

    public ChaosOrchestrator() {
        this(System.currentTimeMillis());
    }

    public void registerNode(DatabaseNode node, FaultyTransport transport) {
        activeNodes.put(node.nodeId(), node);
        transports.put(node.nodeId(), transport);
    }

    public void unregisterNode(NodeId nodeId) {
        activeNodes.remove(nodeId);
        transports.remove(nodeId);
    }

    public long seed() {
        return seed;
    }

    public Optional<NodeId> findLeader() {
        for (DatabaseNode node : activeNodes.values()) {
            if (node.status() == NodeStatus.RUNNING && node.raftNode().isPresent()) {
                if (node.raftNode().get().role() == RaftRole.LEADER) {
                    return Optional.of(node.nodeId());
                }
            }
        }
        return Optional.empty();
    }

    public List<NodeId> findFollowers() {
        List<NodeId> followers = new ArrayList<>();
        for (DatabaseNode node : activeNodes.values()) {
            if (node.status() == NodeStatus.RUNNING && node.raftNode().isPresent()) {
                if (node.raftNode().get().role() == RaftRole.FOLLOWER) {
                    followers.add(node.nodeId());
                }
            }
        }
        return followers;
    }

    public synchronized Optional<NodeId> killLeader() {
        Optional<NodeId> leaderOpt = findLeader();
        leaderOpt.ifPresent(leaderId -> {
            log.warn("[CHAOS] Killing active cluster leader: {}", leaderId);
            DatabaseNode node = activeNodes.get(leaderId);
            if (node != null) {
                node.stop();
            }
        });
        return leaderOpt;
    }

    public synchronized Optional<NodeId> killRandomFollower() {
        List<NodeId> followers = findFollowers();
        if (followers.isEmpty()) {
            return Optional.empty();
        }
        NodeId victim = followers.get(random.nextInt(followers.size()));
        log.warn("[CHAOS] Killing follower: {}", victim);
        DatabaseNode node = activeNodes.get(victim);
        if (node != null) {
            node.stop();
        }
        return Optional.of(victim);
    }

    public synchronized void killNode(NodeId nodeId) {
        DatabaseNode node = activeNodes.get(nodeId);
        if (node != null) {
            log.warn("[CHAOS] Stopping node: {}", nodeId);
            node.stop();
        }
    }

    public synchronized void restartNode(NodeId nodeId) throws Exception {
        DatabaseNode node = activeNodes.get(nodeId);
        if (node != null) {
            log.info("[CHAOS] Restarting node: {}", nodeId);
            node.start();
        }
    }

    public synchronized void createPartition(Set<NodeId> groupA, Set<NodeId> groupB) {
        log.warn("[CHAOS] Creating network partition between {} and {}", groupA, groupB);
        for (NodeId nodeA : groupA) {
            FaultyTransport tA = transports.get(nodeA);
            if (tA != null) {
                for (NodeId nodeB : groupB) {
                    tA.addRule(FaultRule.partition("part-" + nodeA + "-" + nodeB, n -> Objects.equals(n, nodeA), n -> Objects.equals(n, nodeB)));
                }
            }
        }
        for (NodeId nodeB : groupB) {
            FaultyTransport tB = transports.get(nodeB);
            if (tB != null) {
                for (NodeId nodeA : groupA) {
                    tB.addRule(FaultRule.partition("part-" + nodeB + "-" + nodeA, n -> Objects.equals(n, nodeB), n -> Objects.equals(n, nodeA)));
                }
            }
        }
    }

    public synchronized void isolateNode(NodeId isolatedNode) {
        Set<NodeId> isolated = Set.of(isolatedNode);
        Set<NodeId> rest = new HashSet<>(activeNodes.keySet());
        rest.remove(isolatedNode);
        createPartition(isolated, rest);
    }

    public synchronized void healPartitions() {
        log.info("[CHAOS] Healing all network partitions across cluster");
        for (FaultyTransport t : transports.values()) {
            for (FaultRule r : t.rules()) {
                if (r.type() == FaultType.PARTITION) {
                    t.removeRule(r.id());
                }
            }
        }
    }

    public synchronized void injectGlobalPacketDrop(double probability) {
        log.warn("[CHAOS] Injecting global packet drop rate: {}%", (int) (probability * 100));
        for (FaultyTransport t : transports.values()) {
            t.addRule(FaultRule.builder("global-drop-" + t.localNodeId(), FaultType.DROP)
                    .probability(probability)
                    .build());
        }
    }

    public synchronized void injectGlobalDelay(Duration delay) {
        log.warn("[CHAOS] Injecting global packet delay: {}ms", delay.toMillis());
        for (FaultyTransport t : transports.values()) {
            t.addRule(FaultRule.builder("global-delay-" + t.localNodeId(), FaultType.DELAY)
                    .delay(delay)
                    .probability(1.0)
                    .build());
        }
    }

    public synchronized void injectGlobalDuplication(double probability) {
        log.warn("[CHAOS] Injecting packet duplication rate: {}%", (int) (probability * 100));
        for (FaultyTransport t : transports.values()) {
            t.addRule(FaultRule.builder("global-dup-" + t.localNodeId(), FaultType.DUPLICATE)
                    .probability(probability)
                    .duplicateCount(1)
                    .build());
        }
    }

    public synchronized void clearAllFaults() {
        log.info("[CHAOS] Clearing all network fault rules");
        for (FaultyTransport t : transports.values()) {
            t.clearRules();
        }
    }

    public Map<String, Long> aggregateFaultMetrics() {
        long drops = 0;
        long delays = 0;
        long dups = 0;
        long partitions = 0;

        for (FaultyTransport t : transports.values()) {
            drops += t.droppedMessagesCount();
            delays += t.delayedMessagesCount();
            dups += t.duplicatedMessagesCount();
            partitions += t.partitionBlockedCount();
        }

        Map<String, Long> metrics = new HashMap<>();
        metrics.put("dropped_messages", drops);
        metrics.put("delayed_messages", delays);
        metrics.put("duplicated_messages", dups);
        metrics.put("partition_blocked", partitions);
        return metrics;
    }
}
