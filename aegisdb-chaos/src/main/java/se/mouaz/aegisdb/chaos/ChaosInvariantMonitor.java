package se.mouaz.aegisdb.chaos;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.common.NodeStatus;
import se.mouaz.aegisdb.node.DatabaseNode;
import se.mouaz.aegisdb.raft.RaftNode;
import se.mouaz.aegisdb.raft.state.RaftRole;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Continuous safety invariant monitor running concurrently with chaos injection (US016).
 * Enforces Master Plan §7, §10, and §14 consensus and transactional invariants:
 * 1. At most one leader per term.
 * 2. Monotonic non-decreasing terms.
 * 3. Committed log prefix equality.
 * 4. Financial conservation invariant (A + B + C = 3000).
 */
public class ChaosInvariantMonitor {
    private static final Logger log = LoggerFactory.getLogger(ChaosInvariantMonitor.class);

    private final Map<NodeId, Long> maxObservedTerms = new ConcurrentHashMap<>();
    private final AtomicLong assertionsChecked = new AtomicLong(0);
    private final List<String> violations = Collections.synchronizedList(new ArrayList<>());

    /**
     * Asserts that at most one node is LEADER in any given term.
     */
    public void assertElectionSafety(Collection<DatabaseNode> nodes) {
        assertionsChecked.incrementAndGet();
        Map<Long, List<NodeId>> leadersPerTerm = new HashMap<>();

        for (DatabaseNode node : nodes) {
            if (node.status() == NodeStatus.RUNNING && node.raftNode().isPresent()) {
                RaftNode raft = node.raftNode().get();
                if (raft.role() == RaftRole.LEADER) {
                    long term = raft.currentTerm();
                    leadersPerTerm.computeIfAbsent(term, k -> new ArrayList<>()).add(node.nodeId());
                }
            }
        }

        for (Map.Entry<Long, List<NodeId>> entry : leadersPerTerm.entrySet()) {
            if (entry.getValue().size() > 1) {
                String error = String.format("ELECTION SAFETY VIOLATION: Multiple leaders %s elected in term %d",
                        entry.getValue(), entry.getKey());
                violations.add(error);
                log.error(error);
                throw new AssertionError(error);
            }
        }
    }

    /**
     * Asserts that a node's term never decreases.
     */
    public void assertTermMonotonicity(NodeId nodeId, long currentTerm) {
        assertionsChecked.incrementAndGet();
        maxObservedTerms.compute(nodeId, (k, prevMax) -> {
            if (prevMax != null && currentTerm < prevMax) {
                String error = String.format("TERM MONOTONICITY VIOLATION: Node %s term decreased from %d to %d",
                        nodeId, prevMax, currentTerm);
                violations.add(error);
                log.error(error);
                throw new AssertionError(error);
            }
            return prevMax == null ? currentTerm : Math.max(prevMax, currentTerm);
        });
    }

    /**
     * Asserts that committed log prefixes match across nodes.
     */
    public void assertLogPrefixConsistency(Collection<DatabaseNode> nodes) {
        assertionsChecked.incrementAndGet();
        List<DatabaseNode> runningNodes = nodes.stream()
                .filter(n -> n.status() == NodeStatus.RUNNING && n.raftNode().isPresent())
                .toList();

        if (runningNodes.size() < 2) {
            return;
        }

        for (int i = 0; i < runningNodes.size(); i++) {
            for (int j = i + 1; j < runningNodes.size(); j++) {
                RaftNode nodeA = runningNodes.get(i).raftNode().get();
                RaftNode nodeB = runningNodes.get(j).raftNode().get();

                long commitA = nodeA.commitIndex();
                long commitB = nodeB.commitIndex();
                long commonCommit = Math.min(commitA, commitB);

                for (long idx = 1; idx <= commonCommit; idx++) {
                    var entryA = nodeA.log().getEntry(idx);
                    var entryB = nodeB.log().getEntry(idx);

                    if (entryA.isPresent() && entryB.isPresent()) {
                        if (entryA.get().term() != entryB.get().term()) {
                            String error = String.format("LOG PREFIX VIOLATION: Nodes %s and %s diverge at committed index %d (term %d vs %d)",
                                    runningNodes.get(i).nodeId(), runningNodes.get(j).nodeId(),
                                    idx, entryA.get().term(), entryB.get().term());
                            violations.add(error);
                            log.error(error);
                            throw new AssertionError(error);
                        }
                    }
                }
            }
        }
    }

    /**
     * Asserts the financial conservation invariant: A + B + C = 3000.
     */
    public void assertBankInvariant(long a, long b, long c, long expectedTotal) {
        assertionsChecked.incrementAndGet();
        long actualTotal = a + b + c;
        if (actualTotal != expectedTotal) {
            String error = String.format("BANK INVARIANT VIOLATION: Expected sum %d, but found A=%d, B=%d, C=%d (sum=%d)",
                    expectedTotal, a, b, c, actualTotal);
            violations.add(error);
            log.error(error);
            throw new AssertionError(error);
        }
    }

    public long assertionsChecked() {
        return assertionsChecked.get();
    }

    public List<String> violations() {
        return List.copyOf(violations);
    }

    public boolean hasViolations() {
        return !violations.isEmpty();
    }

    public void reset() {
        assertionsChecked.set(0);
        violations.clear();
        maxObservedTerms.clear();
    }
}
