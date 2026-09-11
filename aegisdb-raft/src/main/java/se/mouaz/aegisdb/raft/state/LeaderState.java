package se.mouaz.aegisdb.raft.state;

import se.mouaz.aegisdb.common.NodeId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Volatile Raft state on leaders (Section 17).
 * Reinitialized after election.
 */
public class LeaderState {
    private final Map<NodeId, Long> nextIndex = new ConcurrentHashMap<>();
    private final Map<NodeId, Long> matchIndex = new ConcurrentHashMap<>();

    public void initialize(Iterable<NodeId> peers, long lastLogIndex) {
        nextIndex.clear();
        matchIndex.clear();
        for (NodeId peer : peers) {
            nextIndex.put(peer, lastLogIndex + 1);
            matchIndex.put(peer, 0L);
        }
    }

    public long getNextIndex(NodeId peer) {
        return nextIndex.getOrDefault(peer, 1L);
    }

    public void setNextIndex(NodeId peer, long index) {
        nextIndex.put(peer, index);
    }

    public long getMatchIndex(NodeId peer) {
        return matchIndex.getOrDefault(peer, 0L);
    }

    public void setMatchIndex(NodeId peer, long index) {
        matchIndex.put(peer, index);
    }

    public Map<NodeId, Long> allMatchIndexes() {
        return Map.copyOf(matchIndex);
    }
}
