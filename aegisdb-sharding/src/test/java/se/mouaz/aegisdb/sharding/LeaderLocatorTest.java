package se.mouaz.aegisdb.sharding;

import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.common.ShardId;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class LeaderLocatorTest {

    @Test
    void testUpdateAndRetrieveLeader() {
        DefaultLeaderLocator locator = new DefaultLeaderLocator();
        ShardId shard0 = ShardId.of(0);
        NodeId node1 = NodeId.of("node-1");

        assertThat(locator.getLeader(shard0)).isEmpty();

        locator.updateLeader(shard0, node1);
        assertThat(locator.getLeader(shard0)).contains(node1);
        assertThat(locator.allLeaders()).containsEntry(shard0, node1);
    }

    @Test
    void testInvalidateLeaderStaleCheck() {
        DefaultLeaderLocator locator = new DefaultLeaderLocator();
        ShardId shard0 = ShardId.of(0);
        NodeId node1 = NodeId.of("node-1");
        NodeId node2 = NodeId.of("node-2");

        locator.updateLeader(shard0, node1);

        // Attempt invalidation with wrong suspected node
        locator.invalidateLeader(shard0, node2);
        assertThat(locator.getLeader(shard0)).contains(node1);

        // Invalidation with matching suspected node
        locator.invalidateLeader(shard0, node1);
        assertThat(locator.getLeader(shard0)).isEmpty();
    }

    @Test
    void testLeaderChangeListenerNotification() {
        DefaultLeaderLocator locator = new DefaultLeaderLocator();
        ShardId shard0 = ShardId.of(0);
        NodeId node1 = NodeId.of("node-1");
        NodeId node2 = NodeId.of("node-2");

        AtomicInteger changeCount = new AtomicInteger(0);
        locator.addListener((shardId, oldLeader, newLeader) -> {
            if (shardId.equals(shard0)) {
                changeCount.incrementAndGet();
            }
        });

        locator.updateLeader(shard0, node1);
        locator.updateLeader(shard0, node1); // same leader, no duplicate event
        locator.updateLeader(shard0, node2);
        locator.invalidateLeader(shard0);

        assertThat(changeCount.get()).isEqualTo(3);
    }
}
