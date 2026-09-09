package se.mouaz.aegisdb.sharding;

import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.common.ShardId;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ShardRouterTest {

    @Test
    void testRouteKeyToShardAndReplicationGroup() {
        ShardMap map = new ShardMap();
        ShardId s0 = ShardId.of(0);
        ShardId s1 = ShardId.of(1);

        ReplicationGroup g0 = ReplicationGroup.of(s0, NodeId.of("n1"), NodeId.of("n2"));
        ReplicationGroup g1 = ReplicationGroup.of(s1, NodeId.of("n3"), NodeId.of("n4"));

        map.registerShard(Shard.of(s0, g0));
        map.registerShard(Shard.of(s1, g1));

        ShardRouter router = new ShardRouter(map);

        String key = "test:user:42";
        ShardId targetShardId = router.routeToShardId(key);
        Shard targetShard = router.route(key);
        ReplicationGroup targetGroup = router.routeToReplicationGroup(key);

        assertThat(targetShard.id()).isEqualTo(targetShardId);
        assertThat(targetGroup.shardId()).isEqualTo(targetShardId);
        assertThat(targetGroup.members()).isNotEmpty();
    }
}
