package se.mouaz.aegisdb.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ShardIdTest {

    @Test
    void testValidShardId() {
        ShardId shard0 = ShardId.of("shard-0");
        assertThat(shard0.value()).isEqualTo("shard-0");
        assertThat(shard0.toString()).isEqualTo("shard-0");

        ShardId shardByIndex = ShardId.of(3);
        assertThat(shardByIndex.value()).isEqualTo("shard-3");
    }

    @Test
    void testShardIdValidation() {
        assertThatThrownBy(() -> new ShardId(null))
                .isInstanceOf(NullPointerException.class);

        assertThatThrownBy(() -> new ShardId("   "))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> ShardId.of(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testComparableAndEquality() {
        ShardId s1 = ShardId.of("shard-1");
        ShardId s2 = ShardId.of("shard-2");
        ShardId s1Duplicate = ShardId.of("shard-1");

        assertThat(s1).isEqualTo(s1Duplicate);
        assertThat(s1.hashCode()).isEqualTo(s1Duplicate.hashCode());
        assertThat(s1.compareTo(s2)).isLessThan(0);
        assertThat(s2.compareTo(s1)).isGreaterThan(0);
        assertThat(s1.compareTo(s1Duplicate)).isZero();
    }
}
