package se.mouaz.aegisdb.chaos;

import se.mouaz.aegisdb.common.NodeId;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Encapsulates a declarative fault injection rule evaluated by {@link FaultyTransport}.
 */
public class FaultRule {
    private final String id;
    private final FaultType type;
    private final Predicate<NodeId> sourceFilter;
    private final Predicate<NodeId> destinationFilter;
    private final Predicate<Class<?>> rpcTypeFilter;
    private final double probability;
    private final Duration delay;
    private final int duplicateCount;
    private volatile boolean enabled;

    public FaultRule(String id,
                     FaultType type,
                     Predicate<NodeId> sourceFilter,
                     Predicate<NodeId> destinationFilter,
                     Predicate<Class<?>> rpcTypeFilter,
                     double probability,
                     Duration delay,
                     int duplicateCount,
                     boolean enabled) {
        this.id = Objects.requireNonNull(id, "id cannot be null");
        this.type = Objects.requireNonNull(type, "type cannot be null");
        this.sourceFilter = sourceFilter != null ? sourceFilter : n -> true;
        this.destinationFilter = destinationFilter != null ? destinationFilter : n -> true;
        this.rpcTypeFilter = rpcTypeFilter != null ? rpcTypeFilter : c -> true;
        this.probability = Math.clamp(probability, 0.0, 1.0);
        this.delay = delay != null ? delay : Duration.ZERO;
        this.duplicateCount = Math.max(1, duplicateCount);
        this.enabled = enabled;
    }

    public static Builder builder(String id, FaultType type) {
        return new Builder(id, type);
    }

    public static FaultRule drop(String id, NodeId src, NodeId dst, double probability) {
        return builder(id, FaultType.DROP)
                .from(src)
                .to(dst)
                .probability(probability)
                .build();
    }

    public static FaultRule delay(String id, NodeId src, NodeId dst, Duration delay, double probability) {
        return builder(id, FaultType.DELAY)
                .from(src)
                .to(dst)
                .delay(delay)
                .probability(probability)
                .build();
    }

    public static FaultRule duplicate(String id, NodeId src, NodeId dst, double probability) {
        return builder(id, FaultType.DUPLICATE)
                .from(src)
                .to(dst)
                .probability(probability)
                .duplicateCount(1)
                .build();
    }

    public static FaultRule partition(String id, Predicate<NodeId> groupA, Predicate<NodeId> groupB) {
        return builder(id, FaultType.PARTITION)
                .between(groupA, groupB)
                .probability(1.0)
                .build();
    }

    public boolean matches(NodeId source, NodeId destination, Class<?> rpcClass) {
        if (!enabled) {
            return false;
        }
        return sourceFilter.test(source) && destinationFilter.test(destination) && rpcTypeFilter.test(rpcClass);
    }

    public String id() {
        return id;
    }

    public FaultType type() {
        return type;
    }

    public double probability() {
        return probability;
    }

    public Duration delay() {
        return delay;
    }

    public int duplicateCount() {
        return duplicateCount;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public String toString() {
        return "FaultRule{" +
                "id='" + id + '\'' +
                ", type=" + type +
                ", probability=" + probability +
                ", delay=" + delay +
                ", duplicateCount=" + duplicateCount +
                ", enabled=" + enabled +
                '}';
    }

    public static class Builder {
        private final String id;
        private final FaultType type;
        private Predicate<NodeId> sourceFilter = n -> true;
        private Predicate<NodeId> destinationFilter = n -> true;
        private Predicate<Class<?>> rpcTypeFilter = c -> true;
        private double probability = 1.0;
        private Duration delay = Duration.ZERO;
        private int duplicateCount = 1;
        private boolean enabled = true;

        public Builder(String id, FaultType type) {
            this.id = id;
            this.type = type;
        }

        public Builder from(NodeId src) {
            this.sourceFilter = n -> src == null || Objects.equals(n, src);
            return this;
        }

        public Builder to(NodeId dst) {
            this.destinationFilter = n -> dst == null || Objects.equals(n, dst);
            return this;
        }

        public Builder between(Predicate<NodeId> groupA, Predicate<NodeId> groupB) {
            this.sourceFilter = groupA;
            this.destinationFilter = groupB;
            return this;
        }

        public Builder forRpc(Class<?> rpcClass) {
            this.rpcTypeFilter = c -> rpcClass == null || rpcClass.isAssignableFrom(c);
            return this;
        }

        public Builder probability(double probability) {
            this.probability = probability;
            return this;
        }

        public Builder delay(Duration delay) {
            this.delay = delay;
            return this;
        }

        public Builder duplicateCount(int count) {
            this.duplicateCount = count;
            return this;
        }

        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        public FaultRule build() {
            return new FaultRule(id, type, sourceFilter, destinationFilter, rpcTypeFilter,
                    probability, delay, duplicateCount, enabled);
        }
    }
}
