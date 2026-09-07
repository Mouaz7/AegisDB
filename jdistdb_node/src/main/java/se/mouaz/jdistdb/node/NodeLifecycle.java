package se.mouaz.jdistdb.node;

import se.mouaz.jdistdb.common.NodeStatus;

public interface NodeLifecycle extends AutoCloseable {
    void start() throws Exception;
    void stop();
    NodeStatus status();

    @Override
    default void close() {
        stop();
    }
}
