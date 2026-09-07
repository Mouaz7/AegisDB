package se.mouaz.aegisdb.node;

import se.mouaz.aegisdb.common.NodeStatus;

public interface NodeLifecycle extends AutoCloseable {
    void start() throws Exception;
    void stop();
    NodeStatus status();

    @Override
    default void close() {
        stop();
    }
}
