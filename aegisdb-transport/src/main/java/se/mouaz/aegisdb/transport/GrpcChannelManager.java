package se.mouaz.aegisdb.transport;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class GrpcChannelManager implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(GrpcChannelManager.class);

    private final Map<NodeId, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final boolean useTls;

    public GrpcChannelManager() {
        this(false); // Default to explicit plaintext in dev/test
    }

    public GrpcChannelManager(boolean useTls) {
        this.useTls = useTls;
    }

    public boolean isTlsEnabled() {
        return useTls;
    }

    public ManagedChannel getOrCreateChannel(NodeId destination, Endpoint endpoint) {
        return channels.compute(destination, (id, existing) -> {
            if (existing != null && !existing.isShutdown() && !existing.isTerminated()) {
                return existing;
            }
            log.debug("Creating new gRPC channel to node {} at {} (TLS: {})", id, endpoint, useTls);
            ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forAddress(endpoint.host(), endpoint.port());
            if (useTls) {
                builder.useTransportSecurity();
            } else {
                builder.usePlaintext();
            }
            return builder.build();
        });
    }

    public void closeChannel(NodeId destination) {
        ManagedChannel channel = channels.remove(destination);
        if (channel != null && !channel.isShutdown()) {
            try {
                channel.shutdown().awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
        }
    }

    @Override
    public void close() {
        channels.forEach((nodeId, channel) -> {
            if (!channel.isShutdown()) {
                channel.shutdown();
            }
        });
        channels.forEach((nodeId, channel) -> {
            try {
                if (!channel.awaitTermination(2, TimeUnit.SECONDS)) {
                    channel.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
        });
        channels.clear();
    }
}
