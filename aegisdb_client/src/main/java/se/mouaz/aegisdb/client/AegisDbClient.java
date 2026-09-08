package se.mouaz.aegisdb.client;

import se.mouaz.aegisdb.common.NodeId;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Dedicated Java SDK client interface for AegisDB (Master Project Plan §5, §18; US009, US010; Milestone M2).
 * Provides asynchronous, non-blocking Key-Value operations (PUT, GET, DELETE)
 * with automatic leader discovery, redirect handling, and exponential backoff retry.
 */
public interface AegisDbClient extends Closeable {

    /**
     * Stores a key-value mapping durably across the replicated cluster.
     */
    CompletableFuture<Void> put(String key, byte[] value);

    /**
     * Stores a UTF-8 string value for convenience.
     */
    default CompletableFuture<Void> putString(String key, String value) {
        return put(key, value != null ? value.getBytes(StandardCharsets.UTF_8) : new byte[0]);
    }

    /**
     * Retrieves the value associated with key, returning empty Optional if key not found.
     */
    CompletableFuture<Optional<byte[]>> get(String key);

    /**
     * Retrieves UTF-8 string value for convenience.
     */
    default CompletableFuture<Optional<String>> getString(String key) {
        return get(key).thenApply(opt -> opt.map(bytes -> new String(bytes, StandardCharsets.UTF_8)));
    }

    /**
     * Deletes a key-value mapping from the database.
     */
    CompletableFuture<Optional<byte[]>> delete(String key);

    /**
     * Returns the currently tracked cluster leader, if known.
     */
    Optional<NodeId> currentLeader();

    @Override
    void close();
}
