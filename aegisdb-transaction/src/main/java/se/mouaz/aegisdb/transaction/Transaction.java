package se.mouaz.aegisdb.transaction;

import se.mouaz.aegisdb.common.TransactionId;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Public transactional handle for executing operations atomically (Master Project Plan §9 & §18; US012).
 */
public interface Transaction extends Closeable {
    TransactionId id();
    TransactionState state();
    IsolationLevel isolationLevel();

    Optional<byte[]> get(String key);

    default Optional<String> getString(String key) {
        return get(key).map(bytes -> new String(bytes, StandardCharsets.UTF_8));
    }

    void put(String key, byte[] value);

    default void putString(String key, String value) {
        put(key, value != null ? value.getBytes(StandardCharsets.UTF_8) : new byte[0]);
    }

    void delete(String key);

    void prepare();
    long commit();
    void abort();

    @Override
    default void close() {
        if (!state().isTerminal()) {
            abort();
        }
    }
}
