package se.mouaz.aegisdb.transaction;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Tracks uncommitted mutations performed by a transaction (Master Project Plan §9 & §18).
 * Provides Read-Your-Own-Writes semantics within transaction boundaries and enforces
 * bounded allocation limits (Master Project Plan §12).
 */
public class WriteSet {
    private final ConcurrentMap<String, WriteOperation> writes = new ConcurrentHashMap<>();

    public void put(String key, byte[] value, int maxSize) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        checkCapacity(key, maxSize);
        writes.put(key, WriteOperation.put(key, value));
    }

    public void delete(String key, int maxSize) {
        Objects.requireNonNull(key, "key must not be null");
        checkCapacity(key, maxSize);
        writes.put(key, WriteOperation.delete(key));
    }

    private void checkCapacity(String key, int maxSize) {
        if (!writes.containsKey(key) && writes.size() >= maxSize) {
            throw new TransactionSizeLimitException(
                    "Transaction write set exceeded maximum size limit of " + maxSize + " keys"
            );
        }
    }

    public Optional<WriteOperation> get(String key) {
        return Optional.ofNullable(writes.get(key));
    }

    public boolean contains(String key) {
        return writes.containsKey(key);
    }

    public Set<String> keys() {
        return Collections.unmodifiableSet(writes.keySet());
    }

    public Map<String, WriteOperation> operations() {
        return Collections.unmodifiableMap(writes);
    }

    public int size() {
        return writes.size();
    }

    public boolean isEmpty() {
        return writes.isEmpty();
    }

    public void clear() {
        writes.clear();
    }
}
