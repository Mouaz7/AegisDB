package se.mouaz.aegisdb.transaction;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Tracks keys read by a transaction and their observed commit timestamps (Master Project Plan §9 & §18).
 * Enables serializable conflict detection (write skew and anti-dependency checks).
 */
public class ReadSet {
    private final ConcurrentMap<String, Long> reads = new ConcurrentHashMap<>();

    public void recordRead(String key, long commitTimestamp) {
        Objects.requireNonNull(key, "key must not be null");
        reads.putIfAbsent(key, commitTimestamp);
    }

    public Long getObservedTimestamp(String key) {
        return reads.get(key);
    }

    public boolean contains(String key) {
        return reads.containsKey(key);
    }

    public Map<String, Long> entries() {
        return Collections.unmodifiableMap(reads);
    }

    public int size() {
        return reads.size();
    }

    public boolean isEmpty() {
        return reads.isEmpty();
    }

    public void clear() {
        reads.clear();
    }
}
