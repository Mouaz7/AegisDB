package se.mouaz.aegisdb.transaction;

import se.mouaz.aegisdb.common.ClientId;
import se.mouaz.aegisdb.common.RequestId;
import se.mouaz.aegisdb.common.TransactionId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Thread-safe registry tracking in-flight and committed transaction contexts, idempotency records,
 * and timeout monitoring (Master Project Plan §9 & §10).
 */
public class TransactionRegistry {
    private final ConcurrentMap<TransactionId, TransactionContext> inFlight = new ConcurrentHashMap<>();
    private final ConcurrentMap<TransactionId, Long> committedOutcomes = new ConcurrentHashMap<>();
    private final ConcurrentMap<TransactionId, Long> abortedOutcomes = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, TransactionId> deduplicationMap = new ConcurrentHashMap<>();

    public void register(TransactionContext context) {
        Objects.requireNonNull(context, "context must not be null");
        inFlight.put(context.id(), context);

        if (context.clientId().isPresent() && context.requestId().isPresent()) {
            String deduplicationKey = buildKey(context.clientId().get(), context.requestId().get());
            deduplicationMap.put(deduplicationKey, context.id());
        }
    }

    public Optional<TransactionContext> get(TransactionId id) {
        return Optional.ofNullable(inFlight.get(id));
    }

    public Optional<TransactionId> findByClientRequest(ClientId clientId, RequestId requestId) {
        String key = buildKey(clientId, requestId);
        return Optional.ofNullable(deduplicationMap.get(key));
    }

    public void markCommitted(TransactionId id, long commitTimestamp) {
        TransactionContext ctx = inFlight.remove(id);
        committedOutcomes.put(id, commitTimestamp);
    }

    public void markAborted(TransactionId id) {
        TransactionContext ctx = inFlight.remove(id);
        abortedOutcomes.put(id, System.currentTimeMillis());
    }

    public boolean isCommitted(TransactionId id) {
        return committedOutcomes.containsKey(id);
    }

    public boolean isAborted(TransactionId id) {
        return abortedOutcomes.containsKey(id);
    }

    public Optional<Long> getCommitTimestamp(TransactionId id) {
        return Optional.ofNullable(committedOutcomes.get(id));
    }

    public int activeCount() {
        return inFlight.size();
    }

    public List<TransactionContext> listActive() {
        return Collections.unmodifiableList(new ArrayList<>(inFlight.values()));
    }

    public List<TransactionContext> sweepExpired(long ttlMillis) {
        long now = System.currentTimeMillis();
        List<TransactionContext> expired = new ArrayList<>();
        for (TransactionContext ctx : inFlight.values()) {
            if (ctx.isExpired(now, ttlMillis)) {
                expired.add(ctx);
            }
        }
        return expired;
    }

    public void clear() {
        inFlight.clear();
        committedOutcomes.clear();
        abortedOutcomes.clear();
        deduplicationMap.clear();
    }

    private static String buildKey(ClientId clientId, RequestId requestId) {
        return clientId.value() + "::" + requestId.sequenceNumber();
    }
}
