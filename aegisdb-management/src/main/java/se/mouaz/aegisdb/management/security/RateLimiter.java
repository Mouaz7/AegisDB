package se.mouaz.aegisdb.management.security;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Token-bucket rate limiter defending the management plane against denial-of-service
 * and resource starvation attacks (Master Plan §12 Resource Exhaustion).
 */
public class RateLimiter {
    private final double refillRatePerSecond;
    private final double maxTokens;
    private final Map<String, TokenBucket> buckets = new ConcurrentHashMap<>();

    public RateLimiter(double refillRatePerSecond, double maxTokens) {
        this.refillRatePerSecond = refillRatePerSecond;
        this.maxTokens = maxTokens;
    }

    public static RateLimiter createDefault() {
        // 50 requests per second with burst capacity of 100
        return new RateLimiter(50.0, 100.0);
    }

    public boolean tryAcquire(String clientKey) {
        return tryAcquire(clientKey, 1.0);
    }

    public boolean tryAcquire(String clientKey, double tokensRequested) {
        if (clientKey == null || clientKey.isBlank()) {
            clientKey = "default";
        }
        TokenBucket bucket = buckets.computeIfAbsent(clientKey, k -> new TokenBucket(maxTokens));
        return bucket.tryConsume(tokensRequested, refillRatePerSecond, maxTokens);
    }

    public void reset() {
        buckets.clear();
    }

    private static class TokenBucket {
        private double tokens;
        private long lastRefillTime;

        public TokenBucket(double maxTokens) {
            this.tokens = maxTokens;
            this.lastRefillTime = System.nanoTime();
        }

        public synchronized boolean tryConsume(double requested, double refillRate, double capacity) {
            long now = System.nanoTime();
            double elapsedSeconds = (now - lastRefillTime) / 1_000_000_000.0;
            lastRefillTime = now;

            // Refill tokens
            tokens = Math.min(capacity, tokens + elapsedSeconds * refillRate);

            if (tokens >= requested) {
                tokens -= requested;
                return true;
            }
            return false;
        }
    }
}
