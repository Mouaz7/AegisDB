package se.mouaz.aegisdb.benchmark;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;

/**
 * Utility functions for benchmark execution, latency calculations,
 * and reproducibility metadata gathering per Master Project Plan §20.
 */
public final class BenchmarkUtils {

    private BenchmarkUtils() {}

    public static Map<String, Object> captureMetadata(long seed) {
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("timestamp", Instant.now().toString());
        meta.put("seed", seed);
        meta.put("java_version", System.getProperty("java.version"));
        meta.put("java_vendor", System.getProperty("java.vendor"));
        meta.put("os_name", System.getProperty("os.name"));
        meta.put("os_arch", System.getProperty("os.arch"));
        meta.put("available_processors", Runtime.getRuntime().availableProcessors());
        meta.put("git_commit", resolveGitCommit());
        meta.put("configuration_hash", generateConfigHash(seed));
        return meta;
    }

    private static String resolveGitCommit() {
        try {
            Process process = new ProcessBuilder("git", "rev-parse", "--short", "HEAD").start();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line = reader.readLine();
                if (line != null && !line.isBlank()) {
                    return line.trim();
                }
            }
        } catch (Exception ignored) {}
        return "unknown-dev";
    }

    private static String generateConfigHash(long seed) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            String input = "seed=" + seed + ";os=" + System.getProperty("os.name") + ";cores=" + Runtime.getRuntime().availableProcessors();
            byte[] hash = md.digest(input.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash).substring(0, 16);
        } catch (Exception e) {
            return "0000000000000000";
        }
    }

    public static void awaitCondition(java.util.function.BooleanSupplier condition, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                throw new java.util.concurrent.TimeoutException("Condition not met within " + timeoutMs + " ms");
            }
            Thread.sleep(20);
        }
    }

    public static double calculatePercentile(List<Long> latenciesNanos, double percentile) {
        if (latenciesNanos.isEmpty()) {
            return 0.0;
        }
        List<Long> sorted = new ArrayList<>(latenciesNanos);
        Collections.sort(sorted);
        int index = (int) Math.ceil((percentile / 100.0) * sorted.size()) - 1;
        index = Math.clamp(index, 0, sorted.size() - 1);
        return sorted.get(index) / 1_000_000.0; // Return in milliseconds
    }
}
