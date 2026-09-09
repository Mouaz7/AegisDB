package se.mouaz.aegisdb.sharding;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Pure Java implementation of the 32-bit MurmurHash3 algorithm (Austin Appleby).
 * Provides uniform distribution, avalanche properties, and deterministic cross-platform hashing.
 */
public final class Murmur3 {

    private static final int DEFAULT_SEED = 0x9747b28c;
    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;
    private static final int R1 = 15;
    private static final int R2 = 13;
    private static final int M = 5;
    private static final int N = 0xe6546b64;

    private Murmur3() {
    }

    /**
     * Hashes a string using UTF-8 encoding with the default seed.
     */
    public static int hash32(String text) {
        Objects.requireNonNull(text, "text cannot be null");
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        return hash32(bytes, 0, bytes.length, DEFAULT_SEED);
    }

    /**
     * Hashes a string using UTF-8 encoding with a specific seed.
     */
    public static int hash32(String text, int seed) {
        Objects.requireNonNull(text, "text cannot be null");
        byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
        return hash32(bytes, 0, bytes.length, seed);
    }

    /**
     * Hashes a byte array with the default seed.
     */
    public static int hash32(byte[] data) {
        Objects.requireNonNull(data, "data cannot be null");
        return hash32(data, 0, data.length, DEFAULT_SEED);
    }

    /**
     * Hashes a slice of a byte array using MurmurHash3 32-bit.
     */
    public static int hash32(byte[] data, int offset, int length, int seed) {
        Objects.requireNonNull(data, "data cannot be null");
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IndexOutOfBoundsException("Invalid offset or length for byte array of size " + data.length);
        }

        int h = seed;
        int roundedEnd = offset + (length & 0xFFFFFFFC); // round down to 4-byte boundary

        for (int i = offset; i < roundedEnd; i += 4) {
            int k = (data[i] & 0xFF)
                    | ((data[i + 1] & 0xFF) << 8)
                    | ((data[i + 2] & 0xFF) << 16)
                    | ((data[i + 3] & 0xFF) << 24);

            k *= C1;
            k = Integer.rotateLeft(k, R1);
            k *= C2;

            h ^= k;
            h = Integer.rotateLeft(h, R2);
            h = h * M + N;
        }

        int k = 0;
        int remaining = length & 3;
        if (remaining == 3) {
            k ^= (data[roundedEnd + 2] & 0xFF) << 16;
        }
        if (remaining >= 2) {
            k ^= (data[roundedEnd + 1] & 0xFF) << 8;
        }
        if (remaining >= 1) {
            k ^= (data[roundedEnd] & 0xFF);
            k *= C1;
            k = Integer.rotateLeft(k, R1);
            k *= C2;
            h ^= k;
        }

        // Finalization avalanche
        h ^= length;
        h ^= (h >>> 16);
        h *= 0x85ebca6b;
        h ^= (h >>> 13);
        h *= 0xc2b2ae35;
        h ^= (h >>> 16);

        return h;
    }
}
