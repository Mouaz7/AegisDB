package se.mouaz.aegisdb.common;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Immutable wrapper around a byte array with unsigned lexicographical comparison (§28, Phase 1 Multi-Raft).
 * Safe for use as Map keys and in NavigableMaps / sorted collections.
 */
public final class ByteArrayKey implements Comparable<ByteArrayKey>, Serializable {

    private static final long serialVersionUID = 1L;
    private static final ByteArrayKey EMPTY = new ByteArrayKey(new byte[0], false);

    private final byte[] bytes;
    private final int hashCode;

    private ByteArrayKey(byte[] bytes, boolean clone) {
        this.bytes = clone ? bytes.clone() : bytes;
        this.hashCode = Arrays.hashCode(this.bytes);
    }

    /**
     * Creates an immutable ByteArrayKey from the given byte array (defensively copied).
     */
    public static ByteArrayKey of(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes cannot be null");
        if (bytes.length == 0) {
            return EMPTY;
        }
        return new ByteArrayKey(bytes, true);
    }

    /**
     * Creates an immutable ByteArrayKey from a UTF-8 string.
     */
    public static ByteArrayKey of(String str) {
        Objects.requireNonNull(str, "str cannot be null");
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0) {
            return EMPTY;
        }
        return new ByteArrayKey(bytes, false);
    }

    /**
     * Returns the singleton empty key.
     */
    public static ByteArrayKey empty() {
        return EMPTY;
    }

    /**
     * Returns true if this key has zero length.
     */
    public boolean isEmpty() {
        return bytes.length == 0;
    }

    /**
     * Returns the number of bytes in this key.
     */
    public int length() {
        return bytes.length;
    }

    /**
     * Returns a defensive copy of the underlying bytes.
     */
    public byte[] getBytes() {
        return bytes.clone();
    }

    /**
     * Decodes the bytes as a UTF-8 string.
     */
    public String asUtf8String() {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Compares two keys using unsigned lexicographical byte ordering.
     */
    @Override
    public int compareTo(ByteArrayKey other) {
        if (this == other) {
            return 0;
        }
        Objects.requireNonNull(other, "other cannot be null");
        return Arrays.compareUnsigned(this.bytes, other.bytes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ByteArrayKey other)) {
            return false;
        }
        return Arrays.equals(this.bytes, other.bytes);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public String toString() {
        if (bytes.length == 0) {
            return "ByteArrayKey[]";
        }
        // If printable ASCII, show string representation
        boolean isPrintable = true;
        for (byte b : bytes) {
            if (b < 32 || b > 126) {
                isPrintable = false;
                break;
            }
        }
        if (isPrintable) {
            return "ByteArrayKey[\"" + asUtf8String() + "\"]";
        }
        return "ByteArrayKey[0x" + HexFormat.of().formatHex(bytes) + "]";
    }
}
