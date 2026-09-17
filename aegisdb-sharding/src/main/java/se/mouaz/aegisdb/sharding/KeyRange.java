package se.mouaz.aegisdb.sharding;

import se.mouaz.aegisdb.common.ByteArrayKey;
import se.mouaz.aegisdb.common.KeyBound;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a half-open lexicographical byte interval [startBound, endBound) (§28, Phase 1 Multi-Raft).
 * Supports unbounded infinities (-∞ and +∞) via KeyBound.
 */
public final class KeyRange implements Comparable<KeyRange>, Serializable {

    private static final long serialVersionUID = 1L;
    private static final KeyRange FULL_KEYSPACE = new KeyRange(KeyBound.negativeInfinity(), KeyBound.positiveInfinity());

    private final KeyBound startBound;
    private final KeyBound endBound;

    public KeyRange(KeyBound startBound, KeyBound endBound) {
        this.startBound = Objects.requireNonNull(startBound, "startBound cannot be null");
        this.endBound = Objects.requireNonNull(endBound, "endBound cannot be null");
        if (startBound.compareTo(endBound) >= 0) {
            throw new IllegalArgumentException("Invalid KeyRange: startBound " + startBound + " must be strictly less than endBound " + endBound);
        }
    }

    public static KeyRange of(KeyBound startBound, KeyBound endBound) {
        return new KeyRange(startBound, endBound);
    }

    public static KeyRange of(ByteArrayKey startKey, ByteArrayKey endKey) {
        return new KeyRange(KeyBound.exact(startKey), KeyBound.exact(endKey));
    }

    public static KeyRange fullKeyspace() {
        return FULL_KEYSPACE;
    }

    public KeyBound startBound() {
        return startBound;
    }

    public KeyBound endBound() {
        return endBound;
    }

    /**
     * Checks if a concrete key falls within this half-open range: startBound <= key < endBound.
     */
    public boolean contains(ByteArrayKey key) {
        Objects.requireNonNull(key, "key cannot be null");
        KeyBound bound = KeyBound.exact(key);
        return startBound.compareTo(bound) <= 0 && bound.compareTo(endBound) < 0;
    }

    /**
     * Checks if a concrete key falls strictly inside the range: startBound < key < endBound.
     * Required precondition for valid splitKey.
     */
    public boolean containsStrictInterior(ByteArrayKey key) {
        Objects.requireNonNull(key, "key cannot be null");
        KeyBound bound = KeyBound.exact(key);
        return startBound.compareTo(bound) < 0 && bound.compareTo(endBound) < 0;
    }

    /**
     * Checks if this range overlaps with another range.
     * [s1, e1) overlaps [s2, e2) iff max(s1, s2) < min(e1, e2).
     */
    public boolean overlaps(KeyRange other) {
        Objects.requireNonNull(other, "other cannot be null");
        KeyBound maxStart = this.startBound.compareTo(other.startBound) >= 0 ? this.startBound : other.startBound;
        KeyBound minEnd = this.endBound.compareTo(other.endBound) <= 0 ? this.endBound : other.endBound;
        return maxStart.compareTo(minEnd) < 0;
    }

    /**
     * Checks if this range is directly adjacent to another range without gap or overlap.
     */
    public boolean isAdjacentTo(KeyRange other) {
        Objects.requireNonNull(other, "other cannot be null");
        return this.endBound.equals(other.startBound) || other.endBound.equals(this.startBound);
    }

    /**
     * Splits this range at the given splitKey into left [startBound, splitKey) and right [splitKey, endBound).
     */
    public KeyRange[] splitAt(ByteArrayKey splitKey) {
        if (!containsStrictInterior(splitKey)) {
            throw new IllegalArgumentException("Split key " + splitKey + " is not strictly inside range " + this);
        }
        KeyBound splitBound = KeyBound.exact(splitKey);
        return new KeyRange[] {
                new KeyRange(this.startBound, splitBound),
                new KeyRange(splitBound, this.endBound)
        };
    }

    @Override
    public int compareTo(KeyRange other) {
        int cmp = this.startBound.compareTo(other.startBound);
        if (cmp != 0) {
            return cmp;
        }
        return this.endBound.compareTo(other.endBound);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KeyRange other)) {
            return false;
        }
        return startBound.equals(other.startBound) && endBound.equals(other.endBound);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startBound, endBound);
    }

    @Override
    public String toString() {
        return "[" + startBound + ", " + endBound + ")";
    }
}
