package se.mouaz.aegisdb.raft.log;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * Represents an opaque command payload carried in a RaftLogEntry (Section 18).
 */
public record RaftCommand(byte[] payload) {
    public RaftCommand {
        payload = payload == null ? new byte[0] : payload.clone();
    }

    public static RaftCommand of(byte[] payload) {
        return new RaftCommand(payload);
    }

    public static RaftCommand of(String text) {
        return new RaftCommand(text != null ? text.getBytes(StandardCharsets.UTF_8) : new byte[0]);
    }

    public static RaftCommand empty() {
        return new RaftCommand(new byte[0]);
    }

    public String asString() {
        return new String(payload, StandardCharsets.UTF_8);
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RaftCommand that)) return false;
        return Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(payload);
    }

    @Override
    public String toString() {
        return "RaftCommand{length=" + payload.length + ", text='" + asString() + "'}";
    }
}
