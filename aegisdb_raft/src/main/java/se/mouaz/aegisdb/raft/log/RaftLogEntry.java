package se.mouaz.aegisdb.raft.log;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Individual entry in the Raft log (Section 18, 83; Ongaro §5.3).
 */
public record RaftLogEntry(
        long index,
        long term,
        byte[] data
) {
    public RaftLogEntry {
        if (index < 0) {
            throw new IllegalArgumentException("Log index must be non-negative, was: " + index);
        }
        if (term < 0) {
            throw new IllegalArgumentException("Term must be non-negative, was: " + term);
        }
        data = data == null ? new byte[0] : data.clone();
    }

    public RaftLogEntry(long index, long term, RaftCommand command) {
        this(index, term, command != null ? command.payload() : new byte[0]);
    }

    public RaftCommand command() {
        return RaftCommand.of(data);
    }

    @Override
    public byte[] data() {
        return data.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RaftLogEntry that)) return false;
        return index == that.index &&
                term == that.term &&
                Arrays.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(index, term);
        result = 31 * result + Arrays.hashCode(data);
        return result;
    }

    @Override
    public String toString() {
        return "RaftLogEntry{" +
                "index=" + index +
                ", term=" + term +
                ", dataLength=" + data.length +
                '}';
    }

    public static byte[] serializeList(List<RaftLogEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return new byte[0];
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(entries.size());
            for (RaftLogEntry entry : entries) {
                dos.writeLong(entry.index());
                dos.writeLong(entry.term());
                byte[] d = entry.data();
                dos.writeInt(d.length);
                dos.write(d);
            }
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize RaftLogEntry list", e);
        }
    }

    public static List<RaftLogEntry> deserializeList(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return Collections.emptyList();
        }
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        try (DataInputStream dis = new DataInputStream(bais)) {
            int count = dis.readInt();
            List<RaftLogEntry> list = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                long index = dis.readLong();
                long term = dis.readLong();
                int length = dis.readInt();
                byte[] d = new byte[length];
                dis.readFully(d);
                list.add(new RaftLogEntry(index, term, d));
            }
            return Collections.unmodifiableList(list);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to deserialize RaftLogEntry list", e);
        }
    }
}
