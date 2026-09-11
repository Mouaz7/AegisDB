package se.mouaz.aegisdb.raft.statemachine;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Command representation for Key-Value store operations replicated via Raft (Phase 5; US009, US010).
 */
public record KvCommand(OpType opType, String key, byte[] value) {

    public enum OpType {
        PUT((byte) 1),
        GET((byte) 2),
        DELETE((byte) 3);

        private final byte code;

        OpType(byte code) {
            this.code = code;
        }

        public byte code() {
            return code;
        }

        public static OpType fromCode(byte code) {
            return switch (code) {
                case 1 -> PUT;
                case 2 -> GET;
                case 3 -> DELETE;
                default -> throw new IllegalArgumentException("Unknown OpType code: " + code);
            };
        }
    }

    public static KvCommand put(String key, byte[] value) {
        return new KvCommand(OpType.PUT, Objects.requireNonNull(key, "key cannot be null"), value != null ? value : new byte[0]);
    }

    public static KvCommand get(String key) {
        return new KvCommand(OpType.GET, Objects.requireNonNull(key, "key cannot be null"), null);
    }

    public static KvCommand delete(String key) {
        return new KvCommand(OpType.DELETE, Objects.requireNonNull(key, "key cannot be null"), null);
    }

    public byte[] toBytes() {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valBytes = value != null ? value : new byte[0];
        int valLen = (opType == OpType.PUT) ? valBytes.length : -1;

        int totalLen = 1 + 4 + keyBytes.length + 4 + (valLen >= 0 ? valLen : 0);
        ByteBuffer buf = ByteBuffer.allocate(totalLen);
        buf.put(opType.code());
        buf.putInt(keyBytes.length);
        buf.put(keyBytes);
        buf.putInt(valLen);
        if (valLen > 0) {
            buf.put(valBytes);
        }
        return buf.array();
    }

    public static KvCommand fromBytes(byte[] bytes) {
        Objects.requireNonNull(bytes, "bytes cannot be null");
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        byte opCode = buf.get();
        OpType opType = OpType.fromCode(opCode);

        int keyLen = buf.getInt();
        byte[] keyBytes = new byte[keyLen];
        buf.get(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);

        int valLen = buf.getInt();
        byte[] valBytes = null;
        if (valLen >= 0) {
            valBytes = new byte[valLen];
            buf.get(valBytes);
        }

        return new KvCommand(opType, key, valBytes);
    }
}
