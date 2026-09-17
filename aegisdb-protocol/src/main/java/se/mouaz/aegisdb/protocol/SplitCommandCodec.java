package se.mouaz.aegisdb.protocol;

import se.mouaz.aegisdb.protocol.pb.*;
import com.google.protobuf.InvalidProtocolBufferException;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

/**
 * Encodes and decodes Multi-Raft split coordination commands with deterministic type tags (§28, Phase 1).
 */
public final class SplitCommandCodec {

    public static final byte TAG_PREPARE_SPLIT = 10;
    public static final byte TAG_SNAPSHOT_BARRIER = 11;
    public static final byte TAG_FINALIZE_SPLIT = 12;
    public static final byte TAG_ACTIVATE_SHARD = 13;
    public static final byte TAG_ABORT_SPLIT = 14;
    public static final byte TAG_ABORT_BOOTSTRAP = 15;
    public static final byte TAG_BOOTSTRAP_COMPLETE = 16;
    public static final byte TAG_TOPOLOGY_CUTOVER = 17;

    private SplitCommandCodec() {}

    public static byte[] encode(PrepareSplitCommandProto proto) {
        Objects.requireNonNull(proto, "proto cannot be null");
        return prefixTag(TAG_PREPARE_SPLIT, proto.toByteArray());
    }

    public static byte[] encode(SplitSnapshotBarrierCommandProto proto) {
        Objects.requireNonNull(proto, "proto cannot be null");
        return prefixTag(TAG_SNAPSHOT_BARRIER, proto.toByteArray());
    }

    public static byte[] encode(FinalizeSplitCommandProto proto) {
        Objects.requireNonNull(proto, "proto cannot be null");
        return prefixTag(TAG_FINALIZE_SPLIT, proto.toByteArray());
    }

    public static byte[] encode(ActivateShardCommandProto proto) {
        Objects.requireNonNull(proto, "proto cannot be null");
        return prefixTag(TAG_ACTIVATE_SHARD, proto.toByteArray());
    }

    public static byte[] encode(AbortSplitCommandProto proto) {
        Objects.requireNonNull(proto, "proto cannot be null");
        return prefixTag(TAG_ABORT_SPLIT, proto.toByteArray());
    }

    public static byte[] encode(AbortBootstrapCommandProto proto) {
        Objects.requireNonNull(proto, "proto cannot be null");
        return prefixTag(TAG_ABORT_BOOTSTRAP, proto.toByteArray());
    }

    public static byte[] encode(BootstrapCompleteCommandProto proto) {
        Objects.requireNonNull(proto, "proto cannot be null");
        return prefixTag(TAG_BOOTSTRAP_COMPLETE, proto.toByteArray());
    }

    public static byte[] encode(TopologyCutoverCommandProto proto) {
        Objects.requireNonNull(proto, "proto cannot be null");
        return prefixTag(TAG_TOPOLOGY_CUTOVER, proto.toByteArray());
    }

    public static boolean isSplitCommand(byte[] bytes) {
        if (bytes == null || bytes.length < 1) {
            return false;
        }
        byte tag = bytes[0];
        return tag >= TAG_PREPARE_SPLIT && tag <= TAG_TOPOLOGY_CUTOVER;
    }

    public static byte getTag(byte[] bytes) {
        if (bytes == null || bytes.length < 1) {
            throw new IllegalArgumentException("Cannot get tag from empty or null bytes");
        }
        return bytes[0];
    }

    public static byte[] getPayload(byte[] bytes) {
        if (bytes == null || bytes.length < 1) {
            throw new IllegalArgumentException("Cannot get payload from empty or null bytes");
        }
        return Arrays.copyOfRange(bytes, 1, bytes.length);
    }

    public static PrepareSplitCommandProto decodePrepareSplit(byte[] bytes) throws InvalidProtocolBufferException {
        verifyTag(bytes, TAG_PREPARE_SPLIT);
        return PrepareSplitCommandProto.parseFrom(getPayload(bytes));
    }

    public static SplitSnapshotBarrierCommandProto decodeSnapshotBarrier(byte[] bytes) throws InvalidProtocolBufferException {
        verifyTag(bytes, TAG_SNAPSHOT_BARRIER);
        return SplitSnapshotBarrierCommandProto.parseFrom(getPayload(bytes));
    }

    public static FinalizeSplitCommandProto decodeFinalizeSplit(byte[] bytes) throws InvalidProtocolBufferException {
        verifyTag(bytes, TAG_FINALIZE_SPLIT);
        return FinalizeSplitCommandProto.parseFrom(getPayload(bytes));
    }

    public static ActivateShardCommandProto decodeActivateShard(byte[] bytes) throws InvalidProtocolBufferException {
        verifyTag(bytes, TAG_ACTIVATE_SHARD);
        return ActivateShardCommandProto.parseFrom(getPayload(bytes));
    }

    public static AbortSplitCommandProto decodeAbortSplit(byte[] bytes) throws InvalidProtocolBufferException {
        verifyTag(bytes, TAG_ABORT_SPLIT);
        return AbortSplitCommandProto.parseFrom(getPayload(bytes));
    }

    public static AbortBootstrapCommandProto decodeAbortBootstrap(byte[] bytes) throws InvalidProtocolBufferException {
        verifyTag(bytes, TAG_ABORT_BOOTSTRAP);
        return AbortBootstrapCommandProto.parseFrom(getPayload(bytes));
    }

    public static BootstrapCompleteCommandProto decodeBootstrapComplete(byte[] bytes) throws InvalidProtocolBufferException {
        verifyTag(bytes, TAG_BOOTSTRAP_COMPLETE);
        return BootstrapCompleteCommandProto.parseFrom(getPayload(bytes));
    }

    public static TopologyCutoverCommandProto decodeTopologyCutover(byte[] bytes) throws InvalidProtocolBufferException {
        verifyTag(bytes, TAG_TOPOLOGY_CUTOVER);
        return TopologyCutoverCommandProto.parseFrom(getPayload(bytes));
    }

    private static byte[] prefixTag(byte tag, byte[] payload) {
        byte[] result = new byte[1 + payload.length];
        result[0] = tag;
        System.arraycopy(payload, 0, result, 1, payload.length);
        return result;
    }

    private static void verifyTag(byte[] bytes, byte expectedTag) {
        if (bytes == null || bytes.length < 1 || bytes[0] != expectedTag) {
            throw new IllegalArgumentException("Expected command tag " + expectedTag + " but got " + (bytes != null && bytes.length > 0 ? bytes[0] : "null/empty"));
        }
    }
}
