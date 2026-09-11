package se.mouaz.aegisdb.transaction.distributed;

import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.common.TransactionId;
import se.mouaz.aegisdb.transaction.WriteOperation;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Message sent by 2PC Coordinator to a shard participant during Phase 1 (PREPARE) (Master Project Plan §10; US015).
 */
public record PrepareRequest(
        TransactionId txId,
        ShardId shardId,
        List<WriteOperation> writes,
        Map<String, Optional<byte[]>> expectedReads,
        long readTimestamp) {

    public PrepareRequest(TransactionId txId, ShardId shardId, List<WriteOperation> writes, long readTimestamp) {
        this(txId, shardId, writes, Collections.emptyMap(), readTimestamp);
    }

    public PrepareRequest {
        Objects.requireNonNull(txId, "txId cannot be null");
        Objects.requireNonNull(shardId, "shardId cannot be null");
        writes = writes != null ? Collections.unmodifiableList(List.copyOf(writes)) : Collections.emptyList();
        expectedReads = expectedReads != null ? Collections.unmodifiableMap(Map.copyOf(expectedReads)) : Collections.emptyMap();
    }
}
