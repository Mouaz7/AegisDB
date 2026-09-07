package se.mouaz.aegisdb.protocol;

import se.mouaz.aegisdb.common.NodeId;
import java.util.Objects;

public record RequestVoteRequest(
    NodeId candidateId,
    long term,
    long lastLogIndex,
    long lastLogTerm
) {
    public RequestVoteRequest {
        Objects.requireNonNull(candidateId, "candidateId cannot be null");
    }
}
