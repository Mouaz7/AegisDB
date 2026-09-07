package se.mouaz.jdistdb.protocol;

import se.mouaz.jdistdb.common.NodeId;
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
