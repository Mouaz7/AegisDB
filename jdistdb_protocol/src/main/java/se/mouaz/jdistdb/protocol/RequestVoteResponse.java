package se.mouaz.jdistdb.protocol;

public record RequestVoteResponse(
    long term,
    boolean voteGranted,
    String reason
) {
    public static RequestVoteResponse granted(long term) {
        return new RequestVoteResponse(term, true, "Vote granted");
    }

    public static RequestVoteResponse rejected(long term, String reason) {
        return new RequestVoteResponse(term, false, reason);
    }
}
