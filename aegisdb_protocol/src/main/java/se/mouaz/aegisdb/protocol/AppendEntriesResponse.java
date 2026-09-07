package se.mouaz.aegisdb.protocol;

public record AppendEntriesResponse(
    long term,
    boolean success,
    long matchIndex,
    String reason
) {
    public static AppendEntriesResponse success(long term, long matchIndex) {
        return new AppendEntriesResponse(term, true, matchIndex, "Success");
    }

    public static AppendEntriesResponse failure(long term, String reason) {
        return new AppendEntriesResponse(term, false, 0, reason);
    }
}
