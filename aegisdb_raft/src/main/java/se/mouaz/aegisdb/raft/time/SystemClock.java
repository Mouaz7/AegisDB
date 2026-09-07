package se.mouaz.aegisdb.raft.time;

public class SystemClock implements Clock {
    @Override
    public long currentTimeMillis() {
        return System.currentTimeMillis();
    }
}
