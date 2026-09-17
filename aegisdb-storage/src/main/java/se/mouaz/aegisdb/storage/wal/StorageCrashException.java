package se.mouaz.aegisdb.storage.wal;

import java.io.IOException;

/**
 * Exception thrown when a deterministic simulated crash point is triggered during storage I/O.
 */
public class StorageCrashException extends IOException {

    private final CrashPoint crashPoint;

    public StorageCrashException(CrashPoint crashPoint, String message) {
        super("Simulated storage crash at [" + crashPoint + "]: " + message);
        this.crashPoint = crashPoint;
    }

    public CrashPoint crashPoint() {
        return crashPoint;
    }
}
