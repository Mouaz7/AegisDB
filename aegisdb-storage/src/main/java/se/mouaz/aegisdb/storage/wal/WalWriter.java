package se.mouaz.aegisdb.storage.wal;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

/**
 * High-level WAL writer coordinating record appends, rotation, and sync (Master Project Plan §8).
 */
public class WalWriter implements Closeable {

    private final WalManager walManager;

    public WalWriter(WalManager walManager) {
        this.walManager = Objects.requireNonNull(walManager, "walManager cannot be null");
    }

    public WalManager walManager() {
        return walManager;
    }

    public void open() throws IOException {
        walManager.openWriter();
    }

    public StorageIndex.IndexEntry append(StorageRecord record) throws IOException {
        return walManager.append(record);
    }

    public StorageIndex.IndexEntry appendEntry(long sequenceNumber, long term, long timestamp, byte[] key, byte[] value) throws IOException {
        StorageRecord record = StorageRecord.createEntry(sequenceNumber, term, timestamp, key, value);
        return walManager.append(record);
    }

    public StorageIndex.IndexEntry appendEntry(long sequenceNumber, long term, long timestamp, byte[] value) throws IOException {
        return appendEntry(sequenceNumber, term, timestamp, new byte[0], value);
    }

    public void sync() throws IOException {
        walManager.sync();
    }

    public void truncateFrom(long fromSequenceNumber, StorageIndex storageIndex) throws IOException {
        walManager.truncateFrom(fromSequenceNumber, storageIndex);
    }

    @Override
    public void close() throws IOException {
        walManager.close();
    }
}
