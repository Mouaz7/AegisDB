package se.mouaz.aegisdb.raft.statemachine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Replicated in-memory Key-Value store state machine (Master Project Plan §5, §18; US009, US010; Milestone M2).
 * Backed by ConcurrentSkipListMap for sorted, concurrent access.
 * Supports point-in-time snapshots and state restoration.
 */
public class KeyValueStateMachine implements StateMachine {
    private static final Logger log = LoggerFactory.getLogger(KeyValueStateMachine.class);

    private final ConcurrentNavigableMap<String, byte[]> store = new ConcurrentSkipListMap<>();
    private volatile long lastAppliedIndex = 0L;

    public KeyValueStateMachine() {}

    @Override
    public synchronized byte[] apply(long index, byte[] commandBytes) {
        if (commandBytes == null || commandBytes.length == 0) {
            this.lastAppliedIndex = index;
            return new byte[0];
        }

        KvCommand command;
        try {
            command = KvCommand.fromBytes(commandBytes);
        } catch (Exception e) {
            log.warn("Failed to parse KvCommand at index {}: {}", index, e.getMessage());
            this.lastAppliedIndex = index;
            return new byte[0];
        }

        this.lastAppliedIndex = index;

        return switch (command.opType()) {
            case PUT -> {
                byte[] previous = store.put(command.key(), command.value());
                log.debug("Applied PUT key={}, valLen={}, at index={}", command.key(), command.value().length, index);
                yield previous != null ? previous : new byte[0];
            }
            case GET -> {
                byte[] val = store.get(command.key());
                yield val != null ? val : new byte[0];
            }
            case DELETE -> {
                byte[] removed = store.remove(command.key());
                log.debug("Applied DELETE key={} at index={}", command.key(), index);
                yield removed != null ? removed : new byte[0];
            }
        };
    }

    @Override
    public synchronized byte[] takeSnapshot() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);

            dos.writeInt(store.size());
            for (Map.Entry<String, byte[]> entry : store.entrySet()) {
                byte[] keyBytes = entry.getKey().getBytes(StandardCharsets.UTF_8);
                dos.writeInt(keyBytes.length);
                dos.write(keyBytes);

                byte[] valBytes = entry.getValue();
                dos.writeInt(valBytes.length);
                dos.write(valBytes);
            }
            dos.flush();
            byte[] snapshot = baos.toByteArray();
            log.info("Took state machine snapshot at lastAppliedIndex={}: {} entries, {} bytes",
                    lastAppliedIndex, store.size(), snapshot.length);
            return snapshot;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to serialize state machine snapshot", e);
        }
    }

    @Override
    public synchronized void restoreSnapshot(long lastIncludedIndex, byte[] snapshotData) {
        store.clear();
        this.lastAppliedIndex = lastIncludedIndex;

        if (snapshotData == null || snapshotData.length == 0) {
            log.info("Restored empty state machine snapshot at lastIncludedIndex={}", lastIncludedIndex);
            return;
        }

        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(snapshotData);
            DataInputStream dis = new DataInputStream(bais);

            int entryCount = dis.readInt();
            for (int i = 0; i < entryCount; i++) {
                int keyLen = dis.readInt();
                byte[] keyBytes = new byte[keyLen];
                dis.readFully(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);

                int valLen = dis.readInt();
                byte[] valBytes = new byte[valLen];
                dis.readFully(valBytes);

                store.put(key, valBytes);
            }
            log.info("Restored state machine snapshot at lastIncludedIndex={}: {} entries restored",
                    lastIncludedIndex, store.size());
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to deserialize state machine snapshot", e);
        }
    }

    @Override
    public long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    public byte[] get(String key) {
        return store.get(key);
    }

    public boolean containsKey(String key) {
        return store.containsKey(key);
    }

    public int size() {
        return store.size();
    }

    public Map<String, byte[]> snapshotMap() {
        return Collections.unmodifiableMap(new ConcurrentSkipListMap<>(store));
    }
}
