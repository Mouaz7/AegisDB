package se.mouaz.aegisdb.storage.metadata;

import se.mouaz.aegisdb.common.NodeId;

import java.io.IOException;
import java.util.Optional;

/**
 * Storage contract for durable Raft consensus metadata (Master Project Plan §7, §8; Ongaro §5.2).
 */
public interface RaftMetadataStorage {

    /**
     * Atomically and durably saves currentTerm and votedFor.
     */
    void save(long term, NodeId votedFor) throws IOException;

    /**
     * Loads persisted term and votedFor if present.
     */
    Optional<PersistentRaftMetadata> load() throws IOException;
}
