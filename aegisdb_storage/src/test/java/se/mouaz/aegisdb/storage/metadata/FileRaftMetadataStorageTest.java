package se.mouaz.aegisdb.storage.metadata;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.storage.wal.CorruptedWalException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileRaftMetadataStorageTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("FileRaftMetadataStorage atomically saves and loads term and votedFor")
    void testSaveAndLoadMetadata() throws IOException {
        FileRaftMetadataStorage storage = new FileRaftMetadataStorage(tempDir);

        // Initially empty
        Optional<PersistentRaftMetadata> initial = storage.load();
        assertThat(initial).isEmpty();

        // Save term 1 with vote for node-1
        storage.save(1L, NodeId.of("node-1"));

        Optional<PersistentRaftMetadata> loaded = storage.load();
        assertThat(loaded).isPresent();
        assertThat(loaded.get().currentTerm()).isEqualTo(1L);
        assertThat(loaded.get().votedFor()).isEqualTo(NodeId.of("node-1"));

        // Update to term 2 with null vote (new term started)
        storage.save(2L, null);

        Optional<PersistentRaftMetadata> updated = storage.load();
        assertThat(updated).isPresent();
        assertThat(updated.get().currentTerm()).isEqualTo(2L);
        assertThat(updated.get().votedFor()).isNull();
    }

    @Test
    @DisplayName("FileRaftMetadataStorage detects corrupted metadata file via CRC32")
    void testDetectsCorruptedMetadata() throws IOException {
        FileRaftMetadataStorage storage = new FileRaftMetadataStorage(tempDir);
        storage.save(5L, NodeId.of("leader-candidate"));

        Path metaFile = storage.metadataFile();
        byte[] bytes = Files.readAllBytes(metaFile);

        // Mutate a byte in the body
        bytes[6] ^= 0x55;
        Files.write(metaFile, bytes);

        assertThatThrownBy(storage::load)
                .isInstanceOf(CorruptedWalException.class);
    }
}
