package se.mouaz.aegisdb.storage.wal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class SegmentWriterReaderTest {

    @TempDir
    Path tempDir;

    @Test
    @DisplayName("SegmentWriter and SegmentReader accurately append and read records")
    void testWriteAndReadRecords() throws IOException {
        WalSegment segment = WalSegment.openOrCreate(tempDir, 1L);

        try (SegmentWriter writer = new SegmentWriter(segment, FsyncPolicy.ALWAYS)) {
            for (int i = 1; i <= 5; i++) {
                StorageRecord rec = StorageRecord.createEntry(
                        i,
                        1L,
                        1000L + i,
                        ("key-" + i).getBytes(StandardCharsets.UTF_8),
                        ("value-" + i).getBytes(StandardCharsets.UTF_8)
                );
                writer.append(rec);
            }
        }

        assertThat(segment.size()).isGreaterThan(0);

        try (SegmentReader reader = new SegmentReader(segment)) {
            for (int i = 1; i <= 5; i++) {
                SegmentReader.ReadResult result = reader.readNext();
                assertThat(result).isInstanceOf(SegmentReader.ReadResult.Success.class);
                SegmentReader.ReadResult.Success success = (SegmentReader.ReadResult.Success) result;
                assertThat(success.record().sequenceNumber()).isEqualTo(i);
                assertThat(new String(success.record().key(), StandardCharsets.UTF_8)).isEqualTo("key-" + i);
                assertThat(new String(success.record().value(), StandardCharsets.UTF_8)).isEqualTo("value-" + i);
            }

            SegmentReader.ReadResult eof = reader.readNext();
            assertThat(eof).isInstanceOf(SegmentReader.ReadResult.EndOfFile.class);
        }
    }
}
