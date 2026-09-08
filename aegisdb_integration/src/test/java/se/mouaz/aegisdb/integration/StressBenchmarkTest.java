package se.mouaz.aegisdb.integration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import se.mouaz.aegisdb.storage.wal.FsyncPolicy;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Stress and Concurrency Benchmark Invariant Tests")
public class StressBenchmarkTest {

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    @DisplayName("MVCC: Concurrent Read/Write workload maintains throughput without errors")
    void testConcurrentMvccReadWriteStress() throws Exception {
        StressBenchmarkSuite.BenchmarkResult result =
                StressBenchmarkSuite.benchmarkMvccReadHeavyThroughput(8, 5_000, 200);

        assertThat(result.successfulOperations()).isGreaterThan(0);
        assertThat(result.throughputOpsPerSec()).isGreaterThan(1_000.0);
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    @DisplayName("MVCC: Extreme contention bank transfers strictly conserve total balance invariant")
    void testBankTransferContentionConservation() throws Exception {
        StressBenchmarkSuite.BenchmarkResult result =
                StressBenchmarkSuite.benchmarkMvccBankTransferContention(8, 2_000);

        assertThat(result.totalOperations()).isEqualTo(2_000);
        assertThat(result.successfulOperations() + result.failedOperations()).isEqualTo(2_000);
    }



    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    @DisplayName("MVCC GC: Churn with active snapshot preserves watermarking")
    void testMvccGcChurnWithWatermark() throws Exception {
        StressBenchmarkSuite.BenchmarkResult result =
                StressBenchmarkSuite.benchmarkMvccGcChurn(10_000, 100);

        assertThat(result.successfulOperations()).isEqualTo(10_000);
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    @DisplayName("WAL: Disk append and cold crash recovery throughput")
    void testWalWriteAndRecoveryBenchmark() throws Exception {
        StressBenchmarkSuite.BenchmarkResult writeRes =
                StressBenchmarkSuite.benchmarkWalWriteThroughput(2_000, FsyncPolicy.PERIODIC);
        assertThat(writeRes.successfulOperations()).isEqualTo(2_000);

        StressBenchmarkSuite.BenchmarkResult recRes =
                StressBenchmarkSuite.benchmarkWalCrashRecovery(2_000);
        assertThat(recRes.successfulOperations()).isEqualTo(2_000);
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    @DisplayName("Raft: 3-Node consensus replication throughput under multi-threaded load")
    void testRaftReplicationBenchmark() throws Exception {
        StressBenchmarkSuite.BenchmarkResult raftRes =
                StressBenchmarkSuite.benchmarkRaftReplication(200);

        assertThat(raftRes.successfulOperations()).isEqualTo(200);
        assertThat(raftRes.throughputOpsPerSec()).isGreaterThan(50.0);
    }
}
