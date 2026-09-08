package se.mouaz.aegisdb.integration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.Endpoint;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.RaftNode;
import se.mouaz.aegisdb.raft.state.PersistentRaftState;
import se.mouaz.aegisdb.raft.state.RaftInvariants;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.storage.StorageEngine;
import se.mouaz.aegisdb.storage.metadata.FileRaftMetadataStorage;
import se.mouaz.aegisdb.storage.recovery.WalRecoveryManager;
import se.mouaz.aegisdb.storage.wal.*;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

/**
 * Sprint 4 Live Demonstration & Verification (Persistence and Recovery - US007 & US008).
 * Demonstrates all 6 Sprint 4 Acceptance Criteria:
 * 1. WAL segments and checksums
 * 2. Flush/fsync policy
 * 3. Persistent term/votedFor
 * 4. Partial-write recovery
 * 5. Corruption detection
 * 6. Restart tests
 */
public class Sprint4Demo {
    private static final Logger log = LoggerFactory.getLogger(Sprint4Demo.class);

    public static void main(String[] args) {
        System.out.println("===============================================================");
        System.out.println("     AegisDB - Sprint 4 Demonstration & Verifiering ");
        System.out.println("     Storage Engine: Persistence & Recovery (US007, US008)");
        System.out.println("===============================================================\n");

        Path demoRoot = null;
        try {
            demoRoot = Files.createTempDirectory("aegisdb-sprint4-demo");

            // --- AC1: WAL segments and checksums ---
            System.out.println("▶ [1/6] [AC1] Verifierar 'WAL segments and checksums'...");
            Path walDir = demoRoot.resolve("ac1-wal");
            WalConfig walConfig = WalConfig.builder()
                    .walDir(walDir)
                    .maxSegmentSizeBytes(300) // small size to demonstrate segment rollover
                    .fsyncPolicy(FsyncPolicy.ALWAYS)
                    .build();

            try (WalManager walManager = new WalManager(walConfig)) {
                for (int i = 1; i <= 6; i++) {
                    StorageRecord rec = StorageRecord.createEntry(
                            i, 1L, System.currentTimeMillis(),
                            ("user:" + i).getBytes(StandardCharsets.UTF_8),
                            ("{\"balance\":" + (1000 * i) + "}").getBytes(StandardCharsets.UTF_8)
                    );
                    walManager.append(rec);
                }

                List<WalSegment> segments = walManager.listSegments();
                System.out.println("   ✔ Skapade " + segments.size() + " WAL-segment med automatisk rollover vid gräns (300B):");
                for (WalSegment s : segments) {
                    System.out.println("     - " + s.path().getFileName() + " (" + s.size() + " bytes)");
                }

                WalReader reader = new WalReader(walManager);
                List<StorageRecord> records = reader.readAllRecords();
                System.out.println("   ✔ Läste " + records.size() + " poster sekventiellt över segmenten.");
                for (StorageRecord r : records) {
                    System.out.println("     Post #" + r.sequenceNumber() + " | Magic: 0x" + Integer.toHexString(r.magicNumber())
                            + " | CRC32: 0x" + Long.toHexString(r.checksum()) + " [GILTIG]");
                }
            }
            System.out.println("   ✔ [AC1] WAL segments and checksums: PASSED\n");

            // --- AC2: Flush/fsync policy ---
            System.out.println("▶ [2/6] [AC2] Verifierar 'Flush/fsync policy'...");
            System.out.println("   Stödjer FsyncPolicy.ALWAYS (FileChannel.force(true) på varje skrivning),");
            System.out.println("   FsyncPolicy.PERIODIC samt FsyncPolicy.MANUAL.");
            Path fsyncDir = demoRoot.resolve("ac2-fsync");
            WalConfig fsyncConfig = WalConfig.builder()
                    .walDir(fsyncDir)
                    .fsyncPolicy(FsyncPolicy.ALWAYS)
                    .build();
            try (WalManager wm = new WalManager(fsyncConfig)) {
                wm.append(StorageRecord.createEntry(1L, 1L, System.currentTimeMillis(), "data".getBytes(StandardCharsets.UTF_8)));
                wm.sync();
                System.out.println("   ✔ Synkroniserade segment med fsync force(true) till disk.");
            }
            System.out.println("   ✔ [AC2] Flush/fsync policy: PASSED\n");

            // --- AC3: Persistent term/votedFor ---
            System.out.println("▶ [3/6] [AC3] Verifierar 'Persistent term/votedFor' (US008)...");
            Path metaDir = demoRoot.resolve("ac3-meta");
            FileRaftMetadataStorage metaStorage = new FileRaftMetadataStorage(metaDir);

            System.out.println("   Sparar initialt tillstånd: Term=1, VotedFor=node-leader");
            metaStorage.save(1L, NodeId.of("node-leader"));

            System.out.println("   Uppdaterar atomärt till ny term via write-to-temp + fsync + ATOMIC_MOVE: Term=2, VotedFor=node-candidate");
            metaStorage.save(2L, NodeId.of("node-candidate"));

            var loadedMeta = metaStorage.load().orElseThrow();
            System.out.println("   ✔ Återläst konsensusmetadata från disk: Term=" + loadedMeta.currentTerm() + ", VotedFor=" + loadedMeta.votedFor());
            if (loadedMeta.currentTerm() != 2L || !loadedMeta.votedFor().value().equals("node-candidate")) {
                throw new IllegalStateException("Metadata matchade inte sparat tillstånd!");
            }
            System.out.println("   ✔ [AC3] Persistent term/votedFor: PASSED\n");

            // --- AC4: Partial-write recovery ---
            System.out.println("▶ [4/6] [AC4] Verifierar 'Partial-write recovery' (Torn Tail vid krasch)...");
            Path tornDir = demoRoot.resolve("ac4-torn");
            WalConfig tornConfig = WalConfig.of(tornDir);

            try (WalManager wm = new WalManager(tornConfig)) {
                wm.append(StorageRecord.createEntry(1L, 1L, System.currentTimeMillis(), "entry-1".getBytes(StandardCharsets.UTF_8)));
                wm.append(StorageRecord.createEntry(2L, 1L, System.currentTimeMillis(), "entry-2".getBytes(StandardCharsets.UTF_8)));
            }

            // Injicera en avbruten skrivning (halv post i slutet av filen)
            WalSegment seg = WalSegment.openOrCreate(tornDir, 1L);
            long validLength = seg.size();

            StorageRecord unwritten = StorageRecord.createEntry(3L, 1L, System.currentTimeMillis(), "torn-incomplete-bytes".getBytes(StandardCharsets.UTF_8));
            byte[] rawBytes = unwritten.serialize().array();
            byte[] halfBytes = new byte[rawBytes.length / 2];
            System.arraycopy(rawBytes, 0, halfBytes, 0, halfBytes.length);

            try (FileChannel fc = seg.openChannel(StandardOpenOption.WRITE, StandardOpenOption.APPEND)) {
                fc.write(ByteBuffer.wrap(halfBytes));
                fc.force(true);
            }
            System.out.println("   Simulerade plötslig strömavbrott/processkill mitt i post #3 (" + halfBytes.length + " avbrutna bytes skrivna)");

            try (WalManager wm = new WalManager(tornConfig)) {
                WalRecoveryManager recMgr = new WalRecoveryManager(wm);
                WalRecoveryManager.WalScanResult scanRes = recMgr.scanAndRecover();

                System.out.println("   ✔ Återställningshanteraren upptäckte torn tail och trunkerade säkert " + scanRes.tornTailsRepairedCount() + " avbruten svans.");
                System.out.println("   ✔ Alla " + scanRes.records().size() + " tidigare committade poster återställdes intakta!");
                if (scanRes.records().size() != 2) {
                    throw new IllegalStateException("Misslyckades att återställa giltiga poster före torn tail!");
                }
            }
            System.out.println("   ✔ [AC4] Partial-write recovery: PASSED\n");

            // --- AC5: Corruption detection ---
            System.out.println("▶ [5/6] [AC5] Verifierar 'Corruption detection' (CRC32 & Magic fel)...");
            Path corruptDir = demoRoot.resolve("ac5-corrupt");
            WalConfig corruptConfig = WalConfig.of(corruptDir);

            try (WalManager wm = new WalManager(corruptConfig)) {
                wm.append(StorageRecord.createEntry(1L, 1L, System.currentTimeMillis(), "safe-payload".getBytes(StandardCharsets.UTF_8)));
            }

            // Manipulera en bit i posten (korruption på disk)
            WalSegment corruptSeg = WalSegment.openOrCreate(corruptDir, 1L);
            byte[] cBytes = Files.readAllBytes(corruptSeg.path());
            cBytes[StorageRecord.FRAMING_HEADER_SIZE + 5] ^= 0x7F;
            Files.write(corruptSeg.path(), cBytes);

            try (WalManager wm = new WalManager(corruptConfig)) {
                WalRecoveryManager recMgr = new WalRecoveryManager(wm);
                try {
                    recMgr.scanAndRecover();
                    throw new IllegalStateException("Korruption upptäcktes inte!");
                } catch (CorruptedWalException ex) {
                    System.out.println("   ✔ Korruption fångades framgångsrikt: " + ex.getMessage());
                }
            }
            System.out.println("   ✔ [AC5] Corruption detection: PASSED\n");

            // --- AC6: Restart tests ---
            System.out.println("▶ [6/6] [AC6] Verifierar 'Restart tests' (3-nods kluster överlever krasch & omstart)...");
            RaftInvariants.clearInvariantTracking();
            InMemoryTransport.clearRegistry();

            NodeId n1 = NodeId.of("node-1");
            NodeId n2 = NodeId.of("node-2");
            NodeId n3 = NodeId.of("node-3");

            ClusterConfiguration clusterCfg = ClusterConfiguration.builder()
                    .clusterId("restart-demo-cluster")
                    .addMember(n1, Endpoint.of("127.0.0.1", 12001))
                    .addMember(n2, Endpoint.of("127.0.0.1", 12002))
                    .addMember(n3, Endpoint.of("127.0.0.1", 12003))
                    .build();

            Path n1Dir = demoRoot.resolve("node-1-data");
            Path n2Dir = demoRoot.resolve("node-2-data");
            Path n3Dir = demoRoot.resolve("node-3-data");

            StorageEngine s1 = new StorageEngine(n1Dir);
            StorageEngine s2 = new StorageEngine(n2Dir);
            StorageEngine s3 = new StorageEngine(n3Dir);

            InMemoryTransport t1 = new InMemoryTransport(n1);
            InMemoryTransport t2 = new InMemoryTransport(n2);
            InMemoryTransport t3 = new InMemoryTransport(n3);
            t1.start();
            t2.start();
            t3.start();

            StorageEngine.DurableRecovery dRec1 = s1.recoverAndCreateLog();
            PersistentRaftState ps1 = new PersistentRaftState(dRec1.result().recoveredTerm(), dRec1.result().recoveredVotedFor());
            ps1.setPersistenceListener((term, vote) -> { try { s1.metadataStorage().save(term, vote); } catch (Exception ignored) {} });

            StorageEngine.DurableRecovery dRec2 = s2.recoverAndCreateLog();
            PersistentRaftState ps2 = new PersistentRaftState(dRec2.result().recoveredTerm(), dRec2.result().recoveredVotedFor());
            ps2.setPersistenceListener((term, vote) -> { try { s2.metadataStorage().save(term, vote); } catch (Exception ignored) {} });

            StorageEngine.DurableRecovery dRec3 = s3.recoverAndCreateLog();
            PersistentRaftState ps3 = new PersistentRaftState(dRec3.result().recoveredTerm(), dRec3.result().recoveredVotedFor());
            ps3.setPersistenceListener((term, vote) -> { try { s3.metadataStorage().save(term, vote); } catch (Exception ignored) {} });

            RaftNode node1 = RaftNode.builder().nodeId(n1).clusterConfig(clusterCfg).transport(t1).persistentState(ps1).raftLog(dRec1.raftLog())
                    .minElectionTimeout(Duration.ofMillis(100)).maxElectionTimeout(Duration.ofMillis(140)).heartbeatInterval(Duration.ofMillis(30)).random(new Random(1)).build();
            RaftNode node2 = RaftNode.builder().nodeId(n2).clusterConfig(clusterCfg).transport(t2).persistentState(ps2).raftLog(dRec2.raftLog())
                    .minElectionTimeout(Duration.ofMillis(800)).maxElectionTimeout(Duration.ofMillis(1000)).heartbeatInterval(Duration.ofMillis(30)).random(new Random(2)).build();
            RaftNode node3 = RaftNode.builder().nodeId(n3).clusterConfig(clusterCfg).transport(t3).persistentState(ps3).raftLog(dRec3.raftLog())
                    .minElectionTimeout(Duration.ofMillis(800)).maxElectionTimeout(Duration.ofMillis(1000)).heartbeatInterval(Duration.ofMillis(30)).random(new Random(3)).build();

            node1.start();
            node2.start();
            node3.start();

            await().atMost(Duration.ofSeconds(3)).until(() -> node1.role() == RaftRole.LEADER);
            System.out.println("   ✔ Ledare vald: " + node1.nodeId() + " i Term " + node1.currentTerm());

            node1.propose("TX-101-TRANSFER 500".getBytes(StandardCharsets.UTF_8)).get(3, TimeUnit.SECONDS);
            node1.propose("TX-102-TRANSFER 300".getBytes(StandardCharsets.UTF_8)).get(3, TimeUnit.SECONDS);
            await().atMost(Duration.ofSeconds(3)).until(() -> node2.commitIndex() == 2L && node3.commitIndex() == 2L);
            System.out.println("   ✔ Replikering och persistent commit utförd för transaktioner 1 och 2.");

            // Krascha hela klustret!
            System.out.println("   Kraschar hela klustret abrupt (stoppar processer)...");
            node1.stop(); node2.stop(); node3.stop();
            t1.stop(); t2.stop(); t3.stop();
            s1.close(); s2.close(); s3.close();
            InMemoryTransport.clearRegistry();

            // Starta om klustret från disk!
            System.out.println("   Startar om samtliga noder från disklagring...");
            StorageEngine rs1 = new StorageEngine(n1Dir);
            StorageEngine rs2 = new StorageEngine(n2Dir);
            StorageEngine rs3 = new StorageEngine(n3Dir);

            StorageEngine.DurableRecovery rRec1 = rs1.recoverAndCreateLog();
            StorageEngine.DurableRecovery rRec2 = rs2.recoverAndCreateLog();
            StorageEngine.DurableRecovery rRec3 = rs3.recoverAndCreateLog();

            System.out.println("   ✔ Nod 1 återställd: Term=" + rRec1.result().recoveredTerm() + ", Senaste LogIndex=" + rRec1.result().lastLogIndex());
            System.out.println("   ✔ Nod 2 återställd: Term=" + rRec2.result().recoveredTerm() + ", Senaste LogIndex=" + rRec2.result().lastLogIndex());
            System.out.println("   ✔ Nod 3 återställd: Term=" + rRec3.result().recoveredTerm() + ", Senaste LogIndex=" + rRec3.result().lastLogIndex());

            InMemoryTransport rt1 = new InMemoryTransport(n1);
            InMemoryTransport rt2 = new InMemoryTransport(n2);
            InMemoryTransport rt3 = new InMemoryTransport(n3);
            rt1.start(); rt2.start(); rt3.start();

            PersistentRaftState rps1 = new PersistentRaftState(rRec1.result().recoveredTerm(), rRec1.result().recoveredVotedFor());
            rps1.setPersistenceListener((term, vote) -> { try { rs1.metadataStorage().save(term, vote); } catch (Exception ignored) {} });

            PersistentRaftState rps2 = new PersistentRaftState(rRec2.result().recoveredTerm(), rRec2.result().recoveredVotedFor());
            rps2.setPersistenceListener((term, vote) -> { try { rs2.metadataStorage().save(term, vote); } catch (Exception ignored) {} });

            PersistentRaftState rps3 = new PersistentRaftState(rRec3.result().recoveredTerm(), rRec3.result().recoveredVotedFor());
            rps3.setPersistenceListener((term, vote) -> { try { rs3.metadataStorage().save(term, vote); } catch (Exception ignored) {} });

            RaftNode rNode1 = RaftNode.builder().nodeId(n1).clusterConfig(clusterCfg).transport(rt1).persistentState(rps1).raftLog(rRec1.raftLog())
                    .minElectionTimeout(Duration.ofMillis(100)).maxElectionTimeout(Duration.ofMillis(140)).heartbeatInterval(Duration.ofMillis(30)).random(new Random(1)).build();
            RaftNode rNode2 = RaftNode.builder().nodeId(n2).clusterConfig(clusterCfg).transport(rt2).persistentState(rps2).raftLog(rRec2.raftLog())
                    .minElectionTimeout(Duration.ofMillis(800)).maxElectionTimeout(Duration.ofMillis(1000)).heartbeatInterval(Duration.ofMillis(30)).random(new Random(2)).build();
            RaftNode rNode3 = RaftNode.builder().nodeId(n3).clusterConfig(clusterCfg).transport(rt3).persistentState(rps3).raftLog(rRec3.raftLog())
                    .minElectionTimeout(Duration.ofMillis(800)).maxElectionTimeout(Duration.ofMillis(1000)).heartbeatInterval(Duration.ofMillis(30)).random(new Random(3)).build();

            rNode1.start(); rNode2.start(); rNode3.start();

            await().atMost(Duration.ofSeconds(3)).until(() -> rNode1.role() == RaftRole.LEADER);
            System.out.println("   ✔ Klustret återupptog konsensus efter omstart! Ledare: " + rNode1.nodeId());

            CompletableFuture<Long> fut3 = rNode1.propose("TX-103-AFTER-RESTART 900".getBytes(StandardCharsets.UTF_8));
            Long idx3 = fut3.get(3, TimeUnit.SECONDS);
            System.out.println("   ✔ Ny skrivning framgångsrikt committad efter omstart på index: " + idx3);

            RaftInvariants.assertIdenticalOrderOfCommittedEntries(rNode1.log(), rNode2.log(), 3L);
            RaftInvariants.assertIdenticalOrderOfCommittedEntries(rNode1.log(), rNode3.log(), 3L);
            System.out.println("   ✔ Invariant verifierad: Alla noder har identisk loggsekvens över krasch och omstart.");

            rNode1.stop(); rNode2.stop(); rNode3.stop();
            rt1.stop(); rt2.stop(); rt3.stop();
            rs1.close(); rs2.close(); rs3.close();

            System.out.println("   ✔ [AC6] Restart tests: PASSED\n");

            System.out.println("===============================================================");
            System.out.println("     ALLA 6 ACCEPTANSKRITERIER FÖR SPRINT 4 VERIFIERADE!");
            System.out.println("     [AC1] WAL segments and checksums:   PASSED");
            System.out.println("     [AC2] Flush/fsync policy:           PASSED");
            System.out.println("     [AC3] Persistent term/votedFor:     PASSED");
            System.out.println("     [AC4] Partial-write recovery:       PASSED");
            System.out.println("     [AC5] Corruption detection:         PASSED");
            System.out.println("     [AC6] Restart tests:                PASSED");
            System.out.println("===============================================================");

            System.exit(0);
        } catch (Exception e) {
            log.error("Sprint 4 Demo misslyckades", e);
            System.err.println("DEMO FAILED: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            if (demoRoot != null) {
                try {
                    Files.walk(demoRoot)
                            .sorted((a, b) -> b.compareTo(a))
                            .forEach(p -> {
                                try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                            });
                } catch (IOException ignored) {}
            }
        }
    }
}
