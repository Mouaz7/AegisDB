package se.mouaz.aegisdb.chaos;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.protocol.AppendEntriesRequest;
import se.mouaz.aegisdb.protocol.AppendEntriesResponse;
import se.mouaz.aegisdb.protocol.RequestVoteRequest;
import se.mouaz.aegisdb.protocol.RequestVoteResponse;
import se.mouaz.aegisdb.transport.InMemoryTransport;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FaultyTransportTest {
    private NodeId nodeA;
    private NodeId nodeB;
    private InMemoryTransport inMemoryA;
    private InMemoryTransport inMemoryB;
    private FaultyTransport faultyA;
    private FaultyTransport faultyB;

    @BeforeEach
    void setUp() {
        nodeA = new NodeId("node-A");
        nodeB = new NodeId("node-B");

        inMemoryA = new InMemoryTransport(nodeA);
        inMemoryB = new InMemoryTransport(nodeB);

        inMemoryA.start();
        inMemoryB.start();

        faultyA = new FaultyTransport(inMemoryA, 12345L);
        faultyB = new FaultyTransport(inMemoryB, 12345L);
    }

    @AfterEach
    void tearDown() {
        faultyA.stop();
        faultyB.stop();
    }

    @Test
    @DisplayName("Should successfully deliver RPC when no fault rules are present")
    void shouldDeliverWhenNoFaults() throws Exception {
        RequestVoteRequest request = new RequestVoteRequest(nodeA, 1, 0, 0);

        inMemoryB.registerHandler(new se.mouaz.aegisdb.transport.RaftRequestHandler() {
            @Override
            public CompletableFuture<RequestVoteResponse> handleRequestVote(RequestVoteRequest req) {
                return CompletableFuture.completedFuture(RequestVoteResponse.granted(1));
            }

            @Override
            public CompletableFuture<AppendEntriesResponse> handleAppendEntries(AppendEntriesRequest req) {
                return CompletableFuture.completedFuture(AppendEntriesResponse.success(1, 0));
            }

            @Override
            public CompletableFuture<se.mouaz.aegisdb.protocol.InstallSnapshotResponse> handleInstallSnapshot(se.mouaz.aegisdb.protocol.InstallSnapshotRequest req) {
                return CompletableFuture.completedFuture(se.mouaz.aegisdb.protocol.InstallSnapshotResponse.success(1));
            }
        });

        CompletableFuture<RequestVoteResponse> responseFuture = faultyA.requestVote(nodeB, request);
        RequestVoteResponse resp = responseFuture.get(2, TimeUnit.SECONDS);

        assertThat(resp.voteGranted()).isTrue();
        assertThat(faultyA.droppedMessagesCount()).isEqualTo(0);
        assertThat(faultyA.delayedMessagesCount()).isEqualTo(0);
    }

    @Test
    @DisplayName("Should drop message when DROP rule is active")
    void shouldDropMessageWhenRuleActive() {
        faultyA.addRule(FaultRule.drop("drop-all", nodeA, nodeB, 1.0));

        RequestVoteRequest request = new RequestVoteRequest(nodeA, 1, 0, 0);

        CompletableFuture<RequestVoteResponse> future = faultyA.requestVote(nodeB, request);

        assertThatThrownBy(() -> future.get(300, TimeUnit.MILLISECONDS))
                .isInstanceOf(TimeoutException.class);

        assertThat(faultyA.droppedMessagesCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should delay delivery when DELAY rule is active")
    void shouldDelayMessageWhenRuleActive() throws Exception {
        faultyA.addRule(FaultRule.delay("delay-100", nodeA, nodeB, Duration.ofMillis(150), 1.0));

        inMemoryB.registerHandler(new se.mouaz.aegisdb.transport.RaftRequestHandler() {
            @Override
            public CompletableFuture<RequestVoteResponse> handleRequestVote(RequestVoteRequest req) {
                return CompletableFuture.completedFuture(RequestVoteResponse.granted(1));
            }

            @Override
            public CompletableFuture<AppendEntriesResponse> handleAppendEntries(AppendEntriesRequest req) {
                return CompletableFuture.completedFuture(AppendEntriesResponse.success(1, 0));
            }

            @Override
            public CompletableFuture<se.mouaz.aegisdb.protocol.InstallSnapshotResponse> handleInstallSnapshot(se.mouaz.aegisdb.protocol.InstallSnapshotRequest req) {
                return CompletableFuture.completedFuture(se.mouaz.aegisdb.protocol.InstallSnapshotResponse.success(1));
            }
        });

        RequestVoteRequest request = new RequestVoteRequest(nodeA, 1, 0, 0);
        long start = System.currentTimeMillis();
        CompletableFuture<RequestVoteResponse> future = faultyA.requestVote(nodeB, request);

        RequestVoteResponse response = future.get(1, TimeUnit.SECONDS);
        long elapsed = System.currentTimeMillis() - start;

        assertThat(response.voteGranted()).isTrue();
        assertThat(elapsed).isGreaterThanOrEqualTo(140);
        assertThat(faultyA.delayedMessagesCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("Should block message and fail exceptionally when PARTITION rule is active")
    void shouldBlockPartitionedMessage() {
        faultyA.addRule(FaultRule.partition("part-ab", n -> n.equals(nodeA), n -> n.equals(nodeB)));

        RequestVoteRequest request = new RequestVoteRequest(nodeA, 1, 0, 0);
        CompletableFuture<RequestVoteResponse> future = faultyA.requestVote(nodeB, request);

        assertThatThrownBy(() -> future.get(1, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(se.mouaz.aegisdb.transport.TransportException.class);

        assertThat(faultyA.partitionBlockedCount()).isEqualTo(1);
    }
}
