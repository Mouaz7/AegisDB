package se.mouaz.aegisdb.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.raft.NotLeaderException;
import se.mouaz.aegisdb.raft.statemachine.KeyValueStateMachine;
import se.mouaz.aegisdb.raft.statemachine.KvCommand;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class AegisDbClientTest {

    private KeyValueStateMachine stateMachine;
    private NodeId leaderId;
    private NodeId followerId;
    private AegisDbClient client;

    @BeforeEach
    void setUp() {
        stateMachine = new KeyValueStateMachine();
        leaderId = NodeId.of("node-leader");
        followerId = NodeId.of("node-follower");
    }

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void testPutGetDeleteWithLeader() throws Exception {
        client = new DefaultAegisDbClient(
                List.of(leaderId),
                (target, cmd) -> {
                    byte[] res = stateMachine.apply(1L, cmd);
                    return CompletableFuture.completedFuture(res);
                }
        );

        // PUT
        client.putString("user:alice", "Alice Wonder").get();

        // GET
        Optional<String> getRes = client.getString("user:alice").get();
        assertThat(getRes).isPresent();
        assertThat(getRes.get()).isEqualTo("Alice Wonder");

        // Non-existent key
        Optional<String> missing = client.getString("user:bob").get();
        assertThat(missing).isEmpty();

        // DELETE
        Optional<byte[]> deleted = client.delete("user:alice").get();
        assertThat(deleted).isPresent();
        assertThat(new String(deleted.get(), StandardCharsets.UTF_8)).isEqualTo("Alice Wonder");

        // Verify deleted
        assertThat(client.getString("user:alice").get()).isEmpty();
    }

    @Test
    void testAutomaticRedirectFromFollowerToLeader() throws Exception {
        AtomicInteger followerInvocations = new AtomicInteger(0);
        AtomicInteger leaderInvocations = new AtomicInteger(0);

        // Seed with follower first
        client = new DefaultAegisDbClient(
                List.of(followerId, leaderId),
                (target, cmd) -> {
                    if (target.equals(followerId)) {
                        followerInvocations.incrementAndGet();
                        // Follower returns NotLeaderException with redirect to leader
                        return CompletableFuture.failedFuture(new NotLeaderException(leaderId, 2L));
                    } else if (target.equals(leaderId)) {
                        leaderInvocations.incrementAndGet();
                        byte[] res = stateMachine.apply(1L, cmd);
                        return CompletableFuture.completedFuture(res);
                    }
                    return CompletableFuture.failedFuture(new IllegalArgumentException("Unknown: " + target));
                },
                5,
                Duration.ofMillis(10),
                Duration.ofMillis(100),
                null
        );

        // First call should be routed to follower, redirect to leader, and succeed
        client.putString("order:1", "Created").get();

        assertThat(followerInvocations.get()).isEqualTo(1);
        assertThat(leaderInvocations.get()).isEqualTo(1);
        assertThat(client.currentLeader()).contains(leaderId);

        // Second call should directly go to cached leader without touching follower
        client.putString("order:2", "Shipped").get();
        assertThat(followerInvocations.get()).isEqualTo(1); // Still 1
        assertThat(leaderInvocations.get()).isEqualTo(2);

        // Verify values
        assertThat(client.getString("order:1").get()).contains("Created");
        assertThat(client.getString("order:2").get()).contains("Shipped");
    }

    @Test
    void testRetryUntilLeaderElected() throws Exception {
        AtomicInteger attempts = new AtomicInteger(0);

        // Simulate 2 initial election timeouts / no-leader responses, then success
        client = new DefaultAegisDbClient(
                List.of(leaderId),
                (target, cmd) -> {
                    int count = attempts.incrementAndGet();
                    if (count < 3) {
                        return CompletableFuture.failedFuture(new NotLeaderException(null, 1L)); // Leader unknown yet
                    }
                    byte[] res = stateMachine.apply((long) count, cmd);
                    return CompletableFuture.completedFuture(res);
                },
                5,
                Duration.ofMillis(10),
                Duration.ofMillis(100),
                null
        );

        client.putString("key", "val").get();
        assertThat(attempts.get()).isGreaterThanOrEqualTo(3);
        assertThat(client.getString("key").get()).contains("val");
    }
}
