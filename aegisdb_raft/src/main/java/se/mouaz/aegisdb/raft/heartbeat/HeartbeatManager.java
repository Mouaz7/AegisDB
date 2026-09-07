package se.mouaz.aegisdb.raft.heartbeat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.mouaz.aegisdb.common.ClusterConfiguration;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.protocol.AppendEntriesRequest;
import se.mouaz.aegisdb.raft.state.RaftRole;
import se.mouaz.aegisdb.raft.state.RaftState;
import se.mouaz.aegisdb.raft.time.Scheduler;
import se.mouaz.aegisdb.transport.RaftTransport;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Manages sending periodic heartbeats from the leader to all cluster followers (Section 21).
 */
public class HeartbeatManager {
    private static final Logger log = LoggerFactory.getLogger(HeartbeatManager.class);

    private final RaftState state;
    private final RaftTransport transport;
    private final ClusterConfiguration clusterConfig;
    private final Scheduler scheduler;
    private final Duration heartbeatInterval;
    private final Consumer<AppendEntriesRequest> heartbeatSender;

    private Scheduler.CancellableTask heartbeatTask;

    public HeartbeatManager(RaftState state,
                            RaftTransport transport,
                            ClusterConfiguration clusterConfig,
                            Scheduler scheduler,
                            Duration heartbeatInterval,
                            Consumer<AppendEntriesRequest> heartbeatSender) {
        this.state = Objects.requireNonNull(state, "state cannot be null");
        this.transport = Objects.requireNonNull(transport, "transport cannot be null");
        this.clusterConfig = Objects.requireNonNull(clusterConfig, "clusterConfig cannot be null");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler cannot be null");
        this.heartbeatInterval = heartbeatInterval != null ? heartbeatInterval : Duration.ofMillis(50);
        this.heartbeatSender = heartbeatSender;
    }

    public synchronized void startHeartbeats() {
        stopHeartbeats();
        log.info("Leader {} starting heartbeats every {} ms", state.localNodeId(), heartbeatInterval.toMillis());
        sendHeartbeats(); // send immediately upon election (§5.2)
        heartbeatTask = scheduler.scheduleAtFixedRate(this::sendHeartbeats, heartbeatInterval, heartbeatInterval);
    }

    public synchronized void stopHeartbeats() {
        if (heartbeatTask != null && !heartbeatTask.isCancelled()) {
            heartbeatTask.cancel();
            heartbeatTask = null;
            log.debug("Heartbeats stopped for node {}", state.localNodeId());
        }
    }

    public void sendHeartbeats() {
        if (state.role() != RaftRole.LEADER) {
            stopHeartbeats();
            return;
        }

        long term = state.currentTerm();
        NodeId localId = state.localNodeId();
        AppendEntriesRequest heartbeat = AppendEntriesRequest.heartbeat(term, localId, 0, 0, 0);

        if (heartbeatSender != null) {
            heartbeatSender.accept(heartbeat);
        } else {
            for (NodeId peer : clusterConfig.members().keySet()) {
                if (!peer.equals(localId)) {
                    transport.appendEntries(peer, heartbeat);
                }
            }
        }
    }
}
