package se.mouaz.aegisdb.raft;

import se.mouaz.aegisdb.common.ByteArrayKey;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.common.ShardId;
import se.mouaz.aegisdb.common.ShardLifecycle;
import se.mouaz.aegisdb.protocol.SplitCommandCodec;
import se.mouaz.aegisdb.protocol.pb.*;
import se.mouaz.aegisdb.raft.statemachine.KeyValueStateMachine;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages independent Raft consensus groups on a node (§28, Phase 1 Multi-Raft).
 * Enforces:
 * - Independent terms
 * - Independent elections
 * - Independent leaders
 * - Independent commit indexes
 * - Independent WAL / log state per group
 */
public class MultiRaftManager {

    private static final Logger log = LoggerFactory.getLogger(MultiRaftManager.class);

    private final NodeId localNodeId;
    private final Map<String, RaftNode> groups = new ConcurrentHashMap<>();
    private final Map<String, KeyValueStateMachine> stateMachines = new ConcurrentHashMap<>();

    public MultiRaftManager(NodeId localNodeId) {
        this.localNodeId = Objects.requireNonNull(localNodeId, "localNodeId cannot be null");
    }

    public void registerGroup(String raftGroupId, RaftNode node, KeyValueStateMachine stateMachine) {
        Objects.requireNonNull(raftGroupId, "raftGroupId cannot be null");
        Objects.requireNonNull(node, "node cannot be null");
        Objects.requireNonNull(stateMachine, "stateMachine cannot be null");

        groups.put(raftGroupId, node);
        stateMachines.put(raftGroupId, stateMachine);
        log.info("Registered Raft group {} on node {}", raftGroupId, localNodeId);
    }

    public Optional<RaftNode> getGroup(String raftGroupId) {
        return Optional.ofNullable(groups.get(raftGroupId));
    }

    public Optional<KeyValueStateMachine> getStateMachine(String raftGroupId) {
        return Optional.ofNullable(stateMachines.get(raftGroupId));
    }

    public boolean hasGroup(String raftGroupId) {
        return groups.containsKey(raftGroupId);
    }

    public void stopGroup(String raftGroupId) {
        RaftNode node = groups.remove(raftGroupId);
        if (node != null) {
            try {
                node.stop();
                log.info("Stopped Raft group {} on node {}", raftGroupId, localNodeId);
            } catch (Exception e) {
                log.warn("Error stopping Raft group {}", raftGroupId, e);
            }
        }
    }

    public void stopAll() {
        for (String groupId : new ArrayList<>(groups.keySet())) {
            stopGroup(groupId);
        }
    }

    /**
     * Proposes PrepareSplit to the parent Raft group.
     */
    public CompletableFuture<Long> proposePrepareSplit(
            String parentRaftGroupId,
            String splitOperationId,
            ShardId parentShardId,
            ShardId childShardId,
            String childRaftGroupId,
            ByteArrayKey splitKey,
            List<String> childReplicas,
            ShardEpochProto expectedParentEpoch,
            ShardEpochProto targetEpoch) {

        RaftNode parentNode = groups.get(parentRaftGroupId);
        if (parentNode == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("Parent Raft group not found: " + parentRaftGroupId));
        }

        PrepareSplitCommandProto cmd = PrepareSplitCommandProto.newBuilder()
                .setSplitOperationId(splitOperationId)
                .setParentShardId(parentShardId.toString())
                .setParentRaftGroupId(parentRaftGroupId)
                .setChildShardId(childShardId.toString())
                .setChildRaftGroupId(childRaftGroupId)
                .setSplitKey(ByteString.copyFrom(splitKey.getBytes()))
                .addAllChildReplicas(childReplicas)
                .setExpectedParentEpoch(expectedParentEpoch)
                .setTargetEpoch(targetEpoch)
                .build();

        byte[] encoded = SplitCommandCodec.encode(cmd);
        return parentNode.propose(encoded);
    }

    /**
     * Proposes SplitSnapshotBarrier to the parent Raft group after pre-existing PREPARED txns resolve.
     */
    public CompletableFuture<Long> proposeSnapshotBarrier(
            String parentRaftGroupId,
            String splitOperationId,
            ShardId parentShardId,
            ShardId childShardId,
            String childRaftGroupId) {

        RaftNode parentNode = groups.get(parentRaftGroupId);
        if (parentNode == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("Parent Raft group not found: " + parentRaftGroupId));
        }

        SplitSnapshotBarrierCommandProto cmd = SplitSnapshotBarrierCommandProto.newBuilder()
                .setSplitOperationId(splitOperationId)
                .setParentShardId(parentShardId.toString())
                .setParentRaftGroupId(parentRaftGroupId)
                .setChildShardId(childShardId.toString())
                .setChildRaftGroupId(childRaftGroupId)
                .build();

        byte[] encoded = SplitCommandCodec.encode(cmd);
        return parentNode.propose(encoded);
    }

    /**
     * Bootstraps the Child Raft Group with snapshot slice data.
     */
    public void bootstrapChildGroup(
            String childRaftGroupId,
            RaftNode childNode,
            KeyValueStateMachine childStateMachine,
            long snapshotBarrierIndex,
            byte[] snapshotSliceData) {

        childStateMachine.restoreSnapshot(snapshotBarrierIndex, snapshotSliceData);
        childStateMachine.setLifecycle(ShardLifecycle.READY);
        registerGroup(childRaftGroupId, childNode, childStateMachine);
        childNode.start();
        log.info("Bootstrapped child group {} at barrierIndex={}", childRaftGroupId, snapshotBarrierIndex);
    }

    /**
     * Commits child BootstrapComplete in the child's own Raft group (durable readiness).
     */
    public CompletableFuture<Long> commitChildBootstrapComplete(
            String childRaftGroupId,
            BootstrapCompleteCommandProto cmd) {

        RaftNode childNode = groups.get(childRaftGroupId);
        if (childNode == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("Child Raft group not found: " + childRaftGroupId));
        }

        byte[] encoded = SplitCommandCodec.encode(cmd);
        return childNode.propose(encoded);
    }

    /**
     * Activates child shard in its Raft group.
     */
    public CompletableFuture<Long> activateChildGroup(
            String childRaftGroupId,
            ActivateShardCommandProto cmd) {

        RaftNode childNode = groups.get(childRaftGroupId);
        if (childNode == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("Child Raft group not found: " + childRaftGroupId));
        }

        byte[] encoded = SplitCommandCodec.encode(cmd);
        return childNode.propose(encoded);
    }

    /**
     * Finalizes parent shard in its Raft group.
     */
    public CompletableFuture<Long> finalizeParentGroup(
            String parentRaftGroupId,
            FinalizeSplitCommandProto cmd) {

        RaftNode parentNode = groups.get(parentRaftGroupId);
        if (parentNode == null) {
            return CompletableFuture.failedFuture(new IllegalStateException("Parent Raft group not found: " + parentRaftGroupId));
        }

        byte[] encoded = SplitCommandCodec.encode(cmd);
        return parentNode.propose(encoded);
    }

    /**
     * Aborts split in parent group and tombstones child group.
     */
    public CompletableFuture<Void> abortSplit(
            String parentRaftGroupId,
            String childRaftGroupId,
            String splitOperationId,
            String reason) {

        CompletableFuture<Long> parentFuture = CompletableFuture.completedFuture(0L);
        RaftNode parentNode = groups.get(parentRaftGroupId);
        if (parentNode != null) {
            AbortSplitCommandProto parentAbort = AbortSplitCommandProto.newBuilder()
                    .setSplitOperationId(splitOperationId)
                    .setParentRaftGroupId(parentRaftGroupId)
                    .setChildRaftGroupId(childRaftGroupId != null ? childRaftGroupId : "")
                    .setReason(reason)
                    .build();
            parentFuture = parentNode.propose(SplitCommandCodec.encode(parentAbort));
        }

        if (childRaftGroupId != null && groups.containsKey(childRaftGroupId)) {
            RaftNode childNode = groups.get(childRaftGroupId);
            AbortBootstrapCommandProto childAbort = AbortBootstrapCommandProto.newBuilder()
                    .setSplitOperationId(splitOperationId)
                    .setChildRaftGroupId(childRaftGroupId)
                    .setReason(reason)
                    .build();
            return parentFuture.thenCompose(ignored -> childNode.propose(SplitCommandCodec.encode(childAbort)))
                    .thenAccept(ok -> stopGroup(childRaftGroupId));
        }

        return parentFuture.thenAccept(ignored -> {});
    }

    public NodeId localNodeId() {
        return localNodeId;
    }

    public Set<String> groupIds() {
        return Collections.unmodifiableSet(groups.keySet());
    }
}
