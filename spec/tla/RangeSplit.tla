---------------------------- MODULE RangeSplit ----------------------------
EXTENDS Naturals, FiniteSets

CONSTANTS 
    Keys,           \* Set of discrete key points, e.g. {1, 2, 3, 4, 5}
    MinKey,         \* Minimum boundary key (representing -infinity)
    MaxKey,         \* Maximum boundary key (representing +infinity)
    SplitKey        \* Split boundary key, MinKey < SplitKey <= MaxKey

VARIABLES
    parentState,        \* "ACTIVE", "SPLITTING", "FINALIZED"
    childState,         \* "UNBORN", "BOOTSTRAPPING", "READY", "ACTIVE", "TOMBSTONED"
    writeFenceActive,   \* Boolean: TRUE when writes >= SplitKey are rejected on parent
    preparedTxCount,    \* Nat: count of in-flight PREPARED 2PC transactions
    barrierCommitted,   \* Boolean: TRUE when snapshot barrier index committed
    snapshotTaken,      \* Boolean: TRUE when MVCC snapshot slice extracted
    cutoverCommitted,   \* Boolean: TRUE when Metadata Raft committed TopologyCutover
    topologyVersion,    \* Nat: cluster-wide monotonic version
    publishedTopology,  \* Record mapping "parent" -> [start, end), "child" -> [start, end)
    staleKeysPruned     \* Boolean: TRUE when parent delayed GC executed

vars == <<parentState, childState, writeFenceActive, preparedTxCount, 
          barrierCommitted, snapshotTaken, cutoverCommitted, topologyVersion, 
          publishedTopology, staleKeysPruned>>

-----------------------------------------------------------------------------
(* Range helpers *)

Range(start, end) == {k \in Keys : k >= start /\ k < end}

ParentFullRange == Range(MinKey, MaxKey)
ParentLeftRange == Range(MinKey, SplitKey)
ChildRightRange == Range(SplitKey, MaxKey)

-----------------------------------------------------------------------------
(* Initial State *)

Init ==
    /\ parentState = "ACTIVE"
    /\ childState = "UNBORN"
    /\ writeFenceActive = FALSE
    /\ preparedTxCount \in 0..1
    /\ barrierCommitted = FALSE
    /\ snapshotTaken = FALSE
    /\ cutoverCommitted = FALSE
    /\ topologyVersion = 1
    /\ publishedTopology = [parent |-> ParentFullRange, child |-> {}]
    /\ staleKeysPruned = FALSE

-----------------------------------------------------------------------------
(* Actions *)

PrepareSplit ==
    /\ parentState = "ACTIVE"
    /\ childState = "UNBORN"
    /\ ~cutoverCommitted
    /\ parentState' = "SPLITTING"
    /\ writeFenceActive' = TRUE
    /\ childState' = "BOOTSTRAPPING"
    /\ UNCHANGED <<preparedTxCount, barrierCommitted, snapshotTaken, 
                   cutoverCommitted, topologyVersion, publishedTopology, staleKeysPruned>>

ResolvePreparedTx ==
    /\ parentState = "SPLITTING"
    /\ ~barrierCommitted
    /\ preparedTxCount > 0
    /\ preparedTxCount' = preparedTxCount - 1
    /\ UNCHANGED <<parentState, childState, writeFenceActive, barrierCommitted, 
                   snapshotTaken, cutoverCommitted, topologyVersion, publishedTopology, staleKeysPruned>>

CommitSnapshotBarrier ==
    /\ parentState = "SPLITTING"
    /\ ~barrierCommitted
    /\ preparedTxCount = 0
    /\ barrierCommitted' = TRUE
    /\ snapshotTaken' = TRUE
    /\ UNCHANGED <<parentState, childState, writeFenceActive, preparedTxCount, 
                   cutoverCommitted, topologyVersion, publishedTopology, staleKeysPruned>>

BootstrapChild ==
    /\ childState = "BOOTSTRAPPING"
    /\ snapshotTaken
    /\ childState' = "READY"
    /\ UNCHANGED <<parentState, writeFenceActive, preparedTxCount, barrierCommitted, 
                   snapshotTaken, cutoverCommitted, topologyVersion, publishedTopology, staleKeysPruned>>

CommitMetadataCutover ==
    /\ childState = "READY"
    /\ ~cutoverCommitted
    /\ cutoverCommitted' = TRUE
    /\ topologyVersion' = topologyVersion + 1
    /\ UNCHANGED <<parentState, childState, writeFenceActive, preparedTxCount, 
                   barrierCommitted, snapshotTaken, publishedTopology, staleKeysPruned>>

FinalizeParent ==
    /\ cutoverCommitted
    /\ parentState = "SPLITTING"
    /\ parentState' = "FINALIZED"
    /\ UNCHANGED <<childState, writeFenceActive, preparedTxCount, barrierCommitted, 
                   snapshotTaken, cutoverCommitted, topologyVersion, publishedTopology, staleKeysPruned>>

ActivateChild ==
    /\ cutoverCommitted
    /\ childState = "READY"
    /\ childState' = "ACTIVE"
    /\ UNCHANGED <<parentState, writeFenceActive, preparedTxCount, barrierCommitted, 
                   snapshotTaken, cutoverCommitted, topologyVersion, publishedTopology, staleKeysPruned>>

PublishTopologySnapshot ==
    /\ cutoverCommitted
    /\ publishedTopology.child = {}
    /\ publishedTopology' = [parent |-> ParentLeftRange, child |-> ChildRightRange]
    /\ UNCHANGED <<parentState, childState, writeFenceActive, preparedTxCount, 
                   barrierCommitted, snapshotTaken, cutoverCommitted, topologyVersion, staleKeysPruned>>

PruneStaleKeys ==
    /\ parentState = "FINALIZED"
    /\ childState = "ACTIVE"
    /\ publishedTopology.child = ChildRightRange
    /\ ~staleKeysPruned
    /\ staleKeysPruned' = TRUE
    /\ UNCHANGED <<parentState, childState, writeFenceActive, preparedTxCount, 
                   barrierCommitted, snapshotTaken, cutoverCommitted, topologyVersion, publishedTopology>>

AbortSplit ==
    /\ parentState = "SPLITTING"
    /\ ~cutoverCommitted
    /\ parentState' = "ACTIVE"
    /\ childState' = "TOMBSTONED"
    /\ writeFenceActive' = FALSE
    /\ UNCHANGED <<preparedTxCount, barrierCommitted, snapshotTaken, 
                   cutoverCommitted, topologyVersion, publishedTopology, staleKeysPruned>>

-----------------------------------------------------------------------------
(* Next State Relation *)

Next ==
    \/ PrepareSplit
    \/ ResolvePreparedTx
    \/ CommitSnapshotBarrier
    \/ BootstrapChild
    \/ CommitMetadataCutover
    \/ FinalizeParent
    \/ ActivateChild
    \/ PublishTopologySnapshot
    \/ PruneStaleKeys
    \/ AbortSplit

-----------------------------------------------------------------------------
(* Authoritative Shard Invariants *)

AuthoritativeRange(shard) ==
    IF shard = "parent" THEN
        IF ~cutoverCommitted THEN ParentFullRange ELSE ParentLeftRange
    ELSE IF shard = "child" THEN
        IF cutoverCommitted THEN ChildRightRange ELSE {}
    ELSE {}

SingleAuthoritativeOwner ==
    \A k \in Keys :
        \/ (k \in AuthoritativeRange("parent") /\ k \notin AuthoritativeRange("child"))
        \/ (k \in AuthoritativeRange("child") /\ k \notin AuthoritativeRange("parent"))

NoAuthoritativeOverlap ==
    AuthoritativeRange("parent") \cap AuthoritativeRange("child") = {}

NoAuthoritativeGaps ==
    AuthoritativeRange("parent") \cup AuthoritativeRange("child") = ParentFullRange

CutoverIrreversibility ==
    cutoverCommitted => (parentState /= "ACTIVE" /\ childState /= "TOMBSTONED")

WriteFenceEnforced ==
    (parentState = "SPLITTING" /\ writeFenceActive) => 
        \A k \in ChildRightRange : k \notin ParentLeftRange

SnapshotPreparedDrainSafety ==
    snapshotTaken => (preparedTxCount = 0)

RoutingSafety ==
    publishedTopology.child = ChildRightRange => cutoverCommitted

=============================================================================
