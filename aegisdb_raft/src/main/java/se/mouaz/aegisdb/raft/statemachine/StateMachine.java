package se.mouaz.aegisdb.raft.statemachine;

/**
 * Replicated state machine interface conforming to Raft consensus (§5.3, §7; US009, US010).
 * State machines must be deterministic: identical sequences of committed commands yield identical state.
 */
public interface StateMachine {

    /**
     * Applies a committed log entry payload to the state machine.
     *
     * @param index log index of the command
     * @param command raw bytes of the client command
     * @return result bytes of the operation (e.g. value, previous value, or confirmation)
     */
    byte[] apply(long index, byte[] command);

    /**
     * Serializes the current state machine state into a byte array for snapshot creation (Raft §7).
     */
    byte[] takeSnapshot();

    /**
     * Restores the state machine state from a snapshot byte array.
     * Replaces any current in-memory state.
     *
     * @param lastIncludedIndex the last log index included in this snapshot
     * @param snapshotData raw state data produced by takeSnapshot
     */
    void restoreSnapshot(long lastIncludedIndex, byte[] snapshotData);

    /**
     * Returns the highest log index applied to this state machine.
     */
    long lastAppliedIndex();
}
