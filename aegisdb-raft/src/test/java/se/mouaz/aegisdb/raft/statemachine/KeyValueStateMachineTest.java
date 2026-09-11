package se.mouaz.aegisdb.raft.statemachine;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class KeyValueStateMachineTest {

    private KeyValueStateMachine stateMachine;

    @BeforeEach
    void setUp() {
        stateMachine = new KeyValueStateMachine();
    }

    @Test
    void testPutGetDeleteOperations() {
        KvCommand putCmd = KvCommand.put("user:100", "Alice".getBytes(StandardCharsets.UTF_8));
        stateMachine.apply(1L, putCmd.toBytes());

        assertThat(stateMachine.lastAppliedIndex()).isEqualTo(1L);
        assertThat(stateMachine.size()).isEqualTo(1);
        assertThat(new String(stateMachine.get("user:100"), StandardCharsets.UTF_8)).isEqualTo("Alice");

        // Overwrite
        KvCommand updateCmd = KvCommand.put("user:100", "Alice Smith".getBytes(StandardCharsets.UTF_8));
        byte[] prev = stateMachine.apply(2L, updateCmd.toBytes());
        assertThat(new String(prev, StandardCharsets.UTF_8)).isEqualTo("Alice");
        assertThat(new String(stateMachine.get("user:100"), StandardCharsets.UTF_8)).isEqualTo("Alice Smith");

        // Delete
        KvCommand delCmd = KvCommand.delete("user:100");
        byte[] deleted = stateMachine.apply(3L, delCmd.toBytes());
        assertThat(new String(deleted, StandardCharsets.UTF_8)).isEqualTo("Alice Smith");
        assertThat(stateMachine.get("user:100")).isNull();
        assertThat(stateMachine.size()).isEqualTo(0);
    }

    @Test
    void testTakeAndRestoreSnapshot() {
        for (int i = 1; i <= 50; i++) {
            KvCommand cmd = KvCommand.put("key:" + i, ("val:" + i).getBytes(StandardCharsets.UTF_8));
            stateMachine.apply((long) i, cmd.toBytes());
        }

        assertThat(stateMachine.size()).isEqualTo(50);
        assertThat(stateMachine.lastAppliedIndex()).isEqualTo(50L);

        byte[] snapshotData = stateMachine.takeSnapshot();
        assertThat(snapshotData).isNotEmpty();

        // Restore onto a fresh state machine
        KeyValueStateMachine restoredStateMachine = new KeyValueStateMachine();
        restoredStateMachine.restoreSnapshot(50L, snapshotData);

        assertThat(restoredStateMachine.lastAppliedIndex()).isEqualTo(50L);
        assertThat(restoredStateMachine.size()).isEqualTo(50);
        for (int i = 1; i <= 50; i++) {
            assertThat(new String(restoredStateMachine.get("key:" + i), StandardCharsets.UTF_8))
                    .isEqualTo("val:" + i);
        }
    }
}
