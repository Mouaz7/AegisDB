package se.mouaz.aegisdb.raft.election;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.mouaz.aegisdb.common.NodeId;
import se.mouaz.aegisdb.protocol.RequestVoteRequest;
import se.mouaz.aegisdb.protocol.RequestVoteResponse;
import se.mouaz.aegisdb.raft.log.RaftLog;
import se.mouaz.aegisdb.raft.log.RaftLogEntry;
import se.mouaz.aegisdb.raft.state.PersistentRaftState;
import se.mouaz.aegisdb.raft.state.RaftState;
import se.mouaz.aegisdb.raft.time.Clock;
import se.mouaz.aegisdb.raft.time.Scheduler;

import java.time.Duration;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class RequestVoteHandlerTest {

    private RaftState state;
    private PersistentRaftState persistentState;
    private RaftLog raftLog;
    private RequestVoteHandler handler;
    private ElectionTimer electionTimer;

    private final NodeId localNodeId = NodeId.of("local");
    private final NodeId candidateId = NodeId.of("candidate");
    private final NodeId otherCandidateId = NodeId.of("otherCandidate");

    @BeforeEach
    void setUp() {
        persistentState = new PersistentRaftState();

        state = new RaftState(localNodeId, persistentState, null) {
            @Override public void becomeFollower(long newTerm, NodeId leaderId) {
                persistent().setCurrentTerm(newTerm);
            }
        };

        raftLog = new RaftLog();
        
        Scheduler mockScheduler = new Scheduler() {
            @Override public CancellableTask schedule(Runnable command, Duration delay) {
                return new CancellableTask() {
                    @Override public void cancel() {}
                    @Override public boolean isCancelled() { return false; }
                };
            }
            @Override public CancellableTask scheduleAtFixedRate(Runnable command, Duration initialDelay, Duration period) {
                return null;
            }
            @Override public void close() {}
        };
        Clock mockClock = System::currentTimeMillis;
        
        electionTimer = new ElectionTimer(mockScheduler, mockClock, Duration.ofMillis(150), Duration.ofMillis(300), new Random(), () -> {});
        handler = new RequestVoteHandler(state, electionTimer, raftLog);
    }

    private void setLocalLog(long term, long index) {
        // Build up the log up to the specified index and term
        for (long i = raftLog.lastLogIndex() + 1; i < index; i++) {
            raftLog.append(new RaftLogEntry(i, 1, new byte[0])); // fill gaps with dummy term 1
        }
        if (index > 0) {
            raftLog.append(new RaftLogEntry(index, term, new byte[0]));
        }
    }

    @Test
    void testCandidateTermLower_Deny() {
        persistentState.setCurrentTerm(5);
        RequestVoteRequest request = new RequestVoteRequest(candidateId, 4, 0, 0);
        RequestVoteResponse response = handler.handleRequestVote(request);
        
        assertFalse(response.voteGranted());
        assertEquals(5, response.term());
    }

    @Test
    void testCandidateTermHigher_ProcessNewTerm() {
        persistentState.setCurrentTerm(2);
        setLocalLog(1, 1);
        RequestVoteRequest request = new RequestVoteRequest(candidateId, 3, 1, 1);
        RequestVoteResponse response = handler.handleRequestVote(request);
        
        assertTrue(response.voteGranted());
        assertEquals(3, persistentState.currentTerm());
        assertEquals(candidateId, persistentState.votedFor().orElse(null));
    }

    @Test
    void testCandidateLastLogTermLower_Deny() {
        persistentState.setCurrentTerm(2);
        setLocalLog(2, 5); // local log term 2
        
        RequestVoteRequest request = new RequestVoteRequest(candidateId, 3, 6, 1); // candidate log term 1
        RequestVoteResponse response = handler.handleRequestVote(request);
        
        assertFalse(response.voteGranted());
    }

    @Test
    void testCandidateLastLogTermHigher_Allow() {
        persistentState.setCurrentTerm(2);
        setLocalLog(2, 5); // local log term 2
        
        RequestVoteRequest request = new RequestVoteRequest(candidateId, 3, 2, 3); // candidate log term 3
        RequestVoteResponse response = handler.handleRequestVote(request);
        
        assertTrue(response.voteGranted());
    }

    @Test
    void testSameLastLogTerm_LowerIndex_Deny() {
        persistentState.setCurrentTerm(2);
        setLocalLog(2, 5);
        
        RequestVoteRequest request = new RequestVoteRequest(candidateId, 3, 4, 2);
        RequestVoteResponse response = handler.handleRequestVote(request);
        
        assertFalse(response.voteGranted());
    }

    @Test
    void testSameLastLogTerm_EqualIndex_Allow() {
        persistentState.setCurrentTerm(2);
        setLocalLog(2, 5);
        
        RequestVoteRequest request = new RequestVoteRequest(candidateId, 3, 5, 2);
        RequestVoteResponse response = handler.handleRequestVote(request);
        
        assertTrue(response.voteGranted());
    }

    @Test
    void testSameLastLogTerm_HigherIndex_Allow() {
        persistentState.setCurrentTerm(2);
        setLocalLog(2, 5);
        
        RequestVoteRequest request = new RequestVoteRequest(candidateId, 3, 6, 2);
        RequestVoteResponse response = handler.handleRequestVote(request);
        
        assertTrue(response.voteGranted());
    }

    @Test
    void testAlreadyVotedForAnotherNode_Deny() {
        persistentState.setCurrentTerm(3);
        persistentState.setVotedFor(otherCandidateId);
        
        RequestVoteRequest request = new RequestVoteRequest(candidateId, 3, 0, 0);
        RequestVoteResponse response = handler.handleRequestVote(request);
        
        assertFalse(response.voteGranted());
    }

    @Test
    void testAlreadyVotedForSameCandidate_Allow() {
        persistentState.setCurrentTerm(3);
        persistentState.setVotedFor(candidateId);
        
        RequestVoteRequest request = new RequestVoteRequest(candidateId, 3, 0, 0);
        RequestVoteResponse response = handler.handleRequestVote(request);
        
        assertTrue(response.voteGranted());
    }
}
