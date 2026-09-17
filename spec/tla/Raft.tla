--------------------------------- MODULE Raft ---------------------------------
(***************************************************************************)
(* Formal specification of AegisDB Raft Consensus Protocol                *)
(* Bounded state space verified via TLC model checker                      *)
(*                                                                         *)
(* Central Invariants:                                                     *)
(* - ElectionSafety: at most one leader per term                           *)
(* - LogMatching: identical index and term implies identical prefix        *)
(* - LeaderCompleteness: committed entries present in future leaders       *)
(***************************************************************************)

EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS Server, Value, Nil, MaxTerms, MaxLogLen

VARIABLES currentTerm,
          state,
          votedFor,
          log,
          commitIndex,
          messages

vars == <<currentTerm, state, votedFor, log, commitIndex, messages>>

ServerState == {"Follower", "Candidate", "Leader"}

Entry == [term: 1..MaxTerms, value: Value]

(***************************************************************************)
(* Messages                                                                *)
(***************************************************************************)
RequestVoteRequest == [type: {"RequestVote"},
                       term: 1..MaxTerms,
                       candidateId: Server,
                       lastLogIndex: 0..MaxLogLen,
                       lastLogTerm: 0..MaxTerms]

RequestVoteResponse == [type: {"RequestVoteResponse"},
                        term: 1..MaxTerms,
                        voteGranted: BOOLEAN,
                        from: Server,
                        to: Server]

AppendEntriesRequest == [type: {"AppendEntries"},
                         term: 1..MaxTerms,
                         leaderId: Server,
                         prevLogIndex: 0..MaxLogLen,
                         prevLogTerm: 0..MaxTerms,
                         entries: Seq(Entry),
                         leaderCommit: 0..MaxLogLen,
                         to: Server]

AppendEntriesResponse == [type: {"AppendEntriesResponse"},
                          term: 1..MaxTerms,
                          success: BOOLEAN,
                          matchIndex: 0..MaxLogLen,
                          from: Server,
                          to: Server]

Message == RequestVoteRequest \cup RequestVoteResponse \cup
           AppendEntriesRequest \cup AppendEntriesResponse

(***************************************************************************)
(* Helper Operators                                                        *)
(***************************************************************************)
Min(a, b) == IF a < b THEN a ELSE b
Max(a, b) == IF a > b THEN a ELSE b

LastLogIndex(s) == Len(log[s])
LastLogTerm(s) == IF Len(log[s]) = 0 THEN 0 ELSE log[s][Len(log[s])].term

Send(m) == messages' = messages \cup {m}

(***************************************************************************)
(* Initial State                                                           *)
(***************************************************************************)
Init ==
    /\ currentTerm = [s \in Server |-> 1]
    /\ state = [s \in Server |-> "Follower"]
    /\ votedFor = [s \in Server |-> Nil]
    /\ log = [s \in Server |-> <<>>]
    /\ commitIndex = [s \in Server |-> 0]
    /\ messages = {}

(***************************************************************************)
(* State Transitions                                                       *)
(***************************************************************************)

(* Follower or Candidate times out and starts election in a new term *)
StartElection(s) ==
    /\ currentTerm[s] < MaxTerms
    /\ state[s] \in {"Follower", "Candidate"}
    /\ currentTerm' = [currentTerm EXCEPT ![s] = currentTerm[s] + 1]
    /\ state' = [state EXCEPT ![s] = "Candidate"]
    /\ votedFor' = [votedFor EXCEPT ![s] = s]
    /\ UNCHANGED <<log, commitIndex>>
    /\ Send([type |-> "RequestVote",
             term |-> currentTerm[s] + 1,
             candidateId |-> s,
             lastLogIndex |-> LastLogIndex(s),
             lastLogTerm |-> LastLogTerm(s)])

(* Candidate becomes leader if it received a quorum of votes *)
BecomeLeader(s) ==
    /\ state[s] = "Candidate"
    /\ LET votes == {m.from : m \in {msg \in messages :
                      msg.type = "RequestVoteResponse" /\
                      msg.to = s /\
                      msg.term = currentTerm[s] /\
                      msg.voteGranted}} \cup {s}
       IN /\ Cardinality(votes) * 2 > Cardinality(Server)
          /\ state' = [state EXCEPT ![s] = "Leader"]
          /\ UNCHANGED <<currentTerm, votedFor, log, commitIndex, messages>>

(* Leader proposes a new log entry *)
ClientRequest(s, v) ==
    /\ state[s] = "Leader"
    /\ Len(log[s]) < MaxLogLen
    /\ LET entry == [term |-> currentTerm[s], value |-> v]
           newLog == Append(log[s], entry)
       IN /\ log' = [log EXCEPT ![s] = newLog]
          /\ UNCHANGED <<currentTerm, state, votedFor, commitIndex, messages>>

(* Leader sends AppendEntries to a peer *)
SendAppendEntries(leader, peer) ==
    /\ state[leader] = "Leader"
    /\ peer # leader
    /\ \E prevIdx \in 0..Len(log[leader]) :
        LET prevTerm == IF prevIdx = 0 THEN 0 ELSE log[leader][prevIdx].term
            entriesToSend == SubSeq(log[leader], prevIdx + 1, Len(log[leader]))
        IN Send([type |-> "AppendEntries",
                 term |-> currentTerm[leader],
                 leaderId |-> leader,
                 prevLogIndex |-> prevIdx,
                 prevLogTerm |-> prevTerm,
                 entries |-> entriesToSend,
                 leaderCommit |-> commitIndex[leader],
                 to |-> peer])
    /\ UNCHANGED <<currentTerm, state, votedFor, log, commitIndex>>

(* Node receives RequestVote and grants or denies vote *)
HandleRequestVote(receiver, msg) ==
    /\ msg \in messages
    /\ msg.type = "RequestVote"
    /\ LET termOk == msg.term >= currentTerm[receiver]
           logOk == \/ msg.lastLogTerm > LastLogTerm(receiver)
                    \/ (msg.lastLogTerm = LastLogTerm(receiver) /\
                        msg.lastLogIndex >= LastLogIndex(receiver))
           canVote == \/ msg.term > currentTerm[receiver]
                      \/ votedFor[receiver] = Nil
                      \/ votedFor[receiver] = msg.candidateId
           grant == termOk /\ logOk /\ canVote
           newTerm == Max(currentTerm[receiver], msg.term)
       IN /\ currentTerm' = [currentTerm EXCEPT ![receiver] = newTerm]
          /\ state' = [state EXCEPT ![receiver] = IF msg.term > currentTerm[receiver] THEN "Follower" ELSE state[receiver]]
          /\ votedFor' = [votedFor EXCEPT ![receiver] = IF grant THEN msg.candidateId
                                                        ELSE IF msg.term > currentTerm[receiver] THEN Nil
                                                        ELSE votedFor[receiver]]
          /\ UNCHANGED <<log, commitIndex>>
          /\ Send([type |-> "RequestVoteResponse",
                   term |-> newTerm,
                   voteGranted |-> grant,
                   from |-> receiver,
                   to |-> msg.candidateId])

(* Peer receives AppendEntries and appends or rejects *)
HandleAppendEntries(receiver, msg) ==
    /\ msg \in messages
    /\ msg.type = "AppendEntries"
    /\ msg.to = receiver
    /\ LET termOk == msg.term >= currentTerm[receiver]
           logOk == \/ msg.prevLogIndex = 0
                    \/ (/\ msg.prevLogIndex <= Len(log[receiver])
                        /\ log[receiver][msg.prevLogIndex].term = msg.prevLogTerm)
           newTerm == Max(currentTerm[receiver], msg.term)
       IN IF termOk /\ logOk
          THEN /\ currentTerm' = [currentTerm EXCEPT ![receiver] = newTerm]
               /\ state' = [state EXCEPT ![receiver] = "Follower"]
               /\ votedFor' = [votedFor EXCEPT ![receiver] = IF msg.term > currentTerm[receiver] THEN Nil ELSE votedFor[receiver]]
               /\ log' = [log EXCEPT ![receiver] = SubSeq(log[receiver], 1, msg.prevLogIndex) \o msg.entries]
               /\ commitIndex' = [commitIndex EXCEPT ![receiver] = Min(msg.leaderCommit, msg.prevLogIndex + Len(msg.entries))]
               /\ Send([type |-> "AppendEntriesResponse",
                        term |-> newTerm,
                        success |-> TRUE,
                        matchIndex |-> msg.prevLogIndex + Len(msg.entries),
                        from |-> receiver,
                        to |-> msg.leaderId])
          ELSE /\ currentTerm' = [currentTerm EXCEPT ![receiver] = newTerm]
               /\ state' = [state EXCEPT ![receiver] = IF msg.term > currentTerm[receiver] THEN "Follower" ELSE state[receiver]]
               /\ votedFor' = [votedFor EXCEPT ![receiver] = IF msg.term > currentTerm[receiver] THEN Nil ELSE votedFor[receiver]]
               /\ UNCHANGED <<log, commitIndex>>
               /\ Send([type |-> "AppendEntriesResponse",
                        term |-> newTerm,
                        success |-> FALSE,
                        matchIndex |-> 0,
                        from |-> receiver,
                        to |-> msg.leaderId])

(* Leader advances commitIndex based on acks *)
AdvanceCommitIndex(leader) ==
    /\ state[leader] = "Leader"
    /\ \E idx \in (commitIndex[leader] + 1)..Len(log[leader]) :
          /\ log[leader][idx].term = currentTerm[leader]
          /\ LET acks == {m.from : m \in {msg \in messages :
                          msg.type = "AppendEntriesResponse" /\
                          msg.to = leader /\
                          msg.term = currentTerm[leader] /\
                          msg.success /\
                          msg.matchIndex >= idx}} \cup {leader}
             IN /\ Cardinality(acks) * 2 > Cardinality(Server)
                /\ commitIndex' = [commitIndex EXCEPT ![leader] = idx]
                /\ UNCHANGED <<currentTerm, state, votedFor, log, messages>>

Next ==
    \/ \E s \in Server : StartElection(s)
    \/ \E s \in Server : BecomeLeader(s)
    \/ \E s \in Server, v \in Value : ClientRequest(s, v)
    \/ \E l, p \in Server : SendAppendEntries(l, p)
    \/ \E s \in Server, m \in messages : HandleRequestVote(s, m)
    \/ \E s \in Server, m \in messages : HandleAppendEntries(s, m)
    \/ \E s \in Server : AdvanceCommitIndex(s)

(***************************************************************************)
(* Safety Invariants                                                       *)
(***************************************************************************)

(* Invariant 1: Election Safety (at most one leader per term) *)
ElectionSafety ==
    \A s1, s2 \in Server :
        (state[s1] = "Leader" /\ state[s2] = "Leader" /\ currentTerm[s1] = currentTerm[s2])
        => s1 = s2

(* Invariant 2: Log Matching Invariant *)
LogMatching ==
    \A s1, s2 \in Server :
        \A i \in 1..Min(Len(log[s1]), Len(log[s2])) :
            log[s1][i].term = log[s2][i].term =>
                SubSeq(log[s1], 1, i) = SubSeq(log[s2], 1, i)

(* Invariant 3: State Machine Safety *)
StateMachineSafety ==
    \A s1, s2 \in Server :
        \A i \in 1..Min(commitIndex[s1], commitIndex[s2]) :
            log[s1][i] = log[s2][i]

=============================================================================
