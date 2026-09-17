-------------------------------- MODULE RaftMC --------------------------------
EXTENDS Raft

CONSTANTS n1, n2, n3, v1, v2

MCServer == {n1, n2, n3}
MCValue == {v1, v2}
MCNil == "Nil"
MCMaxTerms == 3
MCMaxLogLen == 3

StateConstraint ==
    /\ Cardinality(messages) <= 6
    /\ \A s \in Server : Len(log[s]) <= MaxLogLen

=============================================================================
