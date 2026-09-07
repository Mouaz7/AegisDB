# AegisDB Felmodell (Failure Model)

---

## 1. Antaganden som systemet hanterar

AegisDB är designat för en asynkron distribuerad miljö med felbenägna noder och opålitliga nätverk (fail-stop / crash-recovery-modell):

1. **Noder kan krascha (Crash):**
   Noder kan stängas ner abrupt vid godtycklig tidpunkt utan varning.
2. **Noder kan starta om (Crash-Recovery):**
   Noder som har kraschat kan återansluta till klustret och återhämta sitt tillstånd via persistent lagring (WAL & snapshots) samt synkronisera saknade loggposter från ledaren.
3. **Meddelanden kan fördröjas (Network Delay):**
   Nätverket garanterar inte maximal latens. Meddelanden kan anlända godtyckligt sent.
4. **Meddelanden kan dupliceras (Message Duplication):**
   Nätverksåterförsök kan leda till att samma RPC levereras mer än en gång. Alla transaktions- och replikeringsoperationer är idempotenta.
5. **Meddelanden kan tappas bort (Message Loss):**
   Paket kan förloras under transport. Transportlagret använder timeouts och automatisk återutsändning.
6. **Nätverkspartitioner (Network Partitions):**
   Nätverket kan delas i isolerade partitioner (t.ex. minoritet vs majoritet). Endast den partition som kontrollerar en strikt majoritet ($> N/2$) av noderna tillåts acceptera och committa nya skrivningar.

---

## 2. Scope-avgränsningar (Vad systemet INTE antar)

- **Inga bysantinska fel (Byzantine Faults):**
  Noder antas inte skicka medvetet felaktig data eller förfalska meddelanden.
- **Inga skadliga medlemmar (Malicious Raft Members):**
  Alla noder i klustret antas köra korrekt och icke-manipulerad AegisDB-kod.
