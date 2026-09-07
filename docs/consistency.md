# AegisDB Konsistensmodell & Hållbarhetsgaranti

---

## 1. Konsistensmodell (Consistency Model)

AegisDB tillhandahåller väldefinierade konsistensgarantier för både enskilda nyckel-värde-operationer och distribuerade transaktioner:

### Nyckel-värde-nivå (Raft Group)
- **Linearizability (Stark Konsistens):**
  Alla bekräftade skrivningar och läsningar inom en enskild Raft-grupp är linjäriserbara. Det garanteras att när en skrivning har bekräftats med `SUCCESS` kommer alla efterföljande läsningar att se antingen det värdet eller ett ännu nyare värde.

### Transaktionsnivå (MVCC)
- **Snapshot Isolation (Standard):**
  Transaktioner läser från en konsekvent ögonblicksbild (snapshot) som fastställs vid transaktionsstart.
  - Läsare blockerar inte skrivare, och skrivare blockerar inte läsare.
  - Förhindrar Dirty Reads, Non-Repeatable Reads och Lost Updates.
- **Serializable Validation (Avancerat mål):**
  Validering av ReadSet och WriteSet vid commit för att upptäcka och avbryta konflikter (t.ex. Write Skew).

---

## 2. Hållbarhetsgaranti (Durability Guarantee)

> **Inget `SUCCESS`-svar skickas till klienten innan den definierade hållbarhetsgarantin är uppfylld.**

När klienten mottar ett `SUCCESS`-svar gäller:
1. Loggposten har säkrats i quorum (strikt majoritet av Raft-noder).
2. Data har persisterats till disk via Write-Ahead Log (WAL) med nödvändig `fsync`-policy.
3. Operationen överlever krascher och omstarter av upp till $\lfloor(N - 1) / 2\rfloor$ noder utan dataförlust.
