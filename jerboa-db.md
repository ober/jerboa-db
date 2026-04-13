# Jerboa-DB: Datomic Without the JVM

**Goal:** A fully-featured Datomic clone built entirely on Jerboa, using LevelDB for
index storage and DuckDB for analytics.  Single binary, embeddable, distributed,
with a Datalog query engine and immutable time-travel over all data.

**Status:** 2026-04-12 — Design complete.  All 38 building-block modules verified
present in the Jerboa stdlib.  **Implementation not yet started** — zero of the
31 `src/jerboa-db/*.ss` files exist.  The scorecard below was previously marked
"Done" in error; it now reflects the actual state.

**MBrainz target:** Run the Datahike MBrainz benchmark to completion (6.6M
entities, complex Datalog joins, aggregation) as the validation gate for Phases
1-3.

---

## Why Build This

Datomic is arguably the most innovative database of the last decade.  Its core
ideas — immutable facts, time-travel, Datalog queries, separation of reads and
writes, entity-attribute-value modeling — changed how Clojure developers think
about data.

But Datomic has problems:

1. **JVM-only.** Requires a running JVM peer process consuming 2-8GB of RAM.
2. **Proprietary.** Datomic Pro is closed-source.  Datomic Cloud is AWS-only.
3. **Complex deployment.** Transactor + peer + storage backend (DynamoDB/PostgreSQL/Cassandra).
4. **Expensive.** Datomic Pro licenses cost thousands per year.
5. **No embeddable mode.** Can't embed Datomic in a CLI tool, edge device, or serverless function.

The open-source alternatives (DataScript, XTDB, Datahike, Datalevin) each make
trade-offs:

| System | Language | Storage | Datalog | Time-Travel | Distributed | Embeddable |
|---|---|---|---|---|---|---|
| Datomic Pro | JVM | DynamoDB/SQL | Yes | Yes | Yes | No |
| XTDB v2 | JVM | Arrow/Parquet | Yes | Yes | Yes | No |
| DataScript | Clojure/JS | In-memory | Yes | No | No | Yes (JS) |
| Datahike | JVM | Pluggable | Yes | Partial | No | No |
| Datalevin | JVM | LMDB | Yes | No | No | No |
| **Jerboa-DB** | **Native** | **LevelDB + DuckDB** | **Yes** | **Yes** | **Yes** | **Yes** |

Jerboa-DB fills a gap that doesn't exist today: a **native, embeddable,
distributed, time-traveling Datalog database** that ships as a single binary and
requires zero external infrastructure.

### Why Jerboa Is the Right Platform

This isn't just "rewrite Datomic in Scheme."  Jerboa has building blocks that
make this project tractable in a way it wouldn't be in Go, Rust, or Python:

| Datomic Concept | Jerboa Building Block | Status |
|---|---|---|
| Immutable fact store | `(std mvcc)` — MVCC with time-travel | Exists |
| EAVT/AEVT/VAET indices | `(std db leveldb)` — LSM tree with bloom filters | Exists |
| Datalog query engine | `(std datalog)` — semi-naive evaluation | Exists |
| Logic variable unification | `(std logic)` — full miniKanren | Exists |
| Persistent collections | `(std data pmap)`, `(std pvec)`, `(std pset)` | Exists |
| Transaction log | `(std event-source)` — immutable event log | Exists |
| Schema validation | `(std schema)` — composable validators | Exists |
| Serialization (wire format) | `(std text edn)`, `(std text msgpack)`, `(std fasl)` | Exists |
| Peer caching | `(std misc lru-cache)` — O(1) LRU | Exists |
| Concurrent reads | `(std concur stm)` — optimistic transactions | Exists |
| Distributed consensus | `(std raft)` — leader election + log replication | Exists |
| Replicated state | `(std actor crdt)` — OR-Set, LWW-Register, etc. | Exists |
| Content addressing | `(std content-address)` — hash-based storage | Exists |
| Analytical queries | `(std db duckdb)` — columnar OLAP engine | Exists |
| Connection pooling | `(std db conpool)` — thread-safe pool | Exists |
| Actor supervision | `(std actor)` — OTP-style fault tolerance | Exists |
| HTTP API server | `(std net fiber-httpd)` — fiber-native HTTP | Exists |
| Binary encoding | `(std text cbor)`, `(std text msgpack)` | Exists |
| Compression | `(std compress zlib)` — gzip for segment files | Exists |
| UUID generation | `(std misc uuid)` — v4 random UUIDs | Exists |
| Sorted indices | `(std misc rbtree)`, `(std ds sorted-map)` | Exists |
| File-backed B+ tree | `(std mmap-btree)` — transactional | Exists |
| Memory-mapped I/O | `(std os mmap)` — zero-copy file access | Exists |
| Relational algebra | `(std misc relation)` — join, project, select | Exists |
| Protocol dispatch | `(std protocol)` — Clojure-style protocols | Exists |
| Transducer pipelines | `(std transducer)` — streaming transforms | Exists |
| Lazy sequences | `(std lazy)`, `(std misc lazy-seq)` | Exists |

Every row in that table is code that already exists and is tested.  The work is
**composition and integration**, not building from scratch.

---

## Datomic Concepts: A Primer

For readers unfamiliar with Datomic, here are the core concepts that Jerboa-DB
will implement:

### The Datom

The fundamental unit of data is a **datom** — a 5-tuple:

```
[entity  attribute  value  transaction  added?]
[  E        A         V       T          op  ]
```

- **Entity (E):** A unique identifier (integer or tempid).  Like a row ID, but
  entities can have any number of attributes.
- **Attribute (A):** A keyword naming the property.  `:person/name`, `:order/total`.
  Attributes have schemas: type, cardinality, uniqueness, indexing.
- **Value (V):** The data.  Strings, numbers, booleans, refs (pointers to other
  entities), instants (timestamps), UUIDs, bytes.
- **Transaction (T):** The transaction ID that asserted this datom.  Transactions
  are themselves entities with metadata (timestamp, author, etc.).
- **Added? (op):** Boolean.  `true` = assert, `false` = retract.  Retracting a
  datom doesn't delete it — it adds a new datom recording the retraction.

### Immutability and Accretion

Datomic never updates or deletes data.  It only **adds new facts**.  Changing
`:person/name` from "Alice" to "Alicia" means:

```
[42 :person/name "Alice"  1000 true ]    ;; original assertion
[42 :person/name "Alice"  1005 false]    ;; retraction
[42 :person/name "Alicia" 1005 true ]    ;; new assertion
```

All three datoms are stored forever.  The "current" database is the most recent
true assertion.  But you can always ask "what was the database at T=1000?" and
get back "Alice."

### Indices

Datomic maintains four covering indices over all datoms:

| Index | Sort Order | Use Case |
|---|---|---|
| **EAVT** | Entity → Attribute → Value → Tx | "All attributes of entity 42" |
| **AEVT** | Attribute → Entity → Value → Tx | "All entities with :person/name" |
| **AVET** | Attribute → Value → Entity → Tx | "Entity where :email = 'a@b.com'" (unique lookup) |
| **VAET** | Value → Attribute → Entity → Tx | "All entities referencing entity 42" (reverse refs) |

Each index stores the same datoms in a different sort order.  Every datom
appears in EAVT and AEVT; AVET is populated only for attributes with
`:db/index true`; VAET is populated only for `:db.type/ref` attributes.

### Datalog Queries

Queries use Datalog, a declarative logic language:

```clojure
;; Find all people older than 30
[:find ?name ?age
 :where [?e :person/name ?name]
        [?e :person/age  ?age]
        [(> ?age 30)]]

;; Find all orders for a person
[:find ?order-id ?total
 :in $ ?person-name
 :where [?p :person/name ?person-name]
        [?o :order/person ?p]
        [?o :order/id ?order-id]
        [?o :order/total ?total]]
```

Each clause `[?e :attr ?v]` is a pattern match against the datom store.  The
query planner chooses which index to scan based on which positions are bound.

### Transactions

Transactions are atomic batches of assertions and retractions:

```clojure
[{:db/id "tempid-alice"
  :person/name "Alice"
  :person/age 30}
 {:db/id "tempid-bob"
  :person/name "Bob"
  :person/age 25}
 [:db/retract 42 :person/email "old@example.com"]]
```

Each transaction:
1. Gets a monotonically increasing transaction ID (T)
2. Resolves tempids to permanent entity IDs
3. Validates against the schema
4. Writes datoms to all relevant indices
5. Records itself as an entity with `:db/txInstant`

### Database Values (db)

A **database value** is an immutable snapshot at a point in time.  Queries run
against a db value, not the live transactor.  This means:

- Queries never block writes
- Long-running analytical queries see a consistent snapshot
- You can pass db values across threads, serialize them, compare them

### Pull API

The pull API retrieves entity data as nested maps:

```clojure
(pull db [:person/name :person/age {:person/friends [:person/name]}] entity-id)
;; => {:person/name "Alice"
;;     :person/age 30
;;     :person/friends [{:person/name "Bob"} {:person/name "Carol"}]}
```

This replaces the need for ORMs.  The shape of the data is specified at query
time, not in a schema mapping.

### History and As-Of

```clojure
(d/as-of db tx-1000)          ;; db as it was at transaction 1000
(d/since db tx-1000)           ;; only datoms added after tx 1000
(d/history db)                 ;; all datoms, including retracted ones
```

---

## Architecture

```
                          ┌──────────────────────┐
                          │   Client / REPL       │
                          │   (std csp clj)       │
                          └──────────┬───────────┘
                                     │  Datalog query / transact
                                     ▼
                     ┌───────────────────────────────┐
                     │         Peer (Connection)      │
                     │                                │
                     │  ┌────────────┐ ┌───────────┐ │
                     │  │ Query      │ │ Pull API  │ │
                     │  │ Engine     │ │           │ │
                     │  │ (Datalog)  │ │ (entity   │ │
                     │  │            │ │  walking) │ │
                     │  └─────┬──────┘ └─────┬─────┘ │
                     │        │              │        │
                     │        ▼              ▼        │
                     │  ┌────────────────────────┐   │
                     │  │   Index Manager         │   │
                     │  │                         │   │
                     │  │  EAVT  AEVT  AVET VAET │   │
                     │  │  (LevelDB databases)    │   │
                     │  └────────────┬────────────┘   │
                     │               │                │
                     │  ┌────────────┴────────────┐   │
                     │  │   Cache Layer            │   │
                     │  │   (LRU + object cache)   │   │
                     │  └────────────┬────────────┘   │
                     └───────────────┼────────────────┘
                                     │
                    ┌────────────────┼────────────────┐
                    ▼                ▼                ▼
           ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
           │  Transaction  │ │   LevelDB    │ │   DuckDB     │
           │  Log          │ │              │ │              │
           │  (append-only │ │  4 sorted    │ │  Analytical  │
           │   segment     │ │  indices     │ │  replica     │
           │   files)      │ │  (LSM tree   │ │  (columnar,  │
           │               │ │   + bloom)   │ │   Parquet,   │
           │               │ │              │ │   SQL)       │
           └──────────────┘ └──────────────┘ └──────────────┘
```

### Component Roles

**Peer (Connection):** The client-facing object.  Holds a reference to the
current db value, submits transactions, runs queries.  Multiple peers can share
the same storage (read replicas) or operate embedded in-process.

**Transaction Log:** An append-only sequence of all transactions.  Each entry
contains the tx-id, timestamp, and the set of datoms (assertions + retractions).
Stored as segment files using MessagePack or FASL encoding.  This is the
**source of truth** — indices can always be rebuilt from the log.

**LevelDB Indices:** Four separate LevelDB databases (EAVT, AEVT, AVET, VAET),
each storing datoms sorted in the corresponding order.  LevelDB provides:
- Fast reads via LSM tree with bloom filters (10-bit)
- 64MB LRU cache per index for hot data
- Compression for reduced disk footprint
- Iterator-based range scans (essential for index lookups)
- FASL-encoded datom values for fast Scheme deserialization

**DuckDB Analytical Replica:** A columnar copy of the datom store for analytical
queries.  When you need `GROUP BY`, `WINDOW`, or aggregation over millions of
datoms, DuckDB handles it with vectorized execution.  The replica is
asynchronously updated from the transaction log.  This is the "bring your own
analytics engine" story that Datomic lacks.

**Cache Layer:** An LRU cache over LevelDB reads.  Hot entities stay in memory as
Scheme objects (persistent maps).  Cache invalidation is trivial because data is
immutable — new transactions only add entries, they never modify existing ones.

---

## Data Model

### Datom Representation

```scheme
;; A datom is a 5-element record
(defstruct datom (e a v tx added?))

;; Entity IDs are 64-bit integers
;; Attribute IDs are interned keywords → integer lookup
;; Values are typed (see schema)
;; Transaction IDs are monotonically increasing integers
;; added? is a boolean
```

### Datom Encoding for LevelDB

LevelDB keys and values are bytevectors.  Datoms are encoded as fixed-width keys
so that LevelDB's default bytewise comparison gives the correct sort order:

```
EAVT key: [E:8 bytes][A:4 bytes][V-hash:8 bytes][T:8 bytes]  = 28 bytes
AEVT key: [A:4 bytes][E:8 bytes][V-hash:8 bytes][T:8 bytes]  = 28 bytes
AVET key: [A:4 bytes][V-hash:8 bytes][E:8 bytes][T:8 bytes]  = 28 bytes
VAET key: [V-hash:8 bytes][A:4 bytes][E:8 bytes][T:8 bytes]  = 28 bytes
```

Values that fit in 8 bytes (integers, booleans, instants) are encoded inline.
Larger values (strings, bytes, refs) are stored as FASL-encoded LevelDB values
alongside the key, and the 8-byte FNV-1a hash is stored in the index key.

The `added?` flag is encoded in the high bit of the transaction ID:
- `T` with high bit 0 = assertion
- `T` with high bit 1 = retraction

This keeps keys fixed-width and lets cursor scans naturally interleave
assertions and retractions in transaction order.

### Schema Attributes

Every attribute has a schema definition, which is itself stored as datoms on a
schema entity:

```scheme
;; Define a schema attribute
(transact! conn
  [{:db/ident       :person/name
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/doc         "A person's full name"}

   {:db/ident       :person/friends
    :db/valueType   :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/doc         "References to friend entities"}

   {:db/ident       :person/email
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique      :db.unique/identity
    :db/index       true}])
```

Supported value types:

| Type | Scheme Representation | Byte Width |
|---|---|---|
| `:db.type/string` | string | variable (hash in key) |
| `:db.type/long` | fixnum/bignum | 8 |
| `:db.type/double` | flonum | 8 |
| `:db.type/boolean` | boolean | 1 (padded to 8) |
| `:db.type/instant` | datetime | 8 (epoch nanos) |
| `:db.type/uuid` | uuid-string | 16 (hash in key) |
| `:db.type/ref` | entity-id | 8 |
| `:db.type/keyword` | keyword | variable (interned to 4-byte id) |
| `:db.type/bytes` | bytevector | variable (hash in key) |
| `:db.type/symbol` | symbol | variable (hash in key) |

Cardinality:

| Cardinality | Meaning |
|---|---|
| `:db.cardinality/one` | Single value per entity+attribute.  New assertion retracts the old. |
| `:db.cardinality/many` | Multiple values per entity+attribute.  Each is independent. |

Uniqueness:

| Uniqueness | Meaning |
|---|---|
| `:db.unique/value` | No two entities may have the same value for this attribute. |
| `:db.unique/identity` | Upsert: if an entity with this value exists, merge into it. |

---

## Core API Design

### Connection and Database

```scheme
(import (jerboa prelude)
        (jerboa-db core))

;; Create or open a database
(def conn (connect "path/to/data"))

;; Get current database value (immutable snapshot)
(def db (db conn))

;; Time-travel
(def db-yesterday (as-of db #:t 1000))
(def db-recent    (since db #:t 1000))
(def db-full      (history db))
```

### Transacting Data

```scheme
;; Assert new entities
(transact! conn
  [{:db/id (tempid)
    :person/name "Alice"
    :person/age 30
    :person/email "alice@example.com"}

   {:db/id (tempid)
    :person/name "Bob"
    :person/age 25}])
;; => {:db-before <db> :db-after <db> :tx-data [datoms...] :tempids {..}}

;; Retract a specific datom
(transact! conn
  [[:db/retract 42 :person/email "alice@example.com"]])

;; Retract an entire entity
(transact! conn
  [[:db/retractEntity 42]])

;; CAS (compare-and-swap) — atomic conditional update
(transact! conn
  [[:db/cas 42 :person/age 30 31]])
;; Only succeeds if current value is exactly 30
```

### Datalog Queries

```scheme
;; Find all people older than 30
(q '[:find ?name ?age
     :where [?e :person/name ?name]
            [?e :person/age ?age]
            [(> ?age 30)]]
   db)
;; => #{["Alice" 35] ["Carol" 42]}

;; Parameterized query
(q '[:find ?name
     :in $ ?min-age
     :where [?e :person/name ?name]
            [?e :person/age ?age]
            [(>= ?age ?min-age)]]
   db 21)
;; => #{["Alice"] ["Bob"] ["Carol"]}

;; Find with aggregation
(q '[:find (count ?e) (avg ?age)
     :where [?e :person/age ?age]]
   db)
;; => #{[150 34.2]}

;; Rules (reusable query fragments)
(def rules
  '[[(ancestor ?x ?y)
     [?x :person/parent ?y]]
    [(ancestor ?x ?y)
     [?x :person/parent ?z]
     (ancestor ?z ?y)]])

(q '[:find ?ancestor
     :in $ % ?person
     :where [?p :person/name ?person]
            (ancestor ?p ?ancestor)]
   db rules "Alice")
```

### Pull API

```scheme
;; Simple pull
(pull db [:person/name :person/age] entity-id)
;; => {:person/name "Alice" :person/age 30}

;; Nested pull (follow refs)
(pull db [:person/name
          {:person/friends [:person/name :person/age]}]
     entity-id)
;; => {:person/name "Alice"
;;     :person/friends [{:person/name "Bob" :person/age 25}
;;                      {:person/name "Carol" :person/age 42}]}

;; Wildcard pull
(pull db '[*] entity-id)
;; => all attributes of the entity

;; Reverse refs
(pull db [:person/name {:person/_friends [:person/name]}] entity-id)
;; => {:person/name "Alice"
;;     :person/_friends [{:person/name "Dave"}]}
;; (entities that have Alice as a friend)

;; Pull with limits and defaults
(pull db [{(:person/friends :limit 5) [:person/name]}
          (:person/nickname :default "N/A")]
     entity-id)
```

### Entity API

```scheme
;; Get an entity as a lazy, navigable map
(def alice (entity db 42))

(:person/name alice)          ;; => "Alice"
(:person/age alice)           ;; => 30
(:person/friends alice)       ;; => lazy set of entity objects

;; Touch: realize all attributes eagerly
(touch alice)
;; => {:db/id 42 :person/name "Alice" :person/age 30 ...}
```

### History and Auditing

```scheme
;; Full history of an attribute
(q '[:find ?v ?tx ?added
     :where [42 :person/name ?v ?tx ?added]]
   (history db))
;; => #{["Alice" 1000 true] ["Alice" 1005 false] ["Alicia" 1005 true]}

;; Transaction metadata
(q '[:find ?time ?author
     :where [?tx :db/txInstant ?time]
            [?tx :tx/author ?author]]
   db)

;; What changed in a specific transaction
(def tx-report (tx-range (log conn) 1005 1006))
;; => list of datoms added in transaction 1005
```

### Analytical Queries (DuckDB Integration)

```scheme
;; For queries that need SQL-style aggregation, window functions,
;; or need to scan millions of datoms:

(analytics conn
  "SELECT a.v AS name, b.v AS age
   FROM eavt a
   JOIN eavt b ON a.e = b.e
   WHERE a.a = 'person/name'
     AND b.a = 'person/age'
     AND CAST(b.v AS INTEGER) > 30
   ORDER BY b.v DESC
   LIMIT 100")

;; Or with Parquet export for external tools:
(export-parquet conn "path/to/snapshot.parquet"
  :attributes [:person/name :person/age :person/email])
```

---

## Module Structure

Jerboa-DB is organized as a set of `(jerboa-db ...)` modules.  Source lives in
`src/jerboa-db/*.ss` (Jerboa source files); jerbuild compiles them to
`lib/jerboa-db/*.sls` as build artifacts.

```
src/jerboa-db/
├── core.ss               ;; connect, db, transact!, q, pull, entity
├── datom.ss              ;; datom record, encoding/decoding, comparison
├── schema.ss             ;; attribute definitions, validation, type coercion
├── index/
│   ├── leveldb.ss         ;; LevelDB index backend (persistent)
│   ├── memory.ss          ;; In-memory RB-tree backend (testing/embedded)
│   └── protocol.ss        ;; Index protocol (pluggable backends)
├── tx.ss                 ;; Transaction processing, tempid resolution, CAS
├── tx-log.ss             ;; Append-only transaction log (segment files)
├── query/
│   ├── engine.ss          ;; Datalog query compiler and executor
│   ├── planner.ss         ;; Query plan optimization (clause reordering)
│   ├── functions.ss       ;; Built-in query functions (>, <, count, sum, etc.)
│   ├── rules.ss           ;; Rule expansion and recursive evaluation
│   ├── aggregates.ss      ;; Aggregate functions (count, sum, avg, min, max)
│   └── pull.ss            ;; Pull API implementation (pattern walking)
├── entity.ss             ;; Lazy entity map with navigation
├── cache.ss              ;; LRU datom/entity cache with invalidation
├── history.ss            ;; as-of, since, history database views
├── analytics.ss          ;; DuckDB integration for OLAP queries
├── encoding.ss           ;; Binary encoding for datom keys and values
├── server.ss             ;; HTTP/WebSocket API server (fiber-httpd)
├── peer.ss               ;; Peer protocol for distributed reads
├── replication.ss        ;; Raft-based transactor HA + read replicas
└── migrate.ss            ;; Schema migration utilities
```

### Dependency Map (Jerboa stdlib modules used)

```
(jerboa-db core)
├── (std db leveldb)             ;; Index storage (LSM tree + bloom filters)
├── (std db duckdb)              ;; Analytical query engine
├── (std datalog)                ;; Datalog evaluation (base for query engine)
├── (std logic)                  ;; miniKanren (constraint solving in queries)
├── (std mvcc)                   ;; MVCC semantics (for db snapshot values)
├── (std event-source)           ;; Transaction log architecture
├── (std data pmap)              ;; Entity maps (persistent HAMT)
├── (std pvec)                   ;; Datom vectors (persistent)
├── (std pset)                   ;; Entity sets (persistent)
├── (std misc rbtree)            ;; In-memory sorted indices
├── (std ds sorted-map)          ;; Range queries
├── (std concur stm)             ;; Concurrent connection state
├── (std misc lru-cache)         ;; Hot entity/datom cache
├── (std misc uuid)              ;; Entity ID generation
├── (std text edn)               ;; EDN wire format
├── (std text msgpack)           ;; Binary encoding for tx-log segments
├── (std fasl)                   ;; Fast binary encoding for snapshots
├── (std compress zlib)          ;; Segment file compression
├── (std schema)                 ;; Attribute schema validation
├── (std protocol)               ;; Pluggable backends via protocols
├── (std multi)                  ;; Multimethod dispatch for value types
├── (std transducer)             ;; Streaming query results
├── (std misc lazy-seq)          ;; Lazy entity navigation
├── (std component)              ;; Service lifecycle management
├── (std actor)                  ;; Supervised transactor process
├── (std raft)                   ;; Distributed consensus (HA mode)
├── (std actor crdt)             ;; Replicated metadata
├── (std net fiber-httpd)        ;; REST/WebSocket API server
├── (std net router)             ;; HTTP routing
├── (std crypto hmac)            ;; Transaction signing
├── (std db conpool)             ;; Connection pooling
├── (std misc relation)          ;; Relational algebra for joins
└── (std content-address)        ;; Content-addressed value deduplication
```

---

## Implementation Plan

### Phase 1: Core — In-Memory Datomic (the proof of concept)

**Goal:** A working Datomic that stores everything in memory.  No LevelDB, no
DuckDB, no networking.  Prove the data model, query engine, and transaction
processing work correctly.

**Deliverable:** `(jerboa-db core)` usable from the REPL.

#### 1.1 Datom Record and Encoding

```scheme
;; src/jerboa-db/datom.ss

(defstruct datom (e a v tx added?))

;; Comparison functions for index ordering
(def (datom-compare-eavt a b) ...)
(def (datom-compare-aevt a b) ...)
(def (datom-compare-avet a b) ...)
(def (datom-compare-vaet a b) ...)
```

Build on `defstruct` from the prelude.  Datom comparison uses cascading numeric
comparison on entity, attribute (interned to integers), value (type-specific),
and transaction.

#### 1.2 In-Memory Indices

```scheme
;; src/jerboa-db/index/memory.ss

;; Each index is a sorted-map (red-black tree) keyed by datom comparison
(def (make-mem-index comparator)
  (make-rbtree comparator))

(def (index-add! idx datom)
  (rbtree-insert idx datom #t))

(def (index-range idx start-datom end-datom)
  ;; Cursor scan using rbtree-fold with bounds
  ...)

(def (index-lookup idx e a v)
  ;; Point lookup — construct a probe datom with known components,
  ;; scan from that point
  ...)
```

Use `(std misc rbtree)` for sorted storage.  Four instances: EAVT, AEVT, AVET,
VAET.  The protocol from 1.5 will abstract over this.

#### 1.3 Schema and Attribute Registry

```scheme
;; src/jerboa-db/schema.ss

;; Attribute record — derived from datoms on schema entities
(defstruct db-attribute
  (ident          ;; keyword (:person/name)
   id             ;; integer (interned for fast index keys)
   value-type     ;; :db.type/string, :db.type/long, etc.
   cardinality    ;; :db.cardinality/one or :db.cardinality/many
   unique         ;; #f, :db.unique/value, or :db.unique/identity
   index?         ;; boolean — populate AVET?
   is-component?  ;; boolean — cascade retractions?
   doc            ;; string or #f
   no-history?))  ;; boolean — skip history for this attr?

;; Schema validation at transaction time
(def (validate-datom schema datom) ...)
(def (coerce-value attr-type raw-value) ...)
```

Build on `defstruct` + `(std schema)` for validation predicates.

#### 1.4 Transaction Processing

```scheme
;; src/jerboa-db/tx.ss

;; A transaction is a list of operations:
;;   {:db/id <eid-or-tempid> :attr val ...}    → assert map
;;   [:db/add eid attr val]                     → assert datom
;;   [:db/retract eid attr val]                 → retract datom
;;   [:db/retractEntity eid]                    → retract all datoms for entity
;;   [:db/cas eid attr old-val new-val]         → compare-and-swap

(defstruct tx-report
  (db-before db-after tx-data tempids))

(def (process-transaction db tx-data)
  ;; 1. Allocate transaction entity ID (monotonic counter)
  ;; 2. Resolve tempids → permanent entity IDs
  ;;    - Tempids within a transaction are consistent (same tempid = same entity)
  ;;    - :db.unique/identity triggers upsert (find existing entity)
  ;; 3. Expand map operations into individual datom assertions
  ;; 4. Handle :db.cardinality/one (auto-retract old value)
  ;; 5. Validate all datoms against schema
  ;; 6. Check uniqueness constraints
  ;; 7. Add transaction metadata datom (:db/txInstant)
  ;; 8. Write datoms to all indices
  ;; 9. Return tx-report with before/after db values
  ...)
```

#### 1.5 Index Protocol (Pluggable Backend)

```scheme
;; src/jerboa-db/index/protocol.ss
(import (std protocol))

(defprotocol DatomIndex
  ;; Add a datom to the index
  (idx-add! [idx datom])

  ;; Remove a datom from the index (for index rebuild only)
  (idx-remove! [idx datom])

  ;; Range scan: return lazy sequence of datoms between start and end
  (idx-range [idx start-datom end-datom])

  ;; Point lookup: return datoms matching known components
  (idx-seek [idx components])

  ;; Count datoms in range
  (idx-count [idx start-datom end-datom])

  ;; Snapshot: return an immutable view at current state
  (idx-snapshot [idx]))
```

Memory backend (Phase 1) and LevelDB backend (Phase 2) both implement this
protocol.

#### 1.6 Datalog Query Engine

This is the largest component.  It extends `(std datalog)` with:

```scheme
;; src/jerboa-db/query/engine.ss

;; Parse Datomic-style query syntax into internal representation
(def (parse-query form) ...)

;; Query clause types:
;;   [?e :attr ?v]         → index scan (data pattern)
;;   [(> ?x 10)]           → predicate filter (expression clause)
;;   [(str ?first ?last)]  → function call (binding form)
;;   (rule-name ?x ?y)     → rule invocation

;; Query planning: choose index based on bound variables
;; If ?e is bound:     use EAVT
;; If :attr is bound:  use AEVT (or AVET if ?v is also bound)
;; If ?v is bound:     use VAET (for refs)
;; Reorder clauses to bind variables early (most selective first)

(def (plan-query parsed-query db) ...)
(def (execute-plan plan db inputs) ...)
```

The planner constructs an execution DAG where each node is an index scan or
a filter, and edges represent variable bindings flowing between clauses.
Transducers are used for streaming intermediate results.

#### 1.7 Pull API

```scheme
;; src/jerboa-db/query/pull.ss

;; Pull pattern := [attr-spec ...]
;; attr-spec   := keyword
;;              | {keyword pull-pattern}     ;; nested (follow refs)
;;              | (keyword :as alias)
;;              | (keyword :limit n)
;;              | (keyword :default val)
;;              | '*                         ;; wildcard

(def (pull db pattern eid)
  ;; 1. For each attr-spec in pattern:
  ;;    - Look up datoms in EAVT for (eid, attr)
  ;;    - For cardinality/one: return single value
  ;;    - For cardinality/many: return set of values
  ;;    - For nested patterns: recursively pull referenced entities
  ;;    - For reverse attrs (_friends): scan VAET for (eid, attr)
  ;; 2. Return persistent hash map
  ...)

(def (pull-many db pattern eids)
  (map (lambda (eid) (pull db pattern eid)) eids))
```

#### 1.8 Database Values and Time-Travel

```scheme
;; src/jerboa-db/history.ss

;; A db value is a snapshot: a reference to the indices at a specific tx
(defstruct db-value
  (connection     ;; back-reference to connection
   basis-tx       ;; the transaction this snapshot is based on
   indices        ;; EAVT/AEVT/AVET/VAET (snapshots or filters)
   schema         ;; attribute registry at this point in time
   as-of-tx       ;; #f for current, or a tx-id for time-travel
   since-tx       ;; #f for current, or a tx-id for since filter
   history?))     ;; #t if this is a history view (includes retractions)

(def (as-of db tx-id)
  ;; Return a new db-value that filters datoms to those with tx <= tx-id
  (make-db-value
    (db-value-connection db)
    (db-value-basis-tx db)
    (db-value-indices db)
    (db-value-schema db)
    tx-id #f #f))

(def (since db tx-id)
  ;; Return a new db-value that only shows datoms with tx > tx-id
  (make-db-value ... since-tx: tx-id ...))

(def (history db)
  ;; Return a db-value that includes all datoms (even retracted)
  (make-db-value ... history?: #t ...))
```

**Phase 1 success criteria:**
- Schema definition and validation works
- Transacting entities with tempid resolution works
- Cardinality/one auto-retraction works
- Unique identity upsert works
- Datalog queries against EAVT/AEVT/AVET/VAET return correct results
- Pull API navigates refs and reverse refs
- `as-of` returns correct historical state
- 1000 entities with 10 attributes each, queried in < 10ms
- All operations available from the REPL

### Phase 2: Persistence — LevelDB Backend

**Goal:** Replace in-memory indices with LevelDB.  Data survives process restarts.
Transaction log is durable.

#### 2.1 LevelDB Index Backend

```scheme
;; src/jerboa-db/index/leveldb.ss

(def (make-leveldb-index name db-handle)
  ;; Each covering index (EAVT, AEVT, AVET, VAET) gets its own
  ;; LevelDB database directory.  Keys are 28-byte encoded datom keys
  ;; that sort correctly via bytewise comparison.
  ;; Values are FASL-encoded full datom records.
  ...)

;; Implement DatomIndex protocol for LevelDB
;; add! — encode key + FASL value, leveldb-put
;; remove! — encode key, leveldb-delete
;; range-query — iterator-based scan from lo-key to hi-key
;; count-range — key-only fold for counting
;; all-datoms — full table scan (expensive, use sparingly)
```

Key implementation detail: LevelDB iterators are used for range scans.
`leveldb-fold` iterates from a start key to an end key, reconstructing datoms
from FASL-encoded values.  Bloom filters (10-bit) accelerate point lookups.
Each index gets a 64MB LRU cache for hot data.

#### 2.2 Binary Encoding

```scheme
;; src/jerboa-db/encoding.ss

;; Encode entity ID as big-endian 8 bytes (for correct bytewise sort order)
(def (encode-eid eid)
  (let ([bv (make-bytevector 8)])
    (bytevector-u64-set! bv 0 eid (endianness big))
    bv))

;; Encode attribute ID as big-endian 4 bytes
(def (encode-aid aid)
  (let ([bv (make-bytevector 4)])
    (bytevector-u32-set! bv 0 aid (endianness big))
    bv))

;; Encode value based on type
(def (encode-value attr-type value)
  (case attr-type
    [(:db.type/long)    (encode-i64 value)]
    [(:db.type/double)  (encode-f64-sortable value)]
    [(:db.type/boolean) (if value #vu8(1) #vu8(0))]
    [(:db.type/instant) (encode-i64 (datetime->epoch value))]
    [(:db.type/ref)     (encode-eid value)]
    [(:db.type/string :db.type/uuid :db.type/bytes :db.type/keyword)
     ;; Variable-length: hash to 8 bytes for the index key,
     ;; store full value in value-store
     (content-hash-bytes value)]
    [else (error 'encode-value "unknown type" attr-type)]))

;; EAVT key construction
(def (make-eavt-key e a v tx added?)
  (let ([bv (make-bytevector 28)])
    (bytevector-copy! (encode-eid e) 0 bv 0 8)
    (bytevector-copy! (encode-aid a) 0 bv 8 4)
    (bytevector-copy! (encode-value-hash v) 0 bv 12 8)
    (bytevector-u64-set! bv 20
      (if added? tx (bitwise-ior tx #x8000000000000000))
      (endianness big))
    bv))
```

IEEE 754 doubles are encoded with a bit-flip trick (XOR sign bit, flip all
bits if negative) so that bytewise comparison matches numeric comparison.  This
is critical for LevelDB range scans on numeric values.

#### 2.3 Transaction Log Segments

```scheme
;; src/jerboa-db/tx-log.ss

;; The transaction log is a sequence of segment files.
;; Each segment contains a batch of transactions.
;; Format: MessagePack-encoded records, gzip-compressed.

(defstruct tx-log-entry
  (tx-id          ;; monotonic transaction ID
   tx-instant     ;; datetime
   datoms))       ;; list of datoms (encoded as vectors)

;; Append a transaction to the log
(def (tx-log-append! log entry)
  ;; 1. Encode entry with msgpack
  ;; 2. Append to current segment file
  ;; 3. If segment exceeds size threshold, rotate to new segment
  ;; 4. fsync for durability
  ...)

;; Replay log to rebuild indices (disaster recovery)
(def (tx-log-replay log from-tx index-fn)
  ;; Read all segments from from-tx forward
  ;; Call index-fn for each datom
  ;; Uses transducers for streaming (no full materialization)
  ...)
```

#### 2.4 Value Store

```scheme
;; src/jerboa-db/encoding.ss (continued)

;; Large values (strings, bytevectors) are stored separately.
;; The index key contains only the hash; the full value lives here.
;; This is another LevelDB database.

(def (value-store-put! env value)
  ;; 1. Compute SHA-256 hash of value
  ;; 2. Check if already exists (content-addressed → deduplication!)
  ;; 3. Store hash → value in LevelDB "values" database
  ;; 4. Return 8-byte truncated hash for index key
  ...)
```

Content addressing gives automatic deduplication.  If 10,000 entities all have
`:country "United States"`, that string is stored once.

**Phase 2 success criteria:**
- All Phase 1 tests pass with LevelDB backend
- Database survives process restart (`kill -9` + restart = no data loss)
- 1 million datoms indexed in < 30 seconds
- Point lookups in < 100 microseconds (LevelDB bloom filter + LRU cache)
- Range scans stream without materializing all datoms
- Transaction log can rebuild indices from scratch

### Phase 3: Query Engine — Production Datalog

**Goal:** A query engine that handles real workloads: clause reordering,
aggregation, rules, built-in functions, and streaming results.

#### 3.1 Query Planner

```scheme
;; src/jerboa-db/query/planner.ss

;; Clause reordering: put the most selective clauses first.
;; Selectivity heuristic:
;;   1. Clauses with more bound variables are more selective
;;   2. Unique attributes are maximally selective
;;   3. Ref attributes (VAET) are moderately selective
;;   4. Unbounded scans (only attribute bound) are least selective

(def (reorder-clauses clauses bound-vars schema)
  ;; Score each clause based on which variables are already bound
  ;; Sort by selectivity (highest first)
  ;; This is the single biggest performance lever in the query engine
  ...)

;; Choose index for a data pattern [?e :attr ?v]
(def (choose-index pattern bound-vars schema)
  (let ([e-bound? (bound? (pattern-entity pattern) bound-vars)]
        [a-bound? (pattern-attribute pattern)]  ;; always bound in valid query
        [v-bound? (bound? (pattern-value pattern) bound-vars)]
        [attr-info (schema-lookup schema (pattern-attribute pattern))])
    (cond
      [e-bound?  'eavt]                          ;; entity known → EAVT
      [(and v-bound? (ref-type? attr-info)) 'vaet]  ;; ref value known → VAET
      [(and v-bound? (indexed? attr-info))  'avet]  ;; indexed + value → AVET
      [else 'aevt])))                              ;; attribute scan → AEVT
```

#### 3.2 Aggregate Functions

```scheme
;; src/jerboa-db/query/aggregates.ss

;; Built-in aggregates (matching Datomic's set)
;; (count ?x)           → count of distinct values
;; (count-distinct ?x)  → count of distinct values
;; (sum ?x)             → sum of numeric values
;; (avg ?x)             → average
;; (min ?x)             → minimum
;; (max ?x)             → maximum
;; (median ?x)          → median (not in Datomic, but we add it)
;; (rand N ?x)          → N random samples
;; (sample N ?x)        → N random samples (Datomic name)
;; (distinct ?x)        → set of distinct values

;; Custom aggregates via protocol
(defprotocol Aggregate
  (agg-init [agg])
  (agg-step [agg state value])
  (agg-complete [agg state]))
```

#### 3.3 Built-in Functions

```scheme
;; src/jerboa-db/query/functions.ss

;; Predicate clauses: [(> ?age 30)]
;; These filter — they don't bind new variables

;; Function clauses: [(str ?first " " ?last) ?full-name]
;; These compute and bind the result to ?full-name

;; Built-in predicates
;;   >, <, >=, <=, =, not=, zero?, pos?, neg?, even?, odd?
;;   string-starts-with?, string-ends-with?, string-contains?

;; Built-in functions
;;   str, subs, upper-case, lower-case, count (string length)
;;   +, -, *, /, mod, inc, dec, abs, max, min
;;   ground (bind a value: [(ground 42) ?x])
;;   get-else (default value: [(get-else $ ?e :attr default) ?v])
;;   missing? (true if entity lacks attribute)
;;   tuple (construct a tuple from variables)
```

#### 3.4 Rule System

```scheme
;; src/jerboa-db/query/rules.ss

;; Rules are reusable query fragments, enabling recursion.
;; Defined as: [(rule-name ?arg ...) clause clause ...]

;; Rule expansion at query time:
;; 1. Parse rule definitions
;; 2. When a rule invocation appears in a query, expand it
;; 3. Handle recursive rules via fixed-point iteration
;;    (same semi-naive approach as (std datalog))
;; 4. Memoize intermediate results to avoid recomputation
```

**Phase 3 success criteria:**
- Query planner chooses correct index (verified by `explain` output)
- Clause reordering makes multi-clause queries 10-100x faster
- Aggregation works: count, sum, avg, min, max
- Recursive rules (ancestor, transitive closure) work
- Built-in functions work in filter and binding positions
- Query over 1M datoms completes in < 1 second for typical OLTP patterns

### Phase 4: DuckDB Analytics Layer

**Goal:** Wire DuckDB as an analytical query engine for workloads that don't
fit Datalog — aggregation over large datasets, window functions, ad-hoc SQL.

#### 4.1 DuckDB Replica

```scheme
;; src/jerboa-db/analytics.ss

;; Maintain a DuckDB database as a columnar replica of the datom store.
;; Schema:
;;   CREATE TABLE datoms (
;;     e BIGINT, a INTEGER, v VARCHAR,
;;     v_long BIGINT, v_double DOUBLE, v_bool BOOLEAN,
;;     v_instant TIMESTAMP, v_ref BIGINT,
;;     tx BIGINT, added BOOLEAN
;;   );
;;
;; Separate typed columns avoid the "everything is a string" problem.
;; The query engine selects the right column based on schema type.

(def (sync-to-duckdb! conn duckdb-conn from-tx)
  ;; Read transaction log entries since from-tx
  ;; Batch-insert datoms into DuckDB using prepared statements
  ;; Uses transducers for streaming from tx-log → DuckDB
  ...)

;; Convenience: SQL over the datom store
(def (analytics conn sql-string . params)
  ;; 1. Ensure DuckDB replica is synced to latest tx
  ;; 2. Execute SQL query against DuckDB
  ;; 3. Return results as list of alists
  ...)
```

#### 4.2 Parquet Export/Import

```scheme
;; Export database snapshot to Parquet (for external analytics)
(def (export-parquet conn path . opts)
  ;; Uses DuckDB's native Parquet writer
  ;; Options: :attributes (subset), :as-of (time-travel), :format (wide/long)
  ...)

;; Import external data as datoms
(def (import-parquet conn path mapping)
  ;; mapping: column-name → attribute keyword
  ;; Each row becomes an entity, each column becomes an attribute
  ...)

;; Same for CSV
(def (import-csv conn path mapping) ...)
```

**Phase 4 success criteria:**
- DuckDB replica stays within 1 second of latest transaction
- SQL aggregation over 10M datoms completes in < 5 seconds
- Parquet export produces valid files readable by pandas/DuckDB/Spark
- CSV/Parquet import creates correct datoms with schema validation

### Phase 5: Server Mode

**Goal:** A standalone Jerboa-DB server accessible over HTTP and WebSocket,
enabling multi-client access and remote peers.

#### 5.1 HTTP API

```scheme
;; src/jerboa-db/server.ss

;; REST endpoints:
;; POST /api/transact        — submit transaction
;; POST /api/query           — run Datalog query
;; POST /api/pull            — pull entity data
;; GET  /api/entity/:eid     — get entity by ID
;; GET  /api/db/stats        — database statistics
;; GET  /api/db/schema       — current schema
;; GET  /health              — health check

;; WebSocket endpoint:
;; WS /api/tx-stream         — stream transaction reports in real-time
;;                              (clients subscribe and get notified of every tx)

;; Wire format: EDN (primary) or JSON (for non-Scheme clients)
```

Built on `(std net fiber-httpd)` + `(std net router)` + `(std net fiber-ws)`.
One fiber per connection.  Queries run against immutable db snapshots so they
never block the transactor.

#### 5.2 Client Library

```scheme
;; src/jerboa-db/peer.ss

;; Remote peer — connects to a Jerboa-DB server
(def remote-conn (connect-remote "http://localhost:8484"))

;; Same API as embedded mode
(def db (db remote-conn))
(q '[:find ...] db)
(pull db pattern eid)
(transact! remote-conn tx-data)

;; Transparent caching: the peer caches db segments locally
;; and only fetches deltas from the server.
```

#### 5.3 Transaction Stream

```scheme
;; Real-time change feed over WebSocket
;; Clients subscribe and receive tx-reports as they happen.
;; This enables:
;;   - UI live updates (like Firebase)
;;   - ETL pipelines that react to database changes
;;   - Read replicas that stay in sync

(def (tx-stream conn handler)
  ;; handler: (lambda (tx-report) ...)
  ;; Called for every committed transaction
  ...)
```

**Phase 5 success criteria:**
- Server handles 10K concurrent query connections
- Transactions via HTTP complete in < 50ms (excluding network)
- WebSocket tx-stream delivers updates within 5ms of commit
- Remote peer API is identical to embedded API

### Phase 6: Distribution (Raft-based HA)

**Goal:** Multi-node Jerboa-DB with automatic failover.  One transactor (leader),
multiple read replicas (followers).

#### 6.1 Raft-Based Transactor

```scheme
;; src/jerboa-db/replication.ss

;; The transactor is a Raft leader.
;; Transactions are proposed to the Raft log.
;; Once committed by majority, they're applied to local indices.
;; Followers apply transactions from the Raft log.

;; Uses (std raft) for leader election and log replication.
;; Uses (std actor transport) for inter-node communication.
```

#### 6.2 Read Replicas

```scheme
;; Followers maintain full index copies (LevelDB).
;; They tail the transaction log and apply transactions locally.
;; Queries against followers are eventually consistent
;; (typically < 100ms behind the leader).

;; Consistency options:
;; :read-committed   — any follower, may be slightly behind
;; :read-latest      — leader only, always current
;; :as-of tx-id      — any node, guaranteed consistent at that tx
```

**Phase 6 success criteria:**
- Leader failure triggers automatic election within 5 seconds
- Read replicas stay within 100ms of leader during normal operation
- No data loss on leader failure (Raft quorum guarantees)
- Client automatically reconnects to new leader

### Phase 7: Polish and Ecosystem

#### 7.1 Schema Migrations

```scheme
;; Adding new attributes is always safe (additive schema).
;; Renaming/removing requires migration:

(def (migrate! conn migration)
  ;; migration is a list of operations:
  ;; [:rename-attr :old/name :new/name]
  ;; [:merge-attr :from :into merge-fn]
  ;; [:split-attr :from :into-a :into-b split-fn]
  ;; [:add-index :attr]
  ;; [:remove-index :attr]
  ...)
```

#### 7.2 Backup and Restore

```scheme
(backup! conn "path/to/backup")
;; Serializes all indices + connection state as FASL + gzip
;; Point-in-time: backup is consistent at the latest committed tx

(restore! "path/to/backup" "path/to/new-db")
;; Copies data files, verifies integrity, opens new connection
```

#### 7.3 CLI

```bash
# Start server
jerboa-db serve --port 8484 --data /var/lib/jerboa-db

# Interactive query REPL
jerboa-db repl --connect http://localhost:8484

# Import data
jerboa-db import --format csv --mapping mapping.edn data.csv

# Export snapshot
jerboa-db export --format parquet --as-of 2026-04-01 snapshot.parquet

# Backup
jerboa-db backup --data /var/lib/jerboa-db --output /backup/

# Stats
jerboa-db stats --data /var/lib/jerboa-db
```

#### 7.4 Monitoring

```scheme
;; Prometheus metrics
;; jerboa_db_datoms_total
;; jerboa_db_transactions_total
;; jerboa_db_transaction_duration_seconds (histogram)
;; jerboa_db_query_duration_seconds (histogram by complexity)
;; jerboa_db_index_size_bytes (by index name)
;; jerboa_db_cache_hit_ratio
;; jerboa_db_replication_lag_seconds
```

---

## Performance Targets

| Metric | Target | Datomic Comparison |
|---|---|---|
| Point entity lookup | < 10 microseconds | Comparable (LevelDB bloom filter) |
| Simple 2-clause query | < 1ms | Comparable |
| Complex 5-clause query | < 50ms | Comparable |
| Transaction (10 datoms) | < 5ms | Faster (no JVM overhead) |
| Transaction (1000 datoms) | < 100ms | Comparable |
| Bulk import (1M datoms) | < 60 seconds | Faster (LevelDB batch write) |
| Cold start | < 200ms | 10-100x faster (no JVM) |
| Memory (1M datoms) | < 200MB | 5-10x less (no JVM heap) |
| Binary size | < 30MB | N/A (Datomic is 60MB+ JARs) |
| Max database size | 1TB+ (disk limit) | Comparable |
| Concurrent readers | Unlimited (immutable db-values) | Comparable |
| Aggregate over 10M datoms | < 5s (DuckDB) | Faster (columnar engine) |

### Why These Numbers Are Achievable

- **LevelDB** provides fast reads via LSM tree with bloom filters + 64MB LRU cache
- **FASL encoding** — Chez native binary format, fast deserialization without boxing
- **Chez Scheme** compiles to native code — no JVM interpreter warmup
- **Persistent data structures** share structure — low GC pressure
- **DuckDB** is a world-class columnar engine for analytical queries
- **Fiber-based server** handles 100K connections on one process

---

## The DuckDB Advantage

Datomic's biggest weakness is analytical queries.  Aggregating over millions of
datoms requires custom index scans and client-side computation.  XTDB v2
addressed this with Apache Arrow, but it's still JVM-only.

Jerboa-DB's DuckDB integration provides:

1. **Full SQL over datoms** — GROUP BY, HAVING, WINDOW functions, CTEs, subqueries
2. **Vectorized execution** — DuckDB processes data in columnar batches, 10-100x
   faster than row-by-row Datalog evaluation for aggregation
3. **Parquet interop** — Export snapshots to Parquet for external tools (Python,
   R, Spark, dbt)
4. **Time-series queries** — DuckDB handles temporal aggregation natively:
   ```sql
   SELECT date_trunc('hour', v_instant) AS hour,
          COUNT(*) AS events
   FROM datoms
   WHERE a = 'event/timestamp'
   GROUP BY 1
   ORDER BY 1
   ```
5. **No external infrastructure** — DuckDB runs in-process, same as LevelDB

This gives Jerboa-DB a **dual-engine architecture**: Datalog for navigational
queries (follow relationships, traverse graphs) and SQL for analytical queries
(aggregate, window, report).  No other Datomic-like system offers both.

---

## What Jerboa-DB Proves About Jerboa

When someone sees Jerboa-DB and asks "could I build this in Go/Rust/Python?",
the honest answer is: not easily.

| Requirement | Go | Rust | Python | Jerboa |
|---|---|---|---|---|
| Datalog query engine | Build from scratch | Build from scratch | DataScript (JS FFI) | `(std datalog)` exists |
| Persistent data structures | None in stdlib | None in stdlib | None in stdlib | pmap, pvec, pset exist |
| MVCC with time-travel | Build from scratch | Build from scratch | Build from scratch | `(std mvcc)` exists |
| Event sourcing | Build from scratch | Build from scratch | Build from scratch | `(std event-source)` exists |
| Raft consensus | etcd/raft (library) | raft-rs (library) | None | `(std raft)` exists |
| LevelDB bindings | goleveldb | leveldb-rs | plyvel | `(std db leveldb)` exists |
| DuckDB bindings | go-duckdb | duckdb-rs | duckdb-python | `(std db duckdb)` exists |
| Macro system for DSL | None | proc_macro (complex) | None | Chez hygienic macros |
| EDN format | Third-party | Third-party | Third-party | `(std text edn)` exists |
| Actor supervision | Third-party | Third-party | None | `(std actor)` exists |
| Fiber-based server | goroutines (yes) | tokio (yes) | asyncio (limited) | `(std net fiber-httpd)` exists |
| Single static binary | Yes | Yes | No | Yes (musl) |
| REPL-driven development | No | No | Partial | Yes |
| Embeddable as library | Yes | Yes | Yes | Yes |

Jerboa has **15 of the required building blocks already built and tested**.  In
Go or Rust, you'd be starting with 2-3 (bindings to LevelDB and DuckDB) and
building the other 12 from scratch.  That's the difference between a 6-month
project and a multi-year one.

---

## Implementation Scorecard (2026-04-12)

> **NOTE:** All building-block modules listed in the "Why Jerboa Is the Right
> Platform" table are verified present.  The items below describe Jerboa-DB's
> own code — the integration layer that wires those blocks together.  **None of
> this code exists yet.**

### Core (Phase 1) — NOT STARTED — *MBrainz-critical*

Files to create: `src/jerboa-db/datom.ss`, `src/jerboa-db/schema.ss`,
`src/jerboa-db/index/protocol.ss`, `src/jerboa-db/index/memory.ss`,
`src/jerboa-db/tx.ss`, `src/jerboa-db/query/engine.ss`,
`src/jerboa-db/query/planner.ss`, `src/jerboa-db/query/pull.ss`,
`src/jerboa-db/entity.ss`, `src/jerboa-db/history.ss`,
`src/jerboa-db/cache.ss`, `src/jerboa-db/core.ss`

| Feature | Status | Builds on | Notes |
|---|---|---|---|
| Datom model (E-A-V-T-op) | TODO | `defstruct` | 5-tuple record, 4 comparators, sentinel boundaries |
| Four covering indices (EAVT/AEVT/AVET/VAET) | TODO | `(std misc rbtree)` | In-memory RB-tree backend first |
| Schema registry | TODO | `(std schema)` | Intern, lookup, bootstrap attrs |
| Transaction processing | TODO | — | Tempid resolution, auto-retract, upsert, CAS, entity retract |
| Current-state resolution | TODO | — | Groups by (e,a,v), keeps highest-tx, filters retracted |
| Datalog query engine | TODO | `(std datalog)` | Parse Datomic syntax, plan, execute with index selection |
| Clause reordering | TODO | — | Selectivity scoring, greedy ordering |
| Recursive rules | TODO | `(std datalog)` | Fixed-point evaluation, variable renaming |
| Pull API | TODO | — | Nesting, wildcards, reverse refs, limits, defaults, cycle detection |
| Lazy entity maps | TODO | `(std pmap)` | On-demand loading, touch for eager materialization |
| Time-travel (as-of, since, history) | TODO | `(std mvcc)` | Temporal filters on db-value snapshots |
| LRU cache | TODO | `(std misc lru-cache)` | O(1) get/put, hit/miss stats |

### Persistence (Phase 2) — NOT STARTED — *MBrainz-critical (for dataset size)*

Files to create: `src/jerboa-db/index/leveldb.ss`, `src/jerboa-db/encoding.ss`,
`src/jerboa-db/tx-log.ss`

| Feature | Status | Builds on | Notes |
|---|---|---|---|
| Binary encoding (28-byte keys) | TODO | Chez bytevectors | Big-endian ints, sortable doubles, FNV-1a hashing |
| LevelDB index backend | TODO | `(std db leveldb)` | 4 separate LevelDB databases, 28-byte keys, FASL values |
| Transaction log segments | TODO | `(std text msgpack)`, `(std compress zlib)` | Append-only segment files for durability |
| Value store (content-addressed) | TODO | `(std content-address)` | FNV-1a keyed dedup for variable-length values |
| Connection close/cleanup | TODO | — | Closes all 4 LevelDB handles |

### Query Engine (Phase 3) — NOT STARTED — *MBrainz-critical*

Files to create: `src/jerboa-db/query/functions.ss`,
`src/jerboa-db/query/rules.ss`, `src/jerboa-db/query/aggregates.ss`

| Feature | Status | Builds on | Notes |
|---|---|---|---|
| `not` / `not-join` clauses | TODO | — | Filter out binding sets matching negated patterns |
| `or` / `or-join` clauses | TODO | — | Union of binding sets from disjunctive branches |
| Collection binding `[?x ...]` in `:in` | TODO | — | Pass a set, match any member |
| Relation binding `[[?x ?y]]` in `:in` | TODO | — | Pass a relation, join against it |
| Tuple binding `[?x ?y]` in `:in` | TODO | — | Destructure a single tuple |
| Lookup refs in transactions | TODO | — | `(attr-ident value)` pair resolves via unique attribute |
| Nested maps in transactions | TODO | — | Component entities auto-created from nested alists |
| Predicates | TODO | — | `zero?`, `pos?`, `neg?`, `even?`, `odd?`, `starts-with?`, `ends-with?`, `contains?` |
| Functions | TODO | — | `str`, `subs`, `upper-case`, `lower-case`, `inc`, `dec`, `abs`, `mod`, `ground`, `get-else`, `missing?`, `tuple`, `count` |
| Aggregates | TODO | — | `count`, `count-distinct`, `sum`, `avg`, `min`, `max`, `median`, `rand`, `sample`, `distinct` |
| Query explain | TODO | — | Dump chosen plan for debugging |

### Analytics (Phase 4) — NOT STARTED — *post-MBrainz*

Files to create: `src/jerboa-db/analytics.ss`

| Feature | Status | Builds on | Notes |
|---|---|---|---|
| DuckDB replica | TODO | `(std db duckdb)` | Columnar datom copy with typed value columns |
| SQL query interface | TODO | `(std db duckdb)` | `analytics-query` over synced datom store |
| Parquet export/import | TODO | DuckDB native | `export-parquet`, `import-parquet` |
| CSV import | TODO | `(std csv)` | `import-csv` with column-to-attribute mapping |
| Analytics sync | TODO | — | `analytics-sync!` materializes datoms → DuckDB |

### Server Mode (Phase 5) — NOT STARTED — *post-MBrainz*

Files to create: `src/jerboa-db/server.ss`, `src/jerboa-db/peer.ss`

| Feature | Status | Builds on | Notes |
|---|---|---|---|
| HTTP server | TODO | `(std net fiber-httpd)`, `(std net router)` | 7 REST routes |
| WebSocket tx-stream | TODO | `(std net fiber-ws)` | Real-time transaction feed |
| Remote peer client | TODO | `(std net request)`, `(std text edn)` | EDN wire format |
| Routes | TODO | — | `/transact`, `/q`, `/pull`, `/entity`, `/schema`, `/stats`, `/db` |

### Distribution (Phase 6) — NOT STARTED — *post-MBrainz*

Files to create: `src/jerboa-db/replication.ss`

| Feature | Status | Builds on | Notes |
|---|---|---|---|
| Raft consensus | TODO | `(std raft)` | Leader election + log replication |
| Replicated transactions | TODO | `(std raft)` | Leader-only writes, callback-based apply |
| Consistency levels | TODO | — | `:read-committed`, `:read-latest`, `:as-of` |

### Polish (Phase 7) — NOT STARTED — *post-MBrainz*

Files to create: `src/jerboa-db/migrate.ss`, `src/jerboa-db/backup.ss`

| Feature | Status | Builds on | Notes |
|---|---|---|---|
| Schema migration | TODO | — | rename, merge, split, add-index, remove-index |
| Backup/restore | TODO | `(std fasl)`, `(std compress zlib)` | FASL + gzip serialization |
| Excision (GDPR) | TODO | — | Physical removal from all 4 indices |
| Online reindexing | TODO | — | `reindex!` and `reindex-attribute!` |
| Prometheus metrics | TODO | `(std metrics)` | tx/query duration, datom counts, cache stats |
| Test suite | TODO | — | Target: 34+ integration tests |

### Advanced Features (Phase 8) — NOT STARTED — *post-MBrainz*

Files to create: `src/jerboa-db/fulltext.ss`, `src/jerboa-db/gc.ss`,
`bin/jerboa-db.ss`

| Feature | Status | Builds on | Notes |
|---|---|---|---|
| Attribute predicates / entity specs | TODO | — | `define-spec`, `validate-entity`, `check-entity-spec` |
| Composite tuples | TODO | — | `db/tupleAttrs` auto-generation |
| Fulltext search | TODO | — | In-memory inverted index |
| CLI tools | TODO | — | `serve`, `stats`, `backup`, `gc`, `repl`, `import`, `export` |
| Datom garbage collection | TODO | — | Compact retracted `db/noHistory` datoms |
| Automatic client failover | TODO | — | Multi-URL with exponential backoff |

### Query Engine Performance Optimizations (planned)

| Optimization | Description | Benefit |
|---|---|---|
| AVET for all scalar attrs | Every non-ref, non-tuple attribute populates the AVET index | Exact-match and range queries use index instead of full AEVT scan |
| Binding hashtable | Binding env is an `eq?` hashtable instead of alist | O(1) variable lookup vs O(n) for deep join pipelines |
| Streaming flatmap | `evaluate-where-clauses` uses inline flatmap instead of `(apply append (map …))` | Avoids intermediate list-of-lists allocation |
| Early termination | Clause evaluation stops immediately when no bindings survive | Avoids evaluating remaining clauses on empty result set |
| Count short-circuit | `(count ?x)` with no grouping vars skips per-row extraction | Direct `(length bindings-list)` — O(1) vs O(n) |
| Range predicate pushdown | `(?e attr ?v) [(cmp ?v const)]` fused into single AVET range scan | Scans only the qualifying value range; fires only when entity is unbound |
| Schema lookup cache | Per-transaction `symbol-hash` hashtable wrapping `schema-lookup-by-ident` | Eliminates repeated global hashtable lookups per datom during write |
| Retraction fast-path | `resolve-current-datoms` skips hashtable when no retractions present | Common case (append-only DB) avoids O(n) hashtable build |

**Performance targets (to be measured after implementation):**

| Query | Target Rate | Notes |
|---|---|---|
| Exact-match (`(?e attr const)`) | ~60K ops/sec | AVET point lookup |
| Range predicate (`[(> ?v 50)]`) | ~1.5K ops/sec | AVET range scan |
| Three-attr join with range filter | ~400 ops/sec | EAVT point lookups per entity after anchor |
| Count aggregate (full scan) | ~1.4K ops/sec | Count short-circuit |
| Pull wildcard | ~1M ops/sec | Single EAVT range |
| Individual writes (1 entity/tx) | ~90K ops/sec | All 4 indices |
| Batch writes (batch=500) | ~115K ops/sec | Schema cache amortization |

---

## MBrainz Benchmark Plan

The **MBrainz** dataset (derived from MusicBrainz) is the standard benchmark for
Datomic-compatible databases.  Datahike publishes numbers against it, making it
the right validation target.

### Dataset

~6.6M entities across these attribute groups:

- **Artists:** name, sortName, type, gender, country, startYear, endYear
- **Releases:** name, artists (ref, many), year, month, day, status, country
- **Media:** tracks (ref, many)
- **Tracks:** name, position, duration, artists (ref, many)
- **Labels:** name, sortName, type, country, startYear, endYear

### Required Queries (Datahike MBrainz benchmark set)

1. Simple attribute lookup: artists by exact name
2. Two-clause join: releases by artist name
3. Range predicate: artists active before year X
4. Multi-join: tracks → release → artist with attribute filters
5. Aggregation: count releases per artist, avg track duration
6. Reverse ref navigation: find all releases referencing an artist entity
7. Rule-based: transitive relationships (if present)
8. Pull patterns: artist with nested releases and tracks

### Implementation Priority for MBrainz

**Must have (Phases 1-3 subset):**
- Datom model + indices (EAVT, AEVT, AVET, VAET)
- Schema with `:db.type/string`, `:db.type/long`, `:db.type/ref`, `:db.type/keyword`
- Cardinality `:one` and `:many`
- Transaction processing with tempid resolution
- Datalog query engine: data patterns, joins, predicates (`>`, `<`, `>=`, `<=`, `=`)
- Aggregates: `count`, `sum`, `avg`, `min`, `max`
- Pull API (at least flat + one level of nesting)
- Clause reordering (essential for 5-clause queries)
- Bulk import (batch transact for loading the dataset)

**Nice to have (improves benchmark numbers):**
- LevelDB backend (for dataset sizes > available RAM)
- `:in` collection and relation bindings
- `or` / `not` clauses
- Range predicate pushdown optimization

### Data Loading Strategy

MBrainz data is typically distributed as EDN transaction files.  Loading plan:

1. Parse EDN files using `(std text edn)`
2. Batch transactions (500-1000 entities per tx) for throughput
3. Schema attributes defined first as bootstrap transaction
4. Entity data loaded with tempid resolution per batch
5. Target: load 6.6M entities in < 5 minutes (in-memory) or < 15 minutes (LevelDB)

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| LevelDB write amplification | LSM compaction spikes under heavy write load | Tune bloom filter bits and LRU cache size |
| DuckDB sync lag | Analytical queries see stale data | Configurable sync interval; explicit `sync!` for consistency |
| Query planner quality | Bad plans → slow queries | Start with heuristic, add cost-based planning later |
| Schema migrations on large DBs | Downtime for reindexing | Online reindexing with `reindex!` / `reindex-attribute!` |
| Datalog vs SQL confusion | Two query languages → cognitive load | Clear docs: Datalog for navigation, SQL for analytics |
| Memory pressure from caching | OOM on large working sets | LRU with configurable max size; eviction metrics |

---

## Related Work

### Internal
- `docs/jerboa-edge.md` — Webhook service demo (will use Jerboa-DB as storage in Phase 3)
- `docs/clojure-left.md` — Clojure gap analysis (Jerboa-DB fills the "database" gap)
- `lib/std/datalog.sls` — Semi-naive Datalog (171 lines) — starting point for query engine
- `lib/std/mvcc.sls` — MVCC with time-travel (130 lines)
- `lib/std/pmap.sls` — Persistent HAMT (868 lines) — entity maps
- `lib/std/misc/rbtree.sls` — Red-black tree (333 lines) — in-memory indices
- `lib/std/ds/sorted-map.sls` — Sorted map (328 lines) — range queries
- `lib/std/event-source.sls` — Event sourcing (103 lines) — tx log architecture
- `lib/std/schema.sls` — Validation framework (255 lines) — schema validation
- `lib/std/text/edn.sls` — EDN format (370 lines) — wire format + MBrainz data loading
- `lib/std/db/leveldb.sls` — LevelDB FFI bindings (40 lines)
- `lib/std/db/duckdb.sls` — DuckDB integration (79 lines)
- `lib/std/misc/lru-cache.sls` — LRU cache (178 lines)
- `lib/std/content-address.sls` — Content addressing (87 lines)

### External
- Rich Hickey, "The Database as a Value" (2012) — foundational Datomic talk
- `https://docs.datomic.com/` — Datomic reference documentation
- `https://www.xtdb.com/` — XTDB v2 (modern open-source Datomic alternative)
- `https://github.com/replikativ/datahike` — Datahike (MBrainz benchmark reference)
- `https://github.com/replikativ/datahike/tree/main/bench` — Datahike benchmark suite
