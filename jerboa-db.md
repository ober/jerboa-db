# Jerboa-DB: Datomic Without the JVM

**Goal:** A fully-featured Datomic clone built entirely on Jerboa, using LevelDB for
persistent index storage and DuckDB for analytics (planned).  Single binary,
embeddable, with a Datalog query engine and immutable time-travel over all data.

**Status:** 2026-04-24 — **Core Datomic parity: complete.** Core test
suite is 37/37 passing (`make test`), including `not-join`, the
`variance`/`stddev` family, stored `:db/fn`, `log` API object,
`index-range`, and `seek-datoms` — all previously tracked as gaps.
Phase 2 (LevelDB persistence) is production-ready.  Phase 4 (DuckDB
SQL-over-datoms) is functional; Parquet/CSV import helpers remain
stubs pending DuckDB `COPY` wiring.  Remaining stubs: schema
migration (rename/retype), Parquet export/import, CSV bulk import,
TLS transport, CLI entrypoint.

Implementation lives in `lib/jerboa-db/` (Jerboa library files using `#!chezscheme`
+ `(library ...)` form).  Built from scratch — no `(std mvcc)`, `(std datalog)`, or
other stdlib modules are used; all functionality is self-contained.

**MBrainz target:** Run the Datahike MBrainz benchmark to completion (6.6M
entities, complex Datalog joins, aggregation) as the validation gate.
**Benchmark harness complete (2026-04-13):** all 8 standard queries run against
synthetic data at configurable scale.  `make mbrainz-quick` for smoke test,
`make mbrainz` for full scale.  See [MBrainz Benchmark](#mbrainz-benchmark-results) section below.

---

## Implementation Status

| Phase | Description | Status |
|---|---|---|
| 1 | Core in-memory (datoms, indices, schema, Datalog, pull, time-travel) | ✅ Complete |
| 2 | LevelDB persistence (`connect("path")`, 4-index LevelDB backend, FASL encoding) | ✅ Complete |
| 3 | Query engine (planner, predicates, aggregates, rules, streaming) | ✅ Complete |
| 4 | DuckDB analytics (SQL over datoms, Parquet export/import) | ✅ Core done, Parquet 🚧 |
| 5 | Server mode (HTTP API, WebSocket tx-stream, remote peer) | ✅ Functional |
| 6 | Raft HA (in-process + TCP transport, read replicas) | ✅ Complete (TLS 🚧) |
| 7 | Polish (backup/restore ✅, GDPR excision ✅, schema migration 🚧) | ✅ Mostly done |
| 8 | Advanced (fulltext ✅, GC ✅, entity specs ✅, composite tuples ✅) | ✅ Complete |

---

## Missing Datomic Features

Comparison of Jerboa-DB against real Datomic.  This section documents gaps — whether
they are unimplemented (❌), implemented (✅), or planned stubs (🚧).

### Query Engine

| Feature | Status | Notes |
|---|---|---|
| `pull [*]` wildcard | ✅ Done | Returns all user attributes; skips internal schema attrs |
| Reverse refs in pull (`_attr`) | ✅ Done | `pull-attr-spec` handles `reverse-attr?` convention |
| `:rules` / `%` in `:in` | ✅ Done | Recursive rules with fixed-point evaluation |
| `not` clause | ✅ Done | Implicit-join-vars form |
| `not-join` clause | ✅ Done | `query/engine.ss` + `query/planner.ss` — explicit join-vars form |
| `or` clause | ✅ Done | Union of disjunctive branches |
| Parameterized `:in` (scalar, tuple, collection, relation) | ✅ Done | All four binding forms implemented |
| `count`, `sum`, `avg`, `min`, `max` aggregates | ✅ Done | Streaming single-pass with mutable accumulators |
| `count-distinct` aggregate | ✅ Done | Implemented in `query/aggregates.ss` |
| `median` aggregate | ✅ Done | Implemented |
| `variance`, `stddev` aggregates | ✅ Done | Welford one-pass in `query/aggregates.ss`; `variance-sample` + `stddev-sample` variants for Bessel-corrected |
| `rand`, `sample` aggregates | ✅ Done | Implemented in `query/aggregates.ss` |
| `ground` function clause | ✅ Done | `[(ground 42) ?x]` in `query/functions.ss` |
| `get-else`, `missing?` functions | ✅ Done | Implemented |
| Query `explain` | ✅ Done | Returns plan without executing |

### Transactions and Schema

| Feature | Status | Notes |
|---|---|---|
| `:db/add` assertions | ✅ Done | Map form and vector form |
| `:db/retract` retractions | ✅ Done | Both map-level and explicit vector form |
| `:db/retractEntity` | ✅ Done | Retracts all datoms for an entity |
| `:db/cas` (compare-and-swap) | ✅ Done | `[:db/cas eid attr old new]` |
| `db/tupleAttrs` composite tuples | ✅ Done | Auto-generation on transact |
| Tempid resolution | ✅ Done | String tempids, within-tx consistency |
| Lookup refs as entity IDs | ✅ Done | `[attr-ident value]` pair resolution |
| Schema migration (rename/retype) | 🚧 Stub | `migrate.ss` skeleton; additive-only works via `transact!` |
| Stored database functions (`:db/fn`) | ✅ Done | `tx.ss` lookup + dispatch; `+db/fn+` attribute in bootstrap schema |
| `:db.unique/identity` upsert | ✅ Done | Merges into existing entity |

### Time-Travel

| Feature | Status | Notes |
|---|---|---|
| `as-of` | ✅ Done | Filter to tx ≤ N |
| `since` | ✅ Done | Filter to tx > N |
| `history` | ✅ Done | All datoms including retracted |
| `tx-range` | ✅ Done | Returns datoms from tx-log between two tx IDs |
| `log` API object | ✅ Done | `(jerboa-db log)` — `(log conn)` returns a navigable handle; `tx-range` also accepts `conn` directly |

### Index Access API

| Feature | Status | Notes |
|---|---|---|
| `pull` | ✅ Done | Full pattern walking with nesting, limits, defaults |
| `pull-many` | ✅ Done | Exported from `core.ss` |
| `entity` / `touch` | ✅ Done | Lazy entity maps with eager materialization |
| `datoms` (direct index iteration) | ✅ Done | `(datoms db 'eavt eid)`, `(datoms db 'avet attr val)` — resolves ident symbols, applies time-travel filters |
| `index-range` | ✅ Done | `core.ss` — AVET range `[start, end]` filtered through time-travel |
| `seek-datoms` | ✅ Done | `core.ss` — positioned scan; materialized list today (generator form is future work) |

### Distribution

| Feature | Status | Notes |
|---|---|---|
| Single-node embedded | ✅ Done | Core use case |
| LevelDB persistent storage | ✅ Done | `connect("path/to/db")` |
| HTTP server | ✅ Done | `server.ss` — all REST + WebSocket endpoints; CLI entrypoint 🚧 |
| Remote peer client | ✅ Done | `peer.ss` — transact, query, pull, multi-URL failover |
| Raft consensus / HA (in-process) | ✅ Done | `replication.ss` + `cluster.ss`; 6/6 tests |
| Raft consensus / HA (TCP transport) | ✅ Done | `transport.ss`; 12/12 tests; plain TCP (TLS 🚧) |
| Read replicas | ✅ Done | Follower apply fiber, ~50ms lag |

### Analytics

| Feature | Status | Notes |
|---|---|---|
| Datalog aggregation (count/sum/avg/min/max) | ✅ Done | Built-in, streaming |
| DuckDB SQL over datoms | ✅ Done | `analytics-export! db`, `analytics-query ae sql` — lazy-loaded, tables: `datoms` + `attrs` |
| Parquet export/import | 🚧 Stub | `export-parquet`/`import-parquet` stubs in `analytics.ss` — DuckDB COPY TO wiring needed |
| CSV bulk import | 🚧 Stub | `import-csv` stub in `analytics.ss` — DuckDB COPY FROM wiring needed |

---

## Roadmap: Closing the Gaps

Detailed implementation specs for every ❌ and 🚧 item above.  Ordered from
smallest to largest effort.  Each entry names the files to touch, the exact
data structures and algorithms, and the expected test impact.

---

### Quick Wins (hours to a day each)

#### `variance` and `stddev` aggregates

**File:** `lib/jerboa-db/query/aggregates.ss`

Add two entries to the aggregate dispatch table alongside the existing
`count`/`sum`/`avg`/`median`.  Both can be computed in a single pass using
Welford's online algorithm, which avoids materializing all values:

```scheme
;; Welford accumulator: #(n mean M2)
;; n   = count of values seen
;; mean = running mean
;; M2  = sum of squared deviations from mean
(def (init-welford-acc) (vector 0 0.0 0.0))

(def (update-welford! acc val)
  (when (number? val)
    (let* ([n    (+ (vector-ref acc 0) 1)]
           [delta  (- (inexact val) (vector-ref acc 1))]
           [mean   (+ (vector-ref acc 1) (/ delta n))]
           [delta2 (- (inexact val) mean)]
           [M2   (+ (vector-ref acc 2) (* delta delta2))])
      (vector-set! acc 0 n)
      (vector-set! acc 1 mean)
      (vector-set! acc 2 M2))))

(def (finalize-variance acc)           ; population variance
  (let ([n (vector-ref acc 0)])
    (if (< n 2) 0.0 (/ (vector-ref acc 2) n))))

(def (finalize-variance-sample acc)    ; sample variance (Bessel-corrected)
  (let ([n (vector-ref acc 0)])
    (if (< n 2) 0.0 (/ (vector-ref acc 2) (- n 1)))))

(def (finalize-stddev acc)             ; population stddev
  (sqrt (finalize-variance acc)))

(def (finalize-stddev-sample acc)      ; sample stddev
  (sqrt (finalize-variance-sample acc)))
```

Wire into the streaming aggregation engine (`engine.ss`) by adding `variance`,
`stddev`, `variance-sample`, `stddev-sample` to `streamable-aggregate?`, and
adding cases to `init-agg-acc`, `update-agg-acc!`, and `finalize-agg-acc`.
The query syntax is `(variance ?x)` and `(stddev ?x)` in the `:find` clause,
identical to `(avg ?x)`.

Also add `variance`/`stddev` to the fallback `aggregate-apply` in
`aggregates.ss` for the non-streaming path.

**Test addition:** Add to `tests/query-tests.ss`:
```scheme
(test "variance aggregate"
  (= (caar (q '((find (variance ?v)) (where (?e thing/v ?v))) db))
     2.0))   ;; values 1 2 3 → variance = 2/3 ≈ 0.667 ... adjust for pop vs sample
```

---

#### `not-join` clause

**File:** `lib/jerboa-db/query/engine.ss`, `lib/jerboa-db/query/planner.ss`

Datomic's `not-join` is like `not` but with an explicit list of join variables.
The difference: `(not [?e] (?e :attr ?v))` means "exclude `?e` values for
which the sub-query succeeds, joining only on `?e`" — any other vars introduced
inside the `not-join` are local and not unified with the outer query.

In contrast, `(not (?e :attr ?v))` would also unify `?v` if it happened to be
bound in the outer context.  `not-join` gives precise control over which
variables cross the boundary.

**Parser change** in `parse-query` / `evaluate-single-clause`:
```scheme
;; Detect (not-join [?e ?x] sub-clause ...)
(def (not-join-clause? clause)
  (and (pair? clause) (eq? (car clause) 'not-join)
       (pair? (cdr clause)) (list? (cadr clause))   ; join-vars list
       (pair? (cddr clause))))                        ; at least one sub-clause

(def (evaluate-not-join-clause db clause bindings schema rules-ht)
  (let* ([join-vars  (cadr clause)]          ; e.g. (?e ?x)
         [sub-clauses (cddr clause)]
         ;; Create a restricted binding env containing ONLY the join vars
         [restricted-bindings
           (let ([new-ht (make-hashtable symbol-hash eq?)])
             (for-each (lambda (v)
                         (let ([val (binding-ref bindings v)])
                           (when val (hashtable-set! new-ht v val))))
                       join-vars)
             new-ht)]
         [results (evaluate-where-clauses db sub-clauses
                    (list restricted-bindings) schema rules-ht)])
    (if (null? results)
        (list bindings)   ; sub-query failed → keep this binding
        '())))            ; sub-query succeeded → exclude
```

Add `not-join-clause?` to `evaluate-single-clause`'s dispatch, and to
`clause-bound-vars` / `clause-used-vars` / `score-clause` in `planner.ss`
(it contributes no new bound vars, and its score follows the same logic as
`not` — score 3 when join-vars are all bound, −100 otherwise).

**Test:**
```scheme
(test "not-join excludes on explicit join var only"
  ;; Entity has both :a/x and :a/y.  not-join on [?e] with ?y local.
  ;; Should exclude entities that have ANY :a/y, not match by ?y value.
  ...)
```

---

#### `log` API Object

**File:** `lib/jerboa-db/core.ss`, new `lib/jerboa-db/log.ss`

Datomic exposes `(d/log conn)` as a navigable value that can be passed to
`tx-range` and treated as an iterable sequence of transaction records.  Each
record has:
- `:db/txInstant` — the wall-clock time of the transaction
- `:db.tx/data` — the datoms produced (as a sequence)

Jerboa-DB has `(tx-range conn start end)` already.  To close the gap, wrap
the connection's `tx-log` list into a log record:

```scheme
;; lib/jerboa-db/log.ss
(library (jerboa-db log)
  (export make-log log? log-conn log->tx-range)

  (defstruct log-handle (conn))

  ;; (log conn) → log-handle
  (def (log conn) (make-log-handle conn))

  ;; Navigate: (tx-range (log conn) start end) already works because
  ;; tx-range's first arg is a conn.  For the log object form used in Datomic:
  ;; (d/tx-range log {:start t1 :end t2})

  ;; Iterable: returns all tx-reports in reverse-chronological order
  (def (log->list log-handle)
    (connection-tx-log (log-handle-conn log-handle)))

  ;; Each tx-report is already a record with:
  ;;   tx-report-db-before, tx-report-db-after, tx-report-tx-data, tx-report-tempids
  ;; Add tx-instant accessor:
  (def (tx-report-instant report)
    ;; Extract db/txInstant from the tx-data datoms
    (let ([inst-datom
            (find (lambda (d)
                    (let ([a (schema-lookup-by-id
                               (db-value-schema (tx-report-db-after report))
                               (datom-a d))])
                      (and a (eq? (db-attribute-ident a) 'db/txInstant))))
                  (tx-report-tx-data report))])
      (and inst-datom (datom-v inst-datom)))))
```

Export `log` from `(jerboa-db core)`.  The existing `tx-range` already
handles `conn` — no behavioral change needed, just the new `log` wrapper
type that can be passed to `tx-range` as a convenience (detect `log-handle?`
and unwrap the conn).

---

#### `index-range` and `seek-datoms`

**File:** `lib/jerboa-db/core.ss`

These are thin wrappers over the `datoms` function that was added in the
previous session.  `datoms` already handles direct index access; these are
Datomic-compatible name aliases.

```scheme
;; (index-range db attr start end)
;; Returns all AVET datoms for attr whose value is in [start, end].
(def (index-range db attr-ident start end)
  ;; Equivalent to (datoms db 'avet attr-ident) filtered by [start, end]
  (let* ([schema (db-value-schema db)]
         [attr   (schema-lookup-by-ident schema attr-ident)]
         [aid    (and attr (db-attribute-id attr))]
         [idx    (db-resolve-index db 'avet)]
         [lo     (make-datom 0 (or aid 0) (if start start +min-val+) 0 #t)]
         [hi     (make-datom (greatest-fixnum) (or aid (greatest-fixnum))
                             (if end end +max-val+) (greatest-fixnum) #t)]
         [raw    (dbi-range idx lo hi)])
    (filter (lambda (d)
              (and (db-filter-datom? db d)
                   (datom-added? d)))
            raw)))

;; (seek-datoms db index-name & components)
;; Like datoms but positions a cursor at the given components and returns
;; a lazy sequence from there to end-of-index.  Since we have no cursor
;; abstraction, implement as a range from the given position to +∞.
;; components follow the index order (e.g. eavt: e a v tx).
(def (seek-datoms db index-name . components)
  ;; Reuse datoms but with an open upper bound
  (apply datoms db index-name components))
```

For a true cursor/lazy-sequence implementation, `seek-datoms` would ideally
return a generator rather than a materialized list.  A generator form using
`(std lazy)` or a simple stateful closure:

```scheme
(def (seek-datoms db index-name . components)
  ;; Returns a thunk that yields datoms one at a time.
  ;; For now, materialize as a list (correct, not lazy).
  (apply datoms db index-name components))
```

Export both from `(jerboa-db core)`.

---

### Medium Effort (days each)

#### Stored Database Functions (`:db/fn`)

**Files:** `lib/jerboa-db/tx.ss`, `lib/jerboa-db/schema.ss`, `lib/jerboa-db/core.ss`

In Datomic, you can store a Clojure function as a datom value and invoke it
during transaction processing:

```clojure
{:db/ident :my-fn
 :db/fn #db/fn {:lang :clojure
                :params [db eid delta]
                :code "(+ (d/q '[:find ?v :where [?e :counter/val ?v]] db) delta)"}}
;; Then in a transaction:
[:my-fn entity-id 1]
```

The function receives the current database value and returns additional
transaction data (a list of `[:db/add ...]` operations).

**Design for Jerboa-DB:**

1. **New attribute** `db/fn`: `db.type/any`, stores a compiled procedure or
   source code list.  Register in `bootstrap-schema!` at ID 14 (currently unused).

2. **Schema change** (`schema.ss`): Add `db-attribute-fn` field to `db-attribute`
   record, or treat `db.type/any` entities with `db/ident` + `db/fn` as
   callable.

3. **Transaction dispatch** (`tx.ss`): In `process-transaction`, after the
   existing map/vector parsing, add a case for `(list fn-ident . args)`:
   ```scheme
   ;; In process-tx-op:
   [(and (pair? op) (symbol? (car op)) (not (keyword? (car op))))
    ;; Looks like a function invocation: (fn-ident arg1 arg2 ...)
    (let* ([fn-ident (car op)]
           [args (cdr op)]
           [fn-attr (schema-lookup-by-ident schema fn-ident)]
           [fn-val  (and fn-attr
                         ;; Look up current value of db/fn for this ident's entity
                         (lookup-fn-value db fn-ident))])
      (unless fn-val
        (error 'transact! "Unknown database function" fn-ident))
      ;; Call the function with (current-db . args), collect returned tx-ops
      (let ([result-ops (apply fn-val db args)])
        ;; result-ops is a list of additional tx operations — recurse
        (for-each (lambda (sub-op) (process-tx-op db sub-op schema)) result-ops)))]
   ```

4. **Storing functions**: In Jerboa, database functions are Jerboa `lambda`
   values stored as `db.type/any`.  They are transacted like:
   ```scheme
   (transact! conn
     (list `((db/ident . inc-counter)
             (db/fn . ,(lambda (db eid delta)
                         (let ([cur (ffirst (q '((find ?v) (where (?e counter/val ?v)))
                                              db eid))])
                           (list (list :db/add eid 'counter/val (+ cur delta)))))))))
   ```

5. **Security note**: Stored procedures execute arbitrary code.  For embedded
   single-process use this is fine (same process trust boundary).  For server
   mode (Phase 5), stored functions should be sandboxed or restricted to
   pure functions with no side effects.

**Effort estimate:** ~2 days.  The transaction dispatch change is surgical;
the main complexity is serializing/deserializing the function value for LevelDB
persistence (FASL handles arbitrary Scheme values, so this may work for free
as long as closures are FASL-serializable — verify with `fasl-write` on a
lambda).

---

#### Schema Migration (Rename / Retype Attributes)

**File:** `lib/jerboa-db/migrate.ss` (skeleton exists at 270 lines)

The current `migrate.ss` has `migrate!`, `reindex!`, `migration-plan`,
`migration-dry-run` as stubs.  Additive migrations (new attributes) already
work via `transact!` — the migration system needs to handle:

1. **Attribute rename** — `{:db/id [:db/ident :old/name] :db/ident :new/name}`

   Implementation: transact a retraction of `:old/name` + assertion of
   `:new/name` for the schema entity.  The in-memory `schema-registry` must
   be updated:
   ```scheme
   (def (migrate-rename! conn old-ident new-ident)
     ;; 1. Find the schema entity for old-ident
     (let* ([db (db conn)]
            [schema (db-value-schema db)]
            [attr (schema-lookup-by-ident schema old-ident)])
       (unless attr (error 'migrate-rename! "Unknown attribute" old-ident))
       (let ([eid (... find entity with db/ident = old-ident ...)])
         ;; 2. Retract old ident, assert new ident in same tx
         (transact! conn
           (list (vector :db/retract eid 'db/ident old-ident)
                 (vector :db/add    eid 'db/ident new-ident)))
         ;; 3. Schema registry is updated automatically by materialize-schema-datoms!
         )))
   ```

2. **Value type change** — only safe for compatible conversions (e.g., `string`
   → `keyword` where all values are valid symbols, or any → `long` when all
   values are integers).

   Implementation:
   ```scheme
   (def (migrate-retype! conn attr-ident new-type coerce-fn)
     ;; 1. Scan all current datoms for attr-ident via AEVT
     ;; 2. For each, compute coerce-fn(old-value) → new-value
     ;; 3. Build a transaction that retracts the old datom and asserts
     ;;    the new value
     ;; 4. Update the schema attribute's value-type
     ...)
   ```

   A dry-run mode should scan all values and report any that `coerce-fn` cannot
   handle before touching the data.

3. **Attribute deletion** — retract all datoms for the attribute, then retract
   the schema entity itself.  Must also drop the AVET index entries.

   The `reindex!` function (already stubbed) handles rebuilding AVET after
   adding `db/index` to an existing attribute.

4. **Migration plan** (`migration-plan`): Return a list of operations as a
   data structure (not executed), so callers can inspect and confirm.

5. **LevelDB consistency**: After retype, if the attribute had AVET entries,
   they need rebuilding under the new value encoding.  Call `reindex!` after
   the value-type change.

**Effort estimate:** ~3–4 days.  The transact-based rename is straightforward;
retype requires a full AEVT scan and batch transaction which could be slow on
large datasets.

---

### Architectural Improvements

#### Hash-Join for Large Intermediate Relations

**Files:** `lib/jerboa-db/query/engine.ss`, `lib/jerboa-db/query/planner.ss`

**Problem:** Q4 (~2468ms) and Q8 (~2056ms) suffer from nested-loop join
explosion.  The current algorithm is clause-at-a-time nested loop: for each
existing binding, evaluate the next clause and produce new bindings.  When two
large relations share a join variable, this is O(M × N).

**Solution:** Detect binary joins between two clauses sharing a variable and
execute them as a hash-join: build a hash table from the smaller side, probe
with the larger.

**Algorithm:**

```
Hash-join of clause A and clause B on shared variable ?x:
1. Evaluate A independently → relation_A  (list of binding maps)
2. Build hash table:  HT[value_of_?x] → list of bindings_from_A
3. Evaluate B independently → relation_B  (list of binding maps)
4. Probe: for each binding in relation_B,
     look up HT[binding[?x]]
     for each match in HT, merge the two bindings
5. Result: merged binding list
```

**Planner change** (`planner.ss`): After `reorder-clauses`, add a
`detect-hash-join-candidates` pass that identifies consecutive pairs of data
patterns sharing a logic variable where neither is bound from the outer context
(meaning they form a true binary join rather than a lookup).

```scheme
(def (detect-hash-join-candidates clauses bound-vars)
  ;; Returns: list of (hash-join clause-A clause-B join-vars)
  ;; or (sequential clause) for non-joinable clauses
  (let loop ([clauses clauses] [bound bound-vars] [result '()])
    (cond
      [(null? clauses) (reverse result)]
      [(null? (cdr clauses))
       (reverse (cons (list 'sequential (car clauses)) result))]
      [else
       (let* ([ca (car clauses)]
              [cb (cadr clauses)]
              [vars-a (clause-bound-vars ca '())]
              [vars-b (clause-bound-vars cb '())]
              [shared (filter (lambda (v) (memq v vars-b)) vars-a)]
              ;; Hash-join only if: both introduce vars, they share a join var,
              ;; and the shared var isn't already bound from outside
              [joinable? (and (pair? shared)
                              (not (for-any (lambda (v) (memq v bound)) shared))
                              (data-pattern? ca)
                              (data-pattern? cb))])
         (if joinable?
             (loop (cddr clauses)
                   (append vars-a vars-b bound)
                   (cons (list 'hash-join ca cb shared) result))
             (loop (cdr clauses)
                   (append (clause-bound-vars ca bound) bound)
                   (cons (list 'sequential ca) result))))])))
```

**Engine change** (`engine.ss`): In `evaluate-where-clauses`, check for
`hash-join` plans and dispatch to `evaluate-hash-join`:

```scheme
(def (evaluate-hash-join db ca cb join-vars bindings-list schema)
  ;; Evaluate both sides from the current bindings-list
  (let* ([side-a (flatmap (lambda (b)
                            (evaluate-single-clause db ca b schema #f))
                          bindings-list)]
         ;; Build probe table indexed on join-vars
         [ht (make-hashtable equal-hash equal?)]
         [_ (for-each (lambda (b)
                        (let ([key (map (lambda (v) (binding-ref b v)) join-vars)])
                          (hashtable-update! ht key
                            (lambda (existing) (cons b existing)) '())))
                      side-a)]
         ;; Evaluate side-b and probe
         [side-b (flatmap (lambda (b)
                            (evaluate-single-clause db cb b schema #f))
                          bindings-list)])
    (flatmap (lambda (b-binding)
               (let* ([key (map (lambda (v) (binding-ref b-binding v)) join-vars)]
                      [matches (hashtable-ref ht key '())])
                 (map (lambda (a-binding) (merge-bindings a-binding b-binding))
                      matches)))
             side-b)))

(def (merge-bindings b1 b2)
  ;; Merge two binding maps; b2 values win on conflict (shouldn't conflict on join vars)
  (let ([new-ht (hashtable-copy b1 #t)])
    (hashtable-walk b2 (lambda (k v) (hashtable-set! new-ht k v)))
    new-ht))
```

**Expected impact:** For Q8, the join between 131K tracks and 13K releases
should become O(131K + 13K) hash probe instead of O(131K × 13K) nested loop.
Estimated 10–50× speedup on Q8.  Q4 has 428K output rows (true join result
size) so hash-join reduces the intermediate work but not the output count.

**Effort estimate:** ~1 week.  The planner change is surgical; the engine
change requires careful handling of the shared binding context.

---

#### Cardinality Statistics for Planner

**Files:** `lib/jerboa-db/query/planner.ss`, `lib/jerboa-db/stats.ss` (new)

**Problem:** The planner has no information about how many datoms exist per
attribute.  It scores patterns purely on whether variables are bound/unbound,
not on actual selectivity.  For Q8, `(?r release/status ?status)` should start
first (13K releases) but the planner starts from `(?t track/duration ?dur)`
(131K tracks) because all patterns score equally with no bound vars.

**Solution:** Maintain per-attribute datom counts, updated on every `transact!`.

```scheme
;; lib/jerboa-db/stats.ss (new)
(library (jerboa-db stats)
  (export make-db-stats db-stats? db-stats-attr-count
          db-stats-update! db-stats-estimate-selectivity)

  (defstruct db-stats
    (attr-counts))     ;; hashtable: attr-id → integer (# of current datoms)

  (def (make-db-stats) (make-db-stats (make-eq-hashtable)))

  (def (db-stats-update! stats tx-datoms)
    ;; Called after each transaction with the new datoms
    (for-each (lambda (d)
                (let ([key (datom-a d)])
                  (hashtable-update! (db-stats-attr-counts stats)
                    key
                    (lambda (n) (if (datom-added? d) (+ n 1) (max 0 (- n 1))))
                    0)))
              tx-datoms))

  (def (db-stats-attr-count stats attr-id)
    (hashtable-ref (db-stats-attr-counts stats) attr-id 0))

  ;; Estimate how selective a pattern (?e attr ?v) is when only attr is known.
  ;; Lower count = more selective = higher score.
  (def (db-stats-estimate-selectivity stats attr-id max-count)
    (let ([n (db-stats-attr-count stats attr-id)])
      (if (= n 0) 1000   ;; unknown → treat as very selective
          (- 1000 (round (* 999 (/ n max-count))))))))
```

**Integration:**

1. Add `db-stats` field to the `connection` record.
2. In `transact!`, call `db-stats-update!` with the new tx-data.
3. Pass `db-stats` to `reorder-clauses`, and use `db-stats-estimate-selectivity`
   in `score-clause` for unbound data patterns:
   ```scheme
   ;; In score-clause for data patterns where e, v are both unbound:
   ;; Use attribute cardinality if available, fall back to current heuristic
   (let ([stat-score (if db-stats
                         (db-stats-estimate-selectivity
                           db-stats aid total-datom-count)
                         20)])  ; fallback: attribute is always bound
     (+ stat-score 20))
   ```

**Expected impact:** Q8 would start with `release/status` (13K datoms) instead
of `track/duration` (131K datoms), potentially 10× speedup.

**Effort estimate:** ~2 days for the stats maintenance; ~1 day for planner
integration.

---

#### Vector-Indexed Bindings (O(1) Copy-on-Write)

**Files:** `lib/jerboa-db/query/engine.ss`, `lib/jerboa-db/query/planner.ss`

**Problem:** The current binding representation is a copy-on-write `eq?`
hashtable.  `(hashtable-copy ht #t)` is O(n) where n = number of bindings.
For Q4's 428K output rows, each with ~5 bound variables, this is ~2M hashtable
copies.  Vector indexing would make `binding-set` O(1) (just copy a fixed-size
vector + set one slot).

**Design:**

At query compile time (in `query-db`), assign each logic variable a slot index:

```scheme
(def (assign-var-slots find-vars where-clauses)
  ;; Collect all logic vars in the query in encounter order
  (let ([seen (make-hashtable symbol-hash eq?)]
        [slots '()]
        [next 0])
    (def (visit v)
      (when (and (logic-var? v) (not (hashtable-ref seen v #f)))
        (hashtable-set! seen v next)
        (set! slots (cons (cons v next) slots))
        (set! next (+ next 1))))
    (for-each visit find-vars)
    (for-each (lambda (c) (for-each visit (clause-used-vars c))) where-clauses)
    (values (list->hashtable slots) next)))  ; var→slot, total-slots
```

Replace the `eq?` hashtable binding with a flat vector:

```scheme
;; New binding representation: #(val0 val1 val2 ... ) — #f for unbound
(def (make-empty-bindings n-slots) (make-vector n-slots #f))
(def (binding-ref bindings slot)   (vector-ref bindings slot))
(def (binding-set bindings slot val)
  (let ([new (vector-copy bindings)])
    (vector-set! new slot val)
    new))
```

All call sites that use `(binding-ref bindings var)` become
`(binding-ref bindings (hashtable-ref var-slots var 0))`.  The `var-slots`
hashtable is closed over in `query-db` and threaded into all evaluation
functions.

**Impact:** `binding-set` drops from O(n) hashtable-copy to O(n) vector-copy,
but `vector-copy` has much lower constant factor than `hashtable-copy`
(contiguous memory, no hash chain traversal).  For small var counts (5–10),
vector-copy is 5–10× faster than hashtable-copy.

**Effort estimate:** ~1 week.  The change touches every function that creates
or reads bindings, which is the entire query engine.  High impact, high risk.
Recommend implementing on a branch with the full MBrainz benchmark suite as
the regression guard.

---

### Infrastructure (Phase 5: HTTP Server)

**File:** `lib/jerboa-db/server.ss` (221-line stub)

The stub already has the library declaration, imports `(std net fiber-httpd)`,
and outlines the handler structure.  What needs implementing:

#### REST API design

```
POST /db/:name/transact      — submit a transaction (EDN or JSON body)
POST /db/:name/q             — run a Datalog query (EDN or JSON body)
GET  /db/:name/entity/:eid   — entity lookup
GET  /db/:name/pull/:eid     — pull with pattern in query string
GET  /db/:name/as-of/:t      — returns db-value token for time travel
WS   /db/:name/tx-stream     — WebSocket: push each new tx-report to subscriber

GET  /status                  — health check, returns db-stats JSON
GET  /dbs                     — list open database names
```

#### Serialization

Requests and responses use EDN (via `(std text edn)`) or JSON (via `(std text
json)`).  Content-type negotiation:
- `application/edn` → EDN (preferred, round-trips all Jerboa types)
- `application/json` → JSON (limited: symbols become strings, refs lose type info)

Datoms serialize as `[e a v tx added?]` EDN vectors.  Query results are
lists of tuples.  Pull results are nested EDN maps.

#### Connection pool

A single `(std db conpool)` connection pool wraps the jerboa-db `connection`
object.  Multiple fiber-httpd worker fibers share the pool.  Since jerboa-db
`transact!` is synchronous and mutates the connection, use a single-writer
mutex for `transact!` and allow concurrent reads (all `q`, `pull`, `entity`
calls operate on immutable `db-value` snapshots and need no lock).

```scheme
;; In server.ss:
(def *conn-registry* (make-hashtable string-hash equal?))  ; db-name → conn
(def *write-mutex*   (make-mutex))                          ; serialize transact!

(def (handle-transact! req db-name)
  (with-mutex *write-mutex*
    (let* ([conn (hashtable-ref *conn-registry* db-name #f)]
           [ops  (edn-parse (request-body req))])
      (let ([report (transact! conn ops)])
        (response 200 (edn-write (tx-report->edn report)))))))

(def (handle-query req db-name)
  ;; No lock needed — operates on immutable snapshot
  (let* ([conn (hashtable-ref *conn-registry* db-name #f)]
         [db   (db conn)]  ; snapshot
         [qform (edn-parse (request-body req))])
    (response 200 (edn-write (q qform db)))))
```

#### WebSocket tx-stream

On `transact!`, publish the tx-report to all active WebSocket subscribers for
that database.  Use a per-database broadcast channel (from `(std actor)` or a
simple list of open WebSocket handles):

```scheme
(def *subscribers* (make-hashtable string-hash equal?))  ; db-name → list of ws-handles

(def (broadcast-tx! db-name report)
  (let ([subs (hashtable-ref *subscribers* db-name '())])
    (for-each (lambda (ws)
                (ws-send! ws (edn-write (tx-report->edn report))))
              subs)))
```

**Effort estimate:** ~2 weeks.  The fiber-httpd integration is straightforward;
the main complexity is EDN/JSON serialization for all value types, and making
the connection registry handle multiple named databases.

---

### Infrastructure (Phase 6: Raft Distribution)

**Files:** `lib/jerboa-db/replication.ss`, `lib/jerboa-db/cluster.ss`, `lib/jerboa-db/peer.ss`

The in-process Raft layer, TCP transport, and HTTP peer client are all complete.
Multi-node replication works both inside a single OS process (in-process channels)
and across separate OS processes over TCP (Phase 6.3).  Local db-value caching
in the peer client (Datomic-style segment cache) is the remaining item (Phase 6.4).

#### What's implemented

**`replication.ss`** — Raft node lifecycle + apply path:
- `start-replication / stop-replication` — wrap a `(std raft)` node
- `start-local-cluster N` — create a fully-wired in-process N-node cluster
- `replicated-transact!` — propose a tx to the Raft log (leader only)
- `replication-for-each-committed!` — iterate newly committed log entries via callback
- `replication-status / replication-leader? / replication-running?`
- Read consistency helpers: `read-committed`, `read-latest`, `as-of-tx`

**`cluster.ss`** — Bridge between `(jerboa-db core)` and `(jerboa-db replication)`:
- `replicated-conn` record — wraps a connection + replication-state + apply fiber
- `cluster-transact!` — propose → wait for consensus → return tx-report
- `cluster-db / cluster-db/consistent` — read db-value at appropriate consistency
- `cluster-status / cluster-leader?` — Raft status + db basis-tx
- `start-local-db-cluster N` — convenience for in-process test clusters
- Background apply fiber — reads the leader's complete Raft log every 50 ms and applies committed entries to each node's local connection via full `transact!` semantics (schema materialization, fulltext, stats)

#### Architecture notes

**In-process clusters** (`start-local-db-cluster`): all nodes read the
**cluster leader's** Raft log in their apply fiber.  This simplifies the
in-process case and was originally a workaround for a log-truncation bug.

**TCP transport nodes** (`start-transport-db-node!`): each node's apply fiber
reads its **own** committed log via `replication-for-each-committed!`.  This
is correct for multi-process clusters where there is no shared memory.  This
path depends on the `(std raft)` AppendEntries log-truncation fix (≤ instead of
<) being in place — without it, followers drop earlier log entries.

For **1-node clusters**, `(std raft)` never fires `try-advance-commit-index!`
(it's triggered by AppendEntriesResponse, which requires peers).  The apply
fiber detects this case and treats the last log entry as effectively committed
— the single node IS the quorum.

#### Roles

- **Leader (transactor):** Accepts `cluster-transact!`.  Proposes entries to
  the Raft log.  Returns after the entry is committed and applied locally.

- **Follower (read replica):** Apply fiber replicates all committed entries
  from the leader's log.  Serves `cluster-db` reads (eventually consistent,
  typically < 100 ms behind the leader).

- **Transport (Phase 6.3, complete):** `transport.ss` bridges each node's
  `(std raft)` channels to TCP sockets using length-framed FASL messages.
  Outbound proxy fibers serialise and write; inbound accept-loop fibers
  deserialise and deliver.  Reconnect backoff (100ms → 5s) handles transient
  failures.  API: `start-transport-db-node!`, `transport-node-add-peer!`.

#### Peer caching (the real Datomic secret)

Datomic's performance at scale comes from peers caching immutable database
segments in memory.  Since segments never mutate, a peer can cache the entire
working set.  For jerboa-db the equivalent is the `db-value` record (an
immutable persistent RB-tree) which lives on each node after `transact!`
updates the connection.  All reads are against a local immutable snapshot —
no network round-trip needed.

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
| **Jerboa-DB** | **Native** | **LevelDB (persistent) + in-memory RB-trees; DuckDB planned** | **Yes** | **Yes** | **Yes (Raft, TCP)** | **Yes** |

Jerboa-DB fills a gap that doesn't exist today: a **native, embeddable,
distributed, time-traveling Datalog database** that ships as a single binary and
requires zero external infrastructure.

### Why Jerboa Is the Right Platform

This isn't just "rewrite Datomic in Scheme."  Jerboa has building blocks that
make this project tractable in a way it wouldn't be in Go, Rust, or Python:

| Datomic Concept | Jerboa Building Block | Actual Usage |
|---|---|---|
| Immutable fact store | `(std mvcc)` — MVCC with time-travel | **Not used** — self-contained db-value snapshots |
| EAVT/AEVT/VAET indices | `(std db leveldb)` — LSM tree with bloom filters | **Used** — `index/leveldb.ss` imports `(std db leveldb)` |
| Datalog query engine | `(std datalog)` — semi-naive evaluation | **Not used** — self-contained engine in `query/engine.ss` |
| Logic variable unification | `(std logic)` — full miniKanren | **Not used** — custom unification in engine |
| Persistent collections | `(std data pmap)`, `(std pvec)`, `(std pset)` | **Not used** — custom RB-tree in `index/memory.ss` |
| Transaction log | `(std event-source)` — immutable event log | **Not used** — FASL segment log in `tx-log.ss` |
| Schema validation | `(std schema)` — composable validators | **Not used** — self-contained `schema.ss` |
| Serialization | `(std fasl)` — Chez native binary format | **Used** — FASL encoding of datom values in LevelDB |
| Server | `(std net fiber-httpd)` — fiber-native HTTP | **Used in stub** — `server.ss` imports it; not functional yet |
| Analytical queries | `(std db duckdb)` — columnar OLAP engine | **Used in stub** — `analytics.ss` imports it; DuckDB planned |
| Distributed consensus | `(std raft)` — leader election + log replication | **Used** — `replication.ss` + `cluster.ss` + `transport.ss`; 3 bugs fixed in `(std raft)` itself |
| Actor supervision | `(std actor)` — OTP-style fault tolerance | **Available** — Phase 5+ |
| EDN wire format | `(std text edn)` — EDN parser | **Used in server stub** — for HTTP request/response |

The implementation chose to build the core engine from scratch (using only
`(chezscheme)` + `(jerboa prelude)`) for full control over data layout and
performance.  Stdlib modules are used for FFI (LevelDB, DuckDB) and planned
for server/distribution phases.

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

Jerboa-DB is organized as a set of `(jerboa-db ...)` modules.  All source lives
in `lib/jerboa-db/` as Jerboa library files (`#!chezscheme` + `(library ...)`
form).  Run with `--libdirs lib:/path/to/jerboa/lib`.

```
lib/jerboa-db/
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
├── analytics.ss          ;; DuckDB-style analytics helpers
├── encoding.ss           ;; Binary encoding for datom keys and values
├── server.ss             ;; HTTP/WebSocket API server (stub)
├── peer.ss               ;; Peer protocol (stub)
├── replication.ss        ;; Raft-based HA (stub)
├── migrate.ss            ;; Schema migration (stub)
├── excision.ss           ;; GDPR physical deletion
├── backup.ss             ;; Backup/restore
├── metrics.ss            ;; Prometheus metrics
├── fulltext.ss           ;; In-memory inverted index for fulltext search
├── spec.ss               ;; Entity specs / attribute predicate validation
├── gc.ss                 ;; Datom garbage collection
└── value-store.ss        ;; Content-addressed value deduplication
```

### Actual Dependencies (as-implemented)

The core engine is built from scratch using `(chezscheme)` and `(jerboa prelude)`
only.  In-memory indices use a custom red-black tree implementation embedded in
`index/memory.ss`.  The LevelDB backend (`index/leveldb.ss`) imports `(std db leveldb)`
for the FFI bindings.  The analytics stub and server stub import `(std db duckdb)`,
`(std net fiber-httpd)`, `(std net fiber-ws)`, and `(std text edn)`.
No `(std mvcc)`, `(std datalog)`, or other stdlib modules are used in the core path.

```
(jerboa-db core)
├── (jerboa prelude)             ;; Language + stdlib
├── (jerboa-db datom)            ;; 5-tuple record + 4 comparators
├── (jerboa-db schema)           ;; Attribute registry
├── (jerboa-db index protocol)   ;; Pluggable index interface
├── (jerboa-db index memory)     ;; In-memory RB-tree indices
├── (jerboa-db history)          ;; db-value snapshots + time-travel
├── (jerboa-db cache)            ;; LRU cache
├── (jerboa-db tx)               ;; Transaction processing
├── (jerboa-db query engine)     ;; Datalog query compiler
├── (jerboa-db query pull)       ;; Pull API
├── (jerboa-db entity)           ;; Lazy entity maps
├── (jerboa-db fulltext)         ;; Fulltext inverted index
├── (jerboa-db gc)               ;; Garbage collection
├── (jerboa-db spec)             ;; Entity specs
└── (jerboa-db value-store)      ;; Content-addressed values
```

---

## Implementation Plan

> **Current state (2026-04-13):** Phases 1–3, 5, 6.1–6.3, 7–8 are fully
> implemented and tested (52/52 tests pass: 34 core + 6 in-process cluster +
> 12 TCP transport).  Phase 2 (LevelDB) is wired and production-ready.  Phase 4
> (DuckDB analytics) is structurally complete but FFI-build-dependent.  The
> sections below describe design intent and serve as reference documentation.

### Phase 1: Core — In-Memory Datomic ✅ IMPLEMENTED

**Goal:** A working Datomic that stores everything in memory.  No LevelDB, no
DuckDB, no networking.  Prove the data model, query engine, and transaction
processing work correctly.

**Deliverable:** `(jerboa-db core)` usable from the REPL.

#### 1.1 Datom Record and Encoding

```scheme
;; lib/jerboa-db/datom.ss

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
;; lib/jerboa-db/index/memory.ss

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
;; lib/jerboa-db/schema.ss

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
;; lib/jerboa-db/tx.ss

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
;; lib/jerboa-db/index/protocol.ss
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
;; lib/jerboa-db/query/engine.ss

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
;; lib/jerboa-db/query/pull.ss

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
;; lib/jerboa-db/history.ss

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

### Phase 2: Persistence — LevelDB Backend ✅ IMPLEMENTED

**Goal:** Replace in-memory indices with LevelDB.  Data survives process restarts.
Transaction log is durable.

**What's wired:** `connect("path/to/db")` triggers `ensure-leveldb!` in `core.ss`,
which lazy-loads `(jerboa-db index leveldb)` and calls `make-leveldb-index-set`.
Four separate LevelDB directories (`eavt/`, `aevt/`, `avet/`, `vaet/`) are opened
with bloom filters (10 bits/key) and a 64MB LRU block cache each.  Keys are 28-byte
binary-encoded datom components that sort correctly via bytewise comparison.  Values
are FASL-encoded full datom records for fast Scheme deserialization.  `close(conn)`
cleanly closes all four handles.

#### 2.1 LevelDB Index Backend

```scheme
;; lib/jerboa-db/index/leveldb.ss

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
;; lib/jerboa-db/encoding.ss

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
;; lib/jerboa-db/tx-log.ss

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
;; lib/jerboa-db/encoding.ss (continued)

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
;; lib/jerboa-db/query/planner.ss

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
;; lib/jerboa-db/query/aggregates.ss

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
;; lib/jerboa-db/query/functions.ss

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
;; lib/jerboa-db/query/rules.ss

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

### Phase 4: DuckDB Analytics Layer ✅ CORE DONE

**Goal:** Wire DuckDB as an analytical query engine for workloads that don't
fit Datalog — aggregation over large datasets, window functions, ad-hoc SQL.

**Current state:** `analytics.ss` fully rewritten and wired into `core.ss`:
- `new-analytics-engine schema` — creates an in-memory or file-backed DuckDB instance and the `datoms` + `attrs` tables
- `analytics-sync! ae db-val` — full EAVT scan → DuckDB bulk insert (delete + re-insert on each sync)
- `analytics-query ae sql` — executes SQL, returns list of alists with string column names
- `analytics-export! db-val` — convenience one-liner: create engine + sync
- `analytics-close! ae` — closes DuckDB handle
- All exported from `(jerboa-db core)` via lazy-load (`ensure-analytics!`)
- Parquet and CSV stubs present but not wired to DuckDB COPY TO/FROM

DuckDB FFI
availability is also build-dependent.  This phase is post-MBrainz work.

#### 4.1 DuckDB Replica

```scheme
;; lib/jerboa-db/analytics.ss

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

### Phase 5: Server Mode ✅ Functional

**Goal:** A standalone Jerboa-DB server accessible over HTTP and WebSocket,
enabling multi-client access and remote peers.

**Current state:** `server.ss` is fully functional.  All REST endpoints are
implemented, the WebSocket tx-stream is wired up, the named-database registry
allows multiple connections to be served from one process, and
`register-cluster!` bridges the cluster status to the HTTP API.  A runnable
entry point (`main`) and CLI are the remaining items before shipping.

#### 5.1 HTTP API

```scheme
;; lib/jerboa-db/server.ss

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

#### 5.2 Client Library ✅ Functional (HTTP-based)

**File:** `lib/jerboa-db/peer.ss` — see Phase 6.4 for full documentation.

```scheme
(import (jerboa-db peer))

;; Single server:
(def conn (connect-remote "http://localhost:8484"))

;; Cluster with automatic failover across leader changes:
(def conn (connect-remote* '("http://node-0:8484" "http://node-1:8484")))

;; All basic operations work over HTTP:
(remote-transact! conn tx-ops)     ;; → tx-report alist
(remote-q conn query-form)         ;; → result rows
(remote-pull conn pattern eid)     ;; → entity map
```

Note: operations are server-side round-trips.  Local db-value caching
(Datomic-style) is planned for Phase 6.4.

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

**Current state:** Complete.  Both the in-process layer (6/6 tests) and the TCP
transport layer (12/12 tests) are implemented and passing.  Each `start-transport-db-node!`
call returns a `(values transport-node replicated-conn)` pair compatible with the
full cluster API.  The remaining items are TLS wrapping for production use and
local db-value caching in the peer client (Phase 6.4).

#### 6.1 Raft-Based Transactor ✅ Complete (in-process)

```scheme
;; lib/jerboa-db/replication.ss + lib/jerboa-db/cluster.ss

;; Create a 3-node in-process cluster:
(def cfg (new-replication-config 'node-0 #f ":memory:"))
(def nodes (start-local-db-cluster 3 cfg))

;; Wait for Raft to elect a leader (150–300 ms):
(sleep (make-time 'time-duration 500000000 0))
(def leader (find cluster-leader? nodes))

;; Transact through the leader (blocks until committed + applied):
(cluster-transact! leader
  '({db/ident person/name  db/valueType db.type/string  db/cardinality db.cardinality/one}
    {db/ident person/age   db/valueType db.type/long    db/cardinality db.cardinality/one}))
(cluster-transact! leader '({person/name "Alice"  person/age 30}))

;; Read from any node (read-committed):
(q '[(find ?n ?a) (where (?e person/name ?n) (?e person/age ?a))]
   (cluster-db leader))
;; => (("Alice" 30))

;; Check status:
(cluster-status leader)
;; => ((node-id . 0) (role . leader) (term . 3) ... (basis-tx . 536870914))

;; Stop all nodes:
(for-each cluster-stop! nodes)
```

#### 6.2 Read Replicas ✅ Complete (in-process)

```scheme
;; Followers maintain independent connections.
;; The apply fiber replicates all committed entries within ~50 ms.

(def followers (filter (lambda (n) (not (cluster-leader? n))) nodes))

;; Queries run against local immutable snapshots:
(q '[(find ?v) (where (?e thing/val ?v))]
   (cluster-db (car followers)))
;; => ((42))

;; Read consistency levels (all implemented):
(cluster-db follower)           ;; read-committed — any node, may lag ≤ 50 ms
(cluster-db/consistent leader)  ;; read-latest — leader only, always current
```

#### 6.3 TCP/TLS Transport ✅ Complete

`transport.ss` bridges each `(std raft)` node's inbox/peer channels to TCP
sockets so nodes can run in separate OS processes or on separate machines.
12/12 integration tests pass (`tests/test-transport.ss`, `make test-transport`).

##### Architecture

Each `raft-node` has two channel roles:
- **Inbox**: the node's message loop reads from its own inbox channel.
- **Peers list**: a list of `(id . channel)` pairs; the node writes outgoing messages to peer channels.

The transport adapter replaces each peer's direct channel with a **proxy channel**:

```
Node A (process 1)                   Node B (process 2)
─────────────────────────────────    ─────────────────────────────────
raft-node-A                          raft-node-B
  peers: [(B . proxy-ch-B)]            peers: [(A . proxy-ch-A)]
  inbox: local-ch-A                    inbox: local-ch-B
         ↕                                    ↕
  outbound fiber:                      outbound fiber:
  read proxy-ch-B → serialize          read proxy-ch-A → serialize
  → write to TCP conn to B →           → write to TCP conn to A →
         ↕   (TLS optional)                   ↕   (TLS optional)
  inbound fiber:                       inbound fiber:
  read TCP conn from B → deserialize   read TCP conn from A → deserialize
  → channel-put local-ch-A            → channel-put local-ch-B
```

The Raft message loop is unchanged — it still reads/writes Scheme channels.
Only the plumbing beneath the channels changes.

##### Wire protocol

```
Frame format (length-prefixed):
  [4 bytes big-endian uint32: body length][body: FASL-encoded message]

Messages (Raft vector messages, as-is from (std raft)):
  #(request-vote term candidate-id last-log-index last-log-term)
  #(vote-response term granted? voter-id)
  #(append-entries term leader-id prev-idx prev-term entries commit-index)
  #(append-response term success? follower-id match-index)
  #(client-propose command reply-channel)   ;; local only, never sent over wire
```

`client-propose` is always local (client → leader via local channel).
Only the four Raft RPC message types cross the wire.

##### Actual Implementation

```scheme
;; lib/jerboa-db/transport.ss

(import (jerboa prelude)
        (rename (only (chezscheme) make-time) (make-time chez-make-time))
        (std net tcp)            ;; tcp-listen, tcp-connect-binary, tcp-accept-binary
        (std fasl)               ;; fasl->bytevector, bytevector->fasl
        (std misc channel)
        (std raft)               ;; raft-node-*, raft-start!, raft-node-add-peer!
        (jerboa-db replication)
        (jerboa-db core)
        (jerboa-db cluster))

;; Convenience: one call creates Raft node + TCP transport + DB connection
(def-values (tnode rconn)
  (start-transport-db-node! 'node-0 '() "/var/db/node-0" 7000))

;; Staged setup (ports only known after start):
(def-values (ta ra) (start-transport-db-node! 'a '() ":memory:" 0))
(def-values (tb rb)
  (start-transport-db-node! 'b `((a "127.0.0.1" ,(transport-node-listen-port ta)))
                            ":memory:" 0))
(transport-node-add-peer! ta 'b "127.0.0.1" (transport-node-listen-port tb))

;; Same cluster API as in-process:
(cluster-transact! ra schema-tx)
(cluster-db ra)
(cluster-status ra)

;; Clean shutdown:
(stop-transport-node! ta)
(stop-transport-node! tb)
```

Key implementation details:
- **Wire protocol:** 4-byte big-endian uint32 length header + FASL body
- **Uni-directional connections:** node A dials B for A→B traffic; node B dials A for B→A traffic
- **Reconnect backoff:** 100ms → 5s (doubles on failure) to handle startup races
- **`client-propose` dropped:** carries a live reply channel; never serialised over wire
- **`raft-node-add-peer!`:** thread-safe; initialises `next-index`/`match-index` if node is already leader (prevents `send-heartbeats!` crash on new peer)

##### TLS (future)

The current TCP transport is plain text — suitable for localhost/loopback
clusters and trusted private networks.  Production cross-datacenter deployments
should add mTLS via `(std net tls)`:

```scheme
;; Mutual TLS (mTLS) for inter-node auth:
(def tls-cfg
  (tls-config-with (default-tls-config)
    :cert "/etc/jerboa-db/node.crt"
    :key  "/etc/jerboa-db/node.key"
    :ca   "/etc/jerboa-db/ca.crt"))

;; Outbound: (tls-connect host port tls-cfg) instead of tcp-connect-binary
;; Inbound:  (tls-accept tcp-server tls-cfg) instead of tcp-accept-binary
```

For development and single-host clusters, plain TCP is fine.

#### 6.4 Peer Client (`peer.ss`) ✅ Functional (HTTP-based)

**File:** `lib/jerboa-db/peer.ss`

The peer library connects to a running `jerboa-db server` over HTTP and
exposes a remote database API.  It is **fully functional** for basic
operations.  It does not yet cache immutable db segments locally (every
operation is a network round-trip).

##### API

```scheme
(import (jerboa-db peer))

;; Connect to a single server:
(def conn (connect-remote "http://localhost:8484"))

;; Connect to a cluster with automatic failover
;; (rotates through URLs on error with exponential backoff):
(def conn (connect-remote* '("http://node-0:8484"
                              "http://node-1:8484"
                              "http://node-2:8484")))

;; Transact — POSTs to /api/transact, returns tx-report alist:
(remote-transact! conn
  '({db/ident person/name  db/valueType db.type/string  db/cardinality db.cardinality/one}))
(remote-transact! conn '({person/name "Alice"  person/age 30}))
;; => ((tx-id . 536870913) (datom-count . 3) (tempids . ...))

;; Query — POSTs to /api/query, results run server-side:
(remote-q conn '((find ?n) (where (?e person/name ?n))))
;; => (("Alice"))

;; Pull — POSTs to /api/pull:
(remote-pull conn '[*] 12345)
;; => ((db/id . 12345) (person/name . "Alice") ...)

;; Cluster status (when server has register-cluster! wired):
;; GET /api/cluster/status → alist of Raft fields
```

##### Failover behavior

`connect-remote*` accepts multiple server URLs.  On any transport or HTTP
error, `with-retry` rotates the URL list (failed primary moves to the end)
and retries with exponential backoff (100 ms, 200 ms, 400 ms, up to 3
attempts).  This means the client automatically follows a Raft leader change
within ~700 ms.

```scheme
;; Under the hood, failover! rotates the URL list:
;; ["node-0" "node-1" "node-2"]
;;   → error on node-0
;; ["node-1" "node-2" "node-0"]   ;; node-1 becomes primary candidate
```

##### What's missing

| Feature | Status | Notes |
|---|---|---|
| Local db-value caching | 🚧 TODO | Every query is a network round-trip; Datomic-style segment caching would let queries run locally |
| WebSocket tx-stream | 🚧 TODO | `remote-tx-stream` stub raises an error; needs `(std net fiber-ws)` in a fiber context |
| Named database support | 🚧 TODO | All ops target the server's "default" db; `/api/db/:name/...` routes not yet used |
| `remote-entity` | 🚧 TODO | Entity API via `GET /api/entity/:eid` |
| `remote-db` as db-value | 🚧 TODO | Currently returns a stats alist; should return a real `db-value` for passing to local `q`/`pull` |

##### Db-value caching (future)

The server needs a `GET /api/db/snapshot` endpoint that returns the full
`db-value` serialized as FASL.  The peer downloads it once and holds it
locally; subsequent `q`/`pull` calls run against the local snapshot.
The WebSocket tx-stream delivers new tx-data so the peer can apply
incremental updates without a full re-download.

```scheme
;; Future: download snapshot once, run queries locally
(def db (remote-snapshot conn))  ;; FASL download of full db-value
(q '[(find ?n) (where (?e person/name ?n))] db)  ;; entirely local
(remote-tx-stream conn (lambda (tx-report)
  (set! db (apply-tx-report db tx-report))))      ;; incremental update
```

#### `(std raft)` Bugs Fixed During Phase 6

Three bugs were found and fixed in `~/mine/jerboa/lib/std/raft.sls` while
building the TCP transport layer.  These affect both in-process and TCP clusters.

| Bug | Symptom | Root cause | Fix |
|---|---|---|---|
| AppendEntries log truncation | Follower always had only the latest single log entry; previous entries were lost | `(< entry-index prev-idx)` instead of `(<= entry-index prev-idx)` in the keep-existing-entries filter — dropped everything with index < prev-idx instead of ≤ prev-idx | Changed `<` to `<=` in AppendEntries handler |
| Synchronised election timers | Leader election failed ~50% of runs with 2+ nodes | `(random n)` in Chez Scheme is not thread-safe; concurrent threads get identical PRNG values → identical election timeouts → perpetual split-vote | `election-timeout-ms` now takes a `node` argument and adds a node-id-derived constant offset, making timeouts deterministically distinct per node |
| `send-heartbeats!` crash on new peers | Raft message loop thread died silently when a peer was added after `become-leader!` | `(cdr (assv peer-id next-index))` = `(cdr #f)` when the peer was absent from `next-index` | Added `raft-node-add-peer!` (thread-safe, initialises `next-index`/`match-index` if leader); added per-peer `guard` + safe `ni` fallback in `send-heartbeats!`; added `guard` around main message loop dispatch |

These fixes are committed to `~/mine/jerboa` (commit c73d576).

**Phase 6 success criteria:**
- ✅ Leader elected within 500ms in 3-node cluster (in-process)
- ✅ `cluster-transact!` commits and returns tx-report (1-node and 3-node)
- ✅ Follower connections converge to leader state within 200ms (in-process)
- ✅ `cluster-status` returns Raft fields + db basis-tx
- ✅ `peer.ss` HTTP client — transact, query, pull with failover (multi-URL)
- ✅ TCP transport for multi-process/multi-host clusters (Phase 6.3) — 12/12 tests pass
- 🚧 TLS wrapping for production inter-node traffic
- 🚧 WebSocket tx-stream in peer client
- 🚧 Local db-value caching in peer client (Datomic-style segment cache)

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
| Aggregate over 10M datoms | < 5s (DuckDB, planned) | DuckDB stub — not yet wired |

### Why These Numbers Are Achievable

- **LevelDB** provides fast reads via LSM tree with bloom filters + 64MB LRU cache
- **FASL encoding** — Chez native binary format, fast deserialization without boxing
- **Chez Scheme** compiles to native code — no JVM interpreter warmup
- **Persistent data structures** share structure — low GC pressure
- **DuckDB** is a world-class columnar engine for analytical queries
- **Fiber-based server** handles 100K connections on one process

---

## The DuckDB Advantage (Planned — Phase 4)

Datomic's biggest weakness is analytical queries.  Aggregating over millions of
datoms requires custom index scans and client-side computation.  XTDB v2
addressed this with Apache Arrow, but it's still JVM-only.

> **Note:** DuckDB integration is planned but not yet wired.  `analytics.ss`
> has the full structure (schema, sync, typed column mapping) but is not connected
> to `core.ss` or any user-facing API.  The capabilities below describe the
> intended final state.

Jerboa-DB's DuckDB integration will provide:

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

This will give Jerboa-DB a **dual-engine architecture**: Datalog for navigational
queries (follow relationships, traverse graphs) and SQL for analytical queries
(aggregate, window, report).  No other Datomic-like system offers both.

---

## What Jerboa-DB Proves About Jerboa

When someone sees Jerboa-DB and asks "could I build this in Go/Rust/Python?",
the honest answer is: not easily.

| Requirement | Go | Rust | Python | Jerboa |
|---|---|---|---|---|
| Datalog query engine | Build from scratch | Build from scratch | DataScript (JS FFI) | `(std datalog)` exists (built own for control) |
| Persistent data structures | None in stdlib | None in stdlib | None in stdlib | pmap, pvec, pset exist |
| MVCC with time-travel | Build from scratch | Build from scratch | Build from scratch | `(std mvcc)` exists (built own db-values) |
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

## Implementation Scorecard (2026-04-13)

> All source lives in `lib/jerboa-db/`.  Core engine built from scratch (no
> `(std mvcc)`, `(std datalog)`, etc. used).  LevelDB FFI via `(std db leveldb)`.
> 52/52 integration tests pass as of 2026-04-13 (34 core + 6 in-process cluster
> + 12 TCP transport).

### Core (Phase 1) — ✅ COMPLETE

Files: `lib/jerboa-db/datom.ss`, `lib/jerboa-db/schema.ss`,
`lib/jerboa-db/index/protocol.ss`, `lib/jerboa-db/index/memory.ss`,
`lib/jerboa-db/tx.ss`, `lib/jerboa-db/entity.ss`,
`lib/jerboa-db/history.ss`, `lib/jerboa-db/cache.ss`, `lib/jerboa-db/core.ss`

| Feature | Status | Notes |
|---|---|---|
| Datom model (E-A-V-T-op) | ✅ Done | 5-tuple record, 4 comparators, sentinel boundaries |
| Four covering indices (EAVT/AEVT/AVET/VAET) | ✅ Done | In-memory RB-tree backend |
| Schema registry | ✅ Done | Intern, lookup, bootstrap attrs, fulltext, tupleAttrs |
| Transaction processing | ✅ Done | Tempid resolution, auto-retract, upsert, CAS, entity retract |
| Current-state resolution | ✅ Done | Groups by (e,a,v), keeps highest-tx, filters retracted |
| Lazy entity maps | ✅ Done | On-demand loading, `touch` for eager materialization |
| Time-travel (as-of, since, history) | ✅ Done | Temporal filters on db-value snapshots |
| LRU cache | ✅ Done | O(1) get/put, hit/miss stats |

### Persistence (Phase 2) — ✅ COMPLETE

Files: `lib/jerboa-db/index/leveldb.ss`, `lib/jerboa-db/encoding.ss`,
`lib/jerboa-db/tx-log.ss`, `lib/jerboa-db/value-store.ss`

| Feature | Status | Notes |
|---|---|---|
| Binary encoding (28-byte keys) | ✅ Done | Big-endian ints, sortable doubles, FNV-1a hashing |
| LevelDB index backend | ✅ Done | 4 separate LevelDB databases via chez-lmdb FFI |
| Transaction log segments | ✅ Done | Append-only FASL segment files |
| Value store (content-addressed) | ✅ Done | FNV-1a keyed dedup for variable-length values |
| Connection close/cleanup | ✅ Done | Closes all 4 LevelDB handles |

### Query Engine (Phase 3) — ✅ COMPLETE

Files: `lib/jerboa-db/query/engine.ss`, `lib/jerboa-db/query/planner.ss`,
`lib/jerboa-db/query/pull.ss`, `lib/jerboa-db/query/functions.ss`,
`lib/jerboa-db/query/rules.ss`, `lib/jerboa-db/query/aggregates.ss`

| Feature | Status | Notes |
|---|---|---|
| Datalog query engine | ✅ Done | Parse Datomic syntax, plan, execute with index selection |
| Clause reordering | ✅ Done | Selectivity scoring, greedy ordering |
| Range predicate pushdown | ✅ Done | `(?e attr ?v) [(cmp ?v const)]` fused into single AVET scan |
| `not` clauses | ✅ Done | Filter out binding sets matching negated patterns |
| `or` clauses | ✅ Done | Union of binding sets from disjunctive branches |
| Collection binding `(?x ...)` in `:in` | ✅ Done | Pass a set, match any member |
| Relation binding `((?x ?y))` in `:in` | ✅ Done | Pass a relation, join against it |
| Tuple binding `(?x ?y)` in `:in` | ✅ Done | Destructure a single tuple |
| Lookup refs in transactions | ✅ Done | `(attr-ident value)` pair resolves via unique attribute |
| Composite tuples | ✅ Done | `db/tupleAttrs` auto-generation |
| Predicates | ✅ Done | `zero?`, `pos?`, `neg?`, `even?`, `odd?`, `starts-with?`, `ends-with?`, `contains?` |
| Functions | ✅ Done | `str`, `subs`, `upper-case`, `lower-case`, `inc`, `dec`, `abs`, `mod`, `ground`, `tuple`, `count` |
| Aggregates | ✅ Done | `count`, `count-distinct`, `sum`, `avg`, `min`, `max`, `median`, `rand`, `sample`, `distinct` |
| Recursive rules | ✅ Done | Fixed-point evaluation, variable renaming |
| Pull API | ✅ Done | Nesting, wildcards, reverse refs, limits, defaults, cycle detection |
| Query explain | ✅ Done | Returns plan without executing |

### Analytics (Phase 4) — ✅ CORE DONE

File: `lib/jerboa-db/analytics.ss`

| Feature | Status | Notes |
|---|---|---|
| DuckDB datom export | ✅ Done | `analytics-export! db-val` — full EAVT scan into `datoms` + `attrs` tables |
| SQL query interface | ✅ Done | `analytics-query ae sql` — returns list of alists |
| Lazy loading | ✅ Done | `ensure-analytics!` in `core.ss` — DuckDB not required for normal operation |
| Parquet export | 🚧 Stub | `export-parquet` present; needs DuckDB `COPY TO` wiring |
| Parquet import | 🚧 Stub | `import-parquet` present; needs DuckDB `COPY FROM` + datom re-ingestion |
| CSV import | 🚧 Stub | `import-csv` present; needs DuckDB `COPY FROM` + datom re-ingestion |

### Server Mode (Phase 5) — ✅ FUNCTIONAL

Files: `lib/jerboa-db/server.ss`

| Feature | Status | Notes |
|---|---|---|
| HTTP REST API | ✅ Done | All routes: transact, query, pull, entity, schema, stats |
| Named database registry | ✅ Done | `register-db! / unregister-db! / lookup-db` |
| Cluster status endpoint | ✅ Done | `GET /api/cluster/status` — Raft status or `{mode: standalone}` |
| WebSocket tx-stream | ✅ Done | `GET /api/tx-stream` — broadcasts tx-reports to all subscribers |
| `register-cluster!` hook | ✅ Done | Wires cluster status thunk to HTTP endpoint |
| Remote peer client | ✅ Done | `peer.ss` — see Phase 6.4 |

### Distribution (Phase 6) — ✅ COMPLETE (in-process + TCP)

Files: `lib/jerboa-db/replication.ss`, `lib/jerboa-db/cluster.ss`,
`lib/jerboa-db/transport.ss`, `tests/test-cluster.ss`, `tests/test-transport.ss`

| Feature | Status | Notes |
|---|---|---|
| Raft node lifecycle | ✅ Done | `start-replication / stop-replication` |
| Local N-node cluster | ✅ Done | `start-local-cluster / start-local-db-cluster` |
| Leader election | ✅ Done | `(std raft)` handles 150–300 ms election |
| Replicated transactions | ✅ Done | `cluster-transact!` — propose → wait → tx-report |
| Follower convergence | ✅ Done | Apply fiber replicates committed entries within 50 ms |
| Read consistency levels | ✅ Done | `read-committed`, `read-latest`, `as-of-tx` |
| Cluster status / introspection | ✅ Done | `cluster-status / cluster-leader?` |
| In-process cluster test suite | ✅ Done | 6/6 tests passing (`tests/test-cluster.ss`) |
| TCP transport test suite | ✅ Done | 12/12 tests passing (`tests/test-transport.ss`) — startup, election, transact, replicate, status, stop |
| TCP transport (plain) | ✅ Done | `transport.ss` — 12/12 tests pass; length-framed FASL, reconnect backoff 100ms→5s, `raft-node-add-peer!` thread-safe |
| TLS wrapping | 🚧 TODO | Replace `tcp-connect-binary`/`tcp-accept-binary` with `(std net tls)` equivalents |
| Remote peer client (HTTP) | ✅ Done | `peer.ss` — connect-remote, remote-transact!, remote-q, remote-pull, multi-URL failover |
| Remote peer — db-value cache | 🚧 TODO | Every op is a network round-trip; Datomic-style local snapshot not yet implemented |
| Remote peer — WebSocket stream | 🚧 TODO | `remote-tx-stream` stub; needs fiber context wiring |

### Polish (Phase 7) — ✅ MOSTLY COMPLETE

Files: `lib/jerboa-db/migrate.ss`, `lib/jerboa-db/backup.ss`,
`lib/jerboa-db/excision.ss`, `lib/jerboa-db/metrics.ss`

| Feature | Status | Notes |
|---|---|---|
| Schema migration | ⚠️ Stub | Skeleton exists |
| Backup/restore | ✅ Done | FASL serialization |
| Excision (GDPR) | ✅ Done | Physical removal from all 4 indices |
| Prometheus metrics | ⚠️ Stub | Skeleton exists |
| Test suite | ✅ Done | 34 integration tests, all passing |

### Advanced Features (Phase 8) — ✅ COMPLETE

Files: `lib/jerboa-db/fulltext.ss`, `lib/jerboa-db/gc.ss`, `lib/jerboa-db/spec.ss`

| Feature | Status | Notes |
|---|---|---|
| Attribute predicates / entity specs | ✅ Done | `define-spec`, `validate-entity`, `check-entity-spec` |
| Composite tuples | ✅ Done | `db/tupleAttrs` auto-generation |
| Fulltext search | ✅ Done | In-memory inverted index, case-insensitive word/substring |
| Datom garbage collection | ✅ Done | Compact retracted `db/noHistory` datoms |

### Query Engine Performance Optimizations

| Optimization | Status | Description | Benefit |
|---|---|---|---|
| AVET for indexed/unique attrs | ✅ Done | `avet-eligible?` — only `db/index #t` or unique attrs use AVET | Prevents incorrect index selection |
| Binding hashtable | ✅ Done | Binding env is an `eq?` hashtable | O(1) variable lookup vs O(n) alist |
| Streaming flatmap | ✅ Done | `evaluate-where-clauses` uses inline flatmap | Avoids intermediate list-of-lists allocation |
| Early termination | ✅ Done | Clause evaluation stops when no bindings survive | Skips remaining clauses on empty result |
| Count short-circuit | ✅ Done | `(count ?x)` with no grouping skips per-row extraction | O(1) vs O(n) |
| Range predicate pushdown | ✅ Done | `(?e attr ?v) [(cmp ?v const)]` fused into AVET range scan | Scans only qualifying value range |
| Schema lookup cache | ✅ Done | Per-transaction `eq?` hashtable `tx-val-cache` in `tx.ss` | Eliminates repeated global hashtable lookups per datom |
| Retraction fast-path | ✅ Done | `resolve-current-datoms` skips hashtable when no retractions | Common append-only case avoids O(n) hashtable build |
| Streaming aggregation | ✅ Done | Single-pass `count`/`sum`/`avg`/`min`/`max` with mutable vector accumulators | Eliminates intermediate list materialization |
| Hash-join | ❌ Planned | O(M+N) join for shared-variable clauses | Would reduce Q4/Q8 from O(M×N) |
| Projection pushdown | ❌ Planned | Reduce binding tuple width early | Cheaper copy-on-write in deep joins |
| Cardinality statistics | ❌ Planned | Per-attribute count estimates for planner | Better clause ordering |

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

## MBrainz Benchmark Results

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

The benchmark uses synthetic data at configurable scale factors (default 1.0 = 262K
artists, 1.31M releases, 13.1M tracks).  Results below are at **1% scale** (2,620
artists, 13,100 releases, 131,000 tracks).

### Benchmark Infrastructure

| Component | Status | Notes |
|---|---|---|
| MBrainz schema definition | ✅ Done | Inline in `benchmarks/mbrainz-bench.ss` — 29 attributes, all entity types |
| Synthetic data loader | ✅ Done | 3-phase flat batching: artists → releases → tracks |
| Benchmark query runner | ✅ Done | 8 standard queries, median timing over 5 runs (1 warmup), `make mbrainz-quick` |

### Benchmark Results (1% scale — 2026-04-13)

```
Scale: 1% (2,620 artists, 13,100 releases, 131,000 tracks)

Query                                                      Median         Result
--------------------------------------------------------------------------------
Q1: Artist exact name lookup                                0 ms        42 rows
Q2: Releases by artist name                                 0 ms       210 rows
Q3: Artists with startYear < 1960                           2 ms      1173 rows
Q4: Tracks > 240s on shared-artist releases             ~2468 ms    428,096 rows
Q5: Count releases per country                              4 ms        10 rows
Q6: All releases for one artist (reverse ref)               0 ms         5 rows
Q7: Pull artist attributes                                  0 ms         5 rows
Q8: Avg track duration by release status                ~2056 ms         3 rows
--------------------------------------------------------------------------------
Total                                                   ~4500 ms
```

### Analysis

**Fast queries (Q1–Q3, Q5–Q7):** 0–4ms.  All use index-optimized paths:
- Q1: AVET point lookup by `artist/name` (exact match)
- Q2: AVET → AEVT join (anchor on artist, scan releases)
- Q3: AVET range scan with predicate pushdown (`startYear < 1960`)
- Q5: AEVT full-attribute scan with streaming `count` aggregation
- Q6: AEVT scan on `release/artists` (join on entity)
- Q7: EAVT point lookup + pull pattern evaluation

**Q4 (~2468ms):** Bottleneck is **join explosion**, not algorithms.  The query
asks for tracks with duration > 240s that share an artist with any release.  At 1%
scale this produces 428,096 output rows — a Cartesian product of tracks × matching
releases per shared artist.  The join itself is correctly using AEVT index scans;
the cost is proportional to the output size.  Hash-join or semi-join pushdown would
reduce intermediate binding count but cannot eliminate the 428K result size.

**Q8 (~2056ms):** Bottleneck is **intermediate binding count**.  The query joins
tracks → artists → releases to get release status, then groups by status and
aggregates.  At 1% scale this produces 655K intermediate `(track, artist, release)`
bindings before aggregation.  Streaming aggregation (single-pass with mutable
vector accumulators) is implemented and working — the cost is the binding
generation, not the aggregation step itself.

### Optimizations Applied (all committed)

| Optimization | Implementation | Effect |
|---|---|---|
| `avet-eligible?` predicate | Only attributes with `db/index #t` or `db/unique` use the AVET index | Prevents incorrect index selection |
| Planner selectivity scoring | Score 1000 when all variables in a clause are bound | Enables AVET range pushdown for Q4 |
| `tx-val-cache` in `tx.ss` | Per-transaction `eq?` hashtable keyed on `eid*256+aid` | Avoids EAVT scan during transact; amortizes schema lookups |
| Streaming aggregation | Single-pass `count`/`sum`/`avg`/`min`/`max` with mutable vector accumulators | Eliminates intermediate list materialization for aggregates |

### Remaining Performance Opportunities

| Opportunity | Target Queries | Expected Gain |
|---|---|---|
| Projection pushdown | Q4, Q8 | Reduce binding tuple width → cheaper copy-on-write |
| Hash-join (O(M+N) instead of O(M×N)) | Q4, Q8 | Eliminate nested-loop join for shared-variable joins |
| Cardinality statistics | All | Better planner ordering (avoid anchoring on low-selectivity clauses) |
| Bulk-index load path | Load time | Defer RB-tree sort until end of load phase; currently ~120s at 1% scale |
| Real MBrainz EDN loader | Full scale | Parse actual Datahike EDN files instead of synthetic data |

### Comparison vs Datahike (published numbers, full scale)

Datahike publishes MBrainz results at **full scale** (262K artists, 1.31M
releases, 13.1M tracks).  Our numbers are at **1% scale** (2,620 artists,
13,100 releases, 131,000 tracks) — a 100× smaller dataset.

| Query | Datahike full scale | Jerboa-DB 1% scale | Scaled projection (×100) | Gap |
|---|---|---|---|---|
| Q1: exact name lookup | ~1ms | 0ms | <1ms | **None** — AVET lookup |
| Q2: releases by artist | ~1ms | 0ms | <1ms | **None** — two-hop AVET/AEVT |
| Q3: range predicate (startYear) | ~5ms | 2ms | ~200ms | **Moderate** — AVET range, degrades with scale |
| Q4: tracks × shared-artist releases | ~2–8s | 2468ms | **~250s** | **Critical** — output is 428K rows at 1%, ~43M at full |
| Q5: count releases per country | ~10ms | 4ms | ~400ms | **Moderate** — full AEVT scan |
| Q6: reverse ref lookup | ~1ms | 0ms | <1ms | **None** |
| Q7: pull | ~1ms | 0ms | <1ms | **None** |
| Q8: avg duration by status | ~500ms–2s | 2056ms | **~205s** | **Critical** — 655K intermediate bindings at 1% |

> Datahike reference: https://github.com/replikativ/datahike/tree/main/bench
> Note: Datahike uses Hitchhiker tree (persistent B+ tree) + hash-join; numbers from
> their CI benchmark reports circa 2023.  XTDB v2 (Apache Arrow + Parquet columnar
> execution) performs similarly to Datahike on Q1–Q3 and 2–5× faster on Q4/Q8.

**The fast queries (Q1, Q2, Q6, Q7) are already competitive.**  The critical gaps
are Q4 and Q8 — multi-hop joins that produce large intermediate result sets.

### Write Throughput Degradation

The load test measures write throughput at small scale (~5K entities) where the
in-memory RB-tree is shallow.  MBrainz data loading reveals the degradation curve:

| Phase | Entities | Observed throughput | Expected (flat) |
|---|---|---|---|
| Load test quick (batch=1) | ~5K total | ~154K ops/sec | baseline |
| Load test quick (batch=100) | ~5K total | ~184K ops/sec | baseline |
| MBrainz 1% data load | ~147K total | ~1,300 entities/sec | ~154K |

**The effective throughput drops ~120× from 5K to 147K entities.**

Root cause: the in-memory index is a persistent RB-tree (functional, immutable).
Each `transact!` creates a new tree root that shares structure with the previous
version.  The tree depth grows as O(log N), meaning each insert performs O(log N)
allocations.  At 147K entities × ~10 datoms × 4 indices = ~5.9M datom insertions,
the tree is ~23 levels deep.  Combined with GC pressure from structural sharing,
throughput degrades geometrically.

Datahike uses the **Hitchhiker tree** — a write-optimised persistent B+ tree that
batches writes in tree-local buffers, amortising structural allocation.  Datomic
uses a similar segment-based approach with a dedicated background indexer process.

### Performance Gap Analysis and Closing the Gaps

#### Gap 1: Q4/Q8 Join Explosion (Critical)

**Root cause:** The query engine executes clauses left-to-right (after a greedy
selectivity reorder).  For Q4, the first anchoring clause binds all 131K tracks
before any filtering.  There is no hash-join: every `(track, artist, release)`
triple is materialised as a binding-environment copy.

**What Datahike does:** Hitchhiker tree stores cardinality statistics per attribute.
The planner uses these to choose the smallest-cardinality anchor.  Binary joins are
executed as hash-joins (O(M+N) instead of O(M×N)).

**Implementation plan to close the gap:**

1. **Hash-join for shared-variable binary joins** (highest impact, ~2–10× speedup
   on Q4/Q8, required for full-scale viability):
   ```
   ;; When two clauses share a variable ?x, instead of nested-loop:
   ;;   (for each binding-A (for each binding-B (if (= ?x-A ?x-B) emit)))
   ;; use hash-join:
   ;;   (build hash-table: ?x → [bindings-A])
   ;;   (for each binding-B: lookup ?x-B in hash-table, emit cross-product)
   ```
   Required changes: `query/planner.ss` (detect shared variables), `query/engine.ss`
   (new `execute-hash-join` path), `query/engine.ss` (replace inner flatmap loop).

2. **Cardinality statistics** (planner quality; enables better anchor selection):
   Maintain per-attribute datom count in a dedicated `:db/stats` attribute updated
   at `transact!` time.  The planner scores clauses by estimated output size instead
   of structural selectivity.  Required changes: `schema.ss` (stats counters),
   `tx.ss` (increment on add, decrement on retract), `query/planner.ss` (cost model).

3. **Semi-join pushdown** (reduces output size for Q4 without needing full result):
   If the query only needs `(count ?x)` or a projection, a semi-join (keep-any-match)
   avoids materialising the full Cartesian product.  This is most impactful for Q4's
   428K-row output at 1% scale.

4. **Selectivity-based clause reordering with bound-variable propagation:**
   Currently the planner reorders once at parse time.  The planner should re-evaluate
   clause cost after each clause executes, using actual intermediate result size.
   This is a partial re-evaluation strategy (like Volcano/IteratorModel lazy pull).

#### Gap 2: Write Throughput Degradation (High Impact at Scale)

**Root cause:** Persistent functional RB-tree deepens O(log N) per insert; no
batching or background indexing.

**Implementation options (easiest first):**

1. **Batch-index path during bulk load** (quick win, ~5–10× improvement):
   Collect all datoms in a transaction as a vector, sort once, and bulk-insert into
   the RB-tree using a merge-sort approach rather than one-by-one splay insertions.
   Required changes: `index/memory.ss` (new `index-bulk-add!`), `tx.ss` (use bulk
   path when batch size > threshold).

2. **Write-ahead log + lazy index merge** (structural fix, complex):
   Keep a flat transaction log as the source of truth.  Build the in-memory index
   lazily (or in a background thread) by merging sorted log segments.  This is
   similar to LevelDB's own LSM approach and would give near-constant write throughput
   at any scale.  Required: new `index/wal.ss`, significant refactor of `core.ss`.

3. **Persistent balanced BST with batch operations** (medium effort):
   Replace the purely functional RB-tree with a persistent B-tree that supports
   bulk-load (sorted input → balanced tree in O(N) rather than O(N log N)).  The
   `(std data sorted-map)` module may support this; verify with `jerboa_module_exports`.

**Target after fix 1 alone:** ~20K entities/sec at 147K scale (15× improvement),
which would reduce MBrainz 1% load time from 112s to ~7s.

#### Gap 3: Full-Scale MBrainz Feasibility

At full scale (262K artists, 13.1M tracks) with the current engine:
- Write throughput: ~1,300 ents/sec → estimated **3+ hours** to load
- Q4: extrapolates to **~250 seconds** per query run
- Q8: extrapolates to **~200 seconds** per query run

**Minimum required for full-scale viability:**
- Hash-join (Gap 1, item 1) — reduces Q4 from O(N²) to O(N), enabling <10s
- Batch-index bulk load (Gap 2, item 1) — reduces load time from hours to ~30min
- Cardinality stats (Gap 1, item 2) — prevents worst-case anchor selection

**DuckDB path (alternative for analytics):** For Q4/Q8-class aggregation queries,
routing them through the DuckDB analytics layer (Phase 4) would bypass the Datalog
engine entirely.  At that point the bottleneck becomes DuckDB sync latency, not
join ordering.  This is a valid production strategy ("Datalog for navigation, SQL
for analytics").

### Data Loading Strategy (Full Scale)

MBrainz data is distributed as EDN transaction files (Datomic format).

1. Parse EDN with Jerboa's EDN reader (Datomic EDN uses `:keyword/attr` style → map to `symbol/attr`)
2. Map Datomic-style tempids (`#db/id[:db.part/user -1000000]`) to Jerboa tempids
3. Batch transactions (1000 entities per tx) for throughput
4. Schema bootstrap transaction first, then entity data
5. **Target:** load 6.6M entities in < 5 min (in-memory), < 15 min (LevelDB)

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
