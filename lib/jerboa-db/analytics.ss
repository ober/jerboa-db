#!chezscheme
;;; (jerboa-db analytics) — DuckDB integration for OLAP queries
;;;
;;; Maintains a columnar replica of the datom store for analytical queries.
;;; Provides SQL over datoms, Parquet export/import, and CSV import.
;;;
;;; Usage:
;;;   (def ae (new-analytics-engine schema-reg index-set))
;;;   (analytics-sync! ae db-value)          ;; load/refresh from db snapshot
;;;   (analytics-query ae "SELECT ...")       ;; returns list of alists
;;;   (export-parquet ae "/tmp/out.parquet")
;;;   (analytics-close ae)
;;;
;;; The engine exposes two tables in DuckDB:
;;;   datoms(e, a, a_name, v_long, v_double, v_string, v_bool,
;;;          v_ref, v_instant, tx, added)
;;;   attrs(id, ident, value_type, cardinality)

(library (jerboa-db analytics)
  (export
    new-analytics-engine analytics-engine?
    analytics-sync! analytics-query
    export-parquet import-parquet
    export-csv     import-csv
    analytics-close)

  (import (except (chezscheme)
                  make-hash-table hash-table?
                  sort sort!
                  printf fprintf
                  path-extension path-absolute?
                  with-input-from-string with-output-to-string
                  iota 1+ 1-
                  partition
                  make-date make-time
                atom? meta)
          (jerboa prelude)
          (std db duckdb)
          (jerboa-db datom)
          (jerboa-db schema)
          (jerboa-db history)
          (jerboa-db index protocol))

  ;; ---- Analytics engine record ----

  (defstruct analytics-engine
    (duckdb-conn     ;; DuckDB connection handle (integer)
     last-synced-tx  ;; last tx whose datoms have been loaded (integer)
     schema-ref      ;; schema-registry reference
     synced?))       ;; boolean: has initial sync been done?

  ;; ---- Constructor ----

  (def (new-analytics-engine schema . opts)
    ;; Optional: path for persistent DuckDB file (default: in-memory)
    (let* ([path (if (pair? opts) (car opts) ":memory:")]
           [conn (duckdb-open path)]
           [ae   (make-analytics-engine conn 0 schema #f)])
      (create-tables! ae)
      ae))

  ;; ---- Schema setup ----

  (def (create-tables! ae)
    (let ([conn (analytics-engine-duckdb-conn ae)])
      (duckdb-exec conn
        "CREATE TABLE IF NOT EXISTS datoms (
           e         BIGINT   NOT NULL,
           a         INTEGER  NOT NULL,
           a_name    VARCHAR,
           v_long    BIGINT,
           v_double  DOUBLE,
           v_string  VARCHAR,
           v_bool    BOOLEAN,
           v_ref     BIGINT,
           v_instant BIGINT,
           tx        BIGINT   NOT NULL,
           added     BOOLEAN  NOT NULL
         )")
      (duckdb-exec conn
        "CREATE TABLE IF NOT EXISTS attrs (
           id          INTEGER PRIMARY KEY,
           ident       VARCHAR NOT NULL,
           value_type  VARCHAR,
           cardinality VARCHAR
         )")))

  ;; ---- Sync from a db-value snapshot ----
  ;;
  ;; Reads all datoms from the EAVT index and (re)loads them into DuckDB.
  ;; For simplicity this does a full reload each time (truncate + insert).
  ;; An incremental variant would track last-synced-tx and only insert new
  ;; datoms — feasible but requires filtering by tx, so we keep full reload
  ;; for correctness.

  (def (analytics-sync! ae db-val)
    (let* ([schema  (analytics-engine-schema-ref ae)]
           [indices (db-value-indices db-val)]
           [eavt    (index-set-eavt indices)]
           [datoms  (dbi-datoms eavt)]
           [conn    (analytics-engine-duckdb-conn ae)])
      ;; Truncate existing data (full refresh semantics)
      (duckdb-exec conn "DELETE FROM datoms")
      (duckdb-exec conn "DELETE FROM attrs")
      ;; Load attributes
      (for-each
        (lambda (attr)
          (duckdb-eval conn
            "INSERT INTO attrs (id, ident, value_type, cardinality)
             VALUES (?, ?, ?, ?)
             ON CONFLICT (id) DO NOTHING"
            (db-attribute-id attr)
            (symbol->string (db-attribute-ident attr))
            (let ([vt (db-attribute-value-type attr)])
              (if vt (symbol->string vt) #f))
            (let ([c (db-attribute-cardinality attr)])
              (if c (symbol->string c) #f))))
        (schema-all-attributes schema))
      ;; Load datoms
      (for-each
        (lambda (d)
          (insert-datom-row! ae schema d))
        datoms)
      ;; Track sync state
      (analytics-engine-last-synced-tx-set! ae (db-value-basis-tx db-val))
      (analytics-engine-synced?-set! ae #t)))

  ;; ---- Insert a single datom record ----

  (def (insert-datom-row! ae schema d)
    ;; d may be a datom record or a serializable vector #(e a v tx added?)
    (let-values ([(e a v tx added?)
                  (if (datom? d)
                      (values (datom-e d) (datom-a d) (datom-v d)
                              (datom-tx d) (datom-added? d))
                      (values (vector-ref d 0) (vector-ref d 1) (vector-ref d 2)
                              (vector-ref d 3) (vector-ref d 4)))])
      (let* ([attr   (schema-lookup-by-id schema a)]
             [a-name (if attr (symbol->string (db-attribute-ident attr)) #f)]
             [vtype  (if attr (db-attribute-value-type attr) #f)])
        (let-values ([(v-long v-double v-string v-bool v-ref v-instant)
                      (classify-value v vtype)])
          (duckdb-eval
            (analytics-engine-duckdb-conn ae)
            "INSERT INTO datoms
               (e, a, a_name, v_long, v_double, v_string, v_bool, v_ref, v_instant, tx, added)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            e a a-name
            v-long v-double v-string v-bool v-ref v-instant
            tx added?)))))

  ;; Map a datom value + schema type to the six typed column slots.
  ;; Returns six values: (v-long v-double v-string v-bool v-ref v-instant).
  ;; Exactly one will be non-#f (or all #f for unknown types).
  (def (classify-value v vtype)
    (cond
      ;; Explicit schema type wins
      [(eq? vtype 'db.type/long)
       (values (and (integer? v) v) #f #f #f #f #f)]
      [(eq? vtype 'db.type/double)
       (values #f (and (number? v) (inexact v)) #f #f #f #f)]
      [(eq? vtype 'db.type/string)
       (values #f #f (and (string? v) v) #f #f #f)]
      [(eq? vtype 'db.type/boolean)
       (values #f #f #f (and (boolean? v) v) #f #f)]
      [(eq? vtype 'db.type/ref)
       (values #f #f #f #f (and (integer? v) v) #f)]
      [(eq? vtype 'db.type/instant)
       (values #f #f #f #f #f (and (integer? v) v))]
      [(eq? vtype 'db.type/keyword)
       ;; Store keyword/symbol as string
       (values #f #f (and (symbol? v) (symbol->string v)) #f #f #f)]
      [(eq? vtype 'db.type/uuid)
       (values #f #f (and (string? v) v) #f #f #f)]
      [(eq? vtype 'db.type/symbol)
       (values #f #f (and (symbol? v) (symbol->string v)) #f #f #f)]
      [(eq? vtype 'db.type/any)
       ;; Infer from Scheme type
       (cond
         [(boolean? v)  (values #f #f #f v #f #f)]
         [(flonum? v)   (values #f v #f #f #f #f)]
         [(integer? v)  (values v #f #f #f #f #f)]
         [(string? v)   (values #f #f v #f #f #f)]
         [(symbol? v)   (values #f #f (symbol->string v) #f #f #f)]
         [else          (values #f #f (format "~s" v) #f #f #f)])]
      ;; No schema type or unrecognized — infer from Scheme type
      [(boolean? v)   (values #f #f #f v #f #f)]
      [(flonum? v)    (values #f v #f #f #f #f)]
      [(integer? v)   (values v #f #f #f #f #f)]
      [(string? v)    (values #f #f v #f #f #f)]
      [(symbol? v)    (values #f #f (symbol->string v) #f #f #f)]
      [else           (values #f #f (format "~s" v) #f #f #f)]))

  ;; ---- SQL query ----
  ;;
  ;; Returns a list of alists, e.g.:
  ;;   (("e" . 100) ("a_name" . "person/name") ("v_string" . "Alice"))

  (def (analytics-query ae sql-string . params)
    (let ([conn (analytics-engine-duckdb-conn ae)])
      (apply duckdb-query conn sql-string params)))

  ;; ---- Parquet export ----

  (def (export-parquet ae path . opts)
    ;; opts: optional SQL override (default: full datoms table)
    (let ([sql (if (pair? opts)
                   (car opts)
                   "SELECT * FROM datoms")])
      (duckdb-write-parquet (analytics-engine-duckdb-conn ae) sql path)))

  ;; ---- CSV export ----

  (def (export-csv ae path . opts)
    ;; opts: optional SQL override (default: full datoms table)
    (let ([sql (if (pair? opts)
                   (car opts)
                   "SELECT * FROM datoms")])
      (duckdb-write-csv (analytics-engine-duckdb-conn ae) sql path)))

  ;; ---- Parquet import ----
  ;;
  ;; mapping: alist of (column-name . attribute-ident-symbol)
  ;; Each row becomes a new entity assertion.

  (def (import-parquet ae path mapping)
    (let ([conn   (analytics-engine-duckdb-conn ae)]
          [schema (analytics-engine-schema-ref ae)])
      ;; Use DuckDB to read the parquet and materialise it in-memory
      (let ([rows (duckdb-read-parquet conn path)])
        (for-each
          (lambda (row)
            (let ([eid (next-import-eid ae)])
              (for-each
                (lambda (col-mapping)
                  (let* ([col-name   (car col-mapping)]
                         [attr-ident (cdr col-mapping)]
                         [raw-val    (cdr (or (assoc col-name row) (cons col-name #f)))]
                         [attr       (schema-lookup-by-ident schema attr-ident)]
                         [a-id       (if attr (db-attribute-id attr) #f)])
                    (when (and raw-val a-id)
                      (let* ([vtype (if attr (db-attribute-value-type attr) #f)]
                             [a-name (symbol->string attr-ident)])
                        (let-values ([(v-long v-double v-string v-bool v-ref v-instant)
                                      (classify-value raw-val vtype)])
                          (duckdb-eval conn
                            "INSERT INTO datoms
                               (e, a, a_name, v_long, v_double, v_string,
                                v_bool, v_ref, v_instant, tx, added)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                            eid a-id a-name
                            v-long v-double v-string v-bool v-ref v-instant
                            0 #t))))))
                mapping)))
          rows))))

  ;; ---- CSV import ----
  ;;
  ;; Same semantics as import-parquet — reads CSV via DuckDB's auto-detect,
  ;; then maps columns to attributes.

  (def (import-csv ae path mapping)
    (let ([conn   (analytics-engine-duckdb-conn ae)]
          [schema (analytics-engine-schema-ref ae)])
      (let ([rows (duckdb-read-csv conn path)])
        (for-each
          (lambda (row)
            (let ([eid (next-import-eid ae)])
              (for-each
                (lambda (col-mapping)
                  (let* ([col-name   (car col-mapping)]
                         [attr-ident (cdr col-mapping)]
                         [raw-val    (cdr (or (assoc col-name row) (cons col-name #f)))]
                         [attr       (schema-lookup-by-ident schema attr-ident)]
                         [a-id       (if attr (db-attribute-id attr) #f)])
                    (when (and raw-val a-id)
                      (let* ([vtype  (if attr (db-attribute-value-type attr) #f)]
                             [a-name (symbol->string attr-ident)])
                        (let-values ([(v-long v-double v-string v-bool v-ref v-instant)
                                      (classify-value raw-val vtype)])
                          (duckdb-eval conn
                            "INSERT INTO datoms
                               (e, a, a_name, v_long, v_double, v_string,
                                v_bool, v_ref, v_instant, tx, added)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                            eid a-id a-name
                            v-long v-double v-string v-bool v-ref v-instant
                            0 #t))))))
                mapping)))
          rows))))

  ;; ---- Close ----

  (def (analytics-close ae)
    (duckdb-close (analytics-engine-duckdb-conn ae))
    (analytics-engine-duckdb-conn-set! ae #f))

  ;; ---- Internal helpers ----

  ;; Monotonic counter for import entity IDs.
  ;; Starts well above any normal entity range so imports don't collide.
  (def *import-eid-counter* (expt 2 48))

  (def (next-import-eid ae)
    (let ([eid *import-eid-counter*])
      (set! *import-eid-counter* (+ *import-eid-counter* 1))
      eid))

) ;; end library
