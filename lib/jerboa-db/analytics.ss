#!chezscheme
;;; (jerboa-db analytics) — DuckDB integration for OLAP queries
;;;
;;; Maintains a columnar replica of the datom store for analytical queries.
;;; Provides SQL over datoms, Parquet export/import, and CSV import.

(library (jerboa-db analytics)
  (export
    new-analytics-engine analytics-engine?
    analytics-sync! analytics-query
    export-parquet import-parquet import-csv
    analytics-close)

  (import (except (chezscheme)
                  make-hash-table hash-table?
                  sort sort!
                  printf fprintf
                  path-extension path-absolute?
                  with-input-from-string with-output-to-string
                  iota 1+ 1-
                  partition
                  make-date make-time)
          (jerboa prelude)
          (std db duckdb)
          (jerboa-db datom)
          (jerboa-db schema)
          (jerboa-db tx-log))

  ;; ---- Analytics engine record ----

  (defstruct analytics-engine
    (duckdb-conn     ;; DuckDB connection handle (integer)
     last-synced-tx  ;; last tx synced to DuckDB
     schema-ref      ;; reference to schema registry
     tx-log-ref))    ;; reference to transaction log

  (def (new-analytics-engine schema tx-log . opts)
    ;; Optional: path for persistent DuckDB file
    (let ([path (if (pair? opts) (car opts) ":memory:")])
      (let ([conn (duckdb-open path)])
        (let ([ae (make-analytics-engine conn 0 schema tx-log)])
          (create-datom-table! ae)
          ae))))

  ;; ---- Schema setup ----

  (def (create-datom-table! ae)
    (duckdb-exec (analytics-engine-duckdb-conn ae)
      "CREATE TABLE IF NOT EXISTS datoms (
         e       BIGINT   NOT NULL,
         a       INTEGER  NOT NULL,
         a_name  VARCHAR,
         v_long    BIGINT,
         v_double  DOUBLE,
         v_string  VARCHAR,
         v_bool    BOOLEAN,
         v_ref     BIGINT,
         v_instant BIGINT,
         tx      BIGINT   NOT NULL,
         added   BOOLEAN  NOT NULL
       )"))

  ;; ---- Sync from transaction log ----

  (def (analytics-sync! ae)
    ;; Read transaction log entries since last-synced-tx and INSERT datoms.
    (let* ([log    (analytics-engine-tx-log-ref ae)]
           [schema (analytics-engine-schema-ref ae)]
           [last-tx (analytics-engine-last-synced-tx ae)]
           [entries (tx-log-range log last-tx (+ (tx-log-latest-tx log) 1))])
      (for-each
        (lambda (entry)
          (for-each
            (lambda (dv)
              ;; dv is #(e a v tx added?) — serializable form from tx-log
              (insert-datom-row! ae schema dv))
            (tx-log-entry-datoms entry)))
        entries)
      (when (pair? entries)
        (analytics-engine-last-synced-tx-set! ae
          (tx-log-entry-tx-id (car (reverse entries)))))))

  (def (insert-datom-row! ae schema dv)
    ;; dv is a vector: #(e a v tx added?)
    (let* ([e      (vector-ref dv 0)]
           [a      (vector-ref dv 1)]
           [v      (vector-ref dv 2)]
           [tx     (vector-ref dv 3)]
           [added? (vector-ref dv 4)]
           [attr   (schema-lookup-by-id schema a)]
           [a-name (if attr (symbol->string (db-attribute-ident attr)) #f)]
           [vtype  (if attr (db-attribute-value-type attr) #f)])
      ;; Resolve each typed slot — only one should be non-NULL per row.
      (let-values ([(v-long v-double v-string v-bool v-ref v-instant)
                    (classify-value v vtype)])
        (duckdb-eval
          (analytics-engine-duckdb-conn ae)
          "INSERT INTO datoms
             (e, a, a_name, v_long, v_double, v_string, v_bool, v_ref, v_instant, tx, added)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
          e a a-name
          v-long v-double v-string v-bool v-ref v-instant
          tx added?))))

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
      ;; No schema — infer from Scheme type
      [(boolean? v)   (values #f #f #f v #f #f)]
      [(flonum? v)    (values #f v #f #f #f #f)]
      [(integer? v)   (values v #f #f #f #f #f)]
      [(string? v)    (values #f #f v #f #f #f)]
      [(symbol? v)    (values #f #f (symbol->string v) #f #f #f)]
      [else           (values #f #f (format "~s" v) #f #f #f)]))

  ;; ---- SQL query ----

  (def (analytics-query ae sql-string . params)
    ;; Sync first so the view is up-to-date, then run SQL.
    (analytics-sync! ae)
    (apply duckdb-query (analytics-engine-duckdb-conn ae) sql-string params))

  ;; ---- Parquet export ----

  (def (export-parquet ae path . opts)
    ;; opts: optional SQL override (default: full datoms table)
    (analytics-sync! ae)
    (let ([sql (if (pair? opts)
                   (car opts)
                   "SELECT * FROM datoms")])
      (duckdb-write-parquet (analytics-engine-duckdb-conn ae) sql path)))

  ;; ---- Parquet import ----
  ;;
  ;; mapping: alist of (column-name . attribute-ident)
  ;; Each row becomes a new entity assertion. All values imported as strings;
  ;; callers can add type coercion via the schema after import.

  (def (import-parquet ae path mapping)
    (let ([conn (analytics-engine-duckdb-conn ae)]
          [schema (analytics-engine-schema-ref ae)]
          [tx-id  (+ (tx-log-latest-tx (analytics-engine-tx-log-ref ae)) 1)])
      ;; Use DuckDB to read the parquet and materialise it in-memory
      (let ([rows (duckdb-read-parquet conn path)])
        (for-each
          (lambda (row)
            ;; Allocate a fresh entity id (use a stable hash of row position
            ;; relative to tx so re-import is idempotent-ish)
            (let ([eid (next-import-eid ae)])
              (for-each
                (lambda (col-mapping)
                  (let* ([col-name (car col-mapping)]
                         [attr-ident (cdr col-mapping)]
                         [raw-val  (cdr (or (assoc col-name row) '(#f . #f)))]
                         [attr     (schema-lookup-by-ident schema attr-ident)]
                         [a-id     (if attr (db-attribute-id attr) #f)])
                    (when (and raw-val a-id)
                      (let ([vtype (if attr (db-attribute-value-type attr) #f)])
                        (let-values ([(v-long v-double v-string v-bool v-ref v-instant)
                                      (classify-value raw-val vtype)])
                          (duckdb-eval conn
                            "INSERT INTO datoms
                               (e, a, a_name, v_long, v_double, v_string,
                                v_bool, v_ref, v_instant, tx, added)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                            eid a-id
                            (if attr (symbol->string (db-attribute-ident attr)) #f)
                            v-long v-double v-string v-bool v-ref v-instant
                            tx-id #t))))))
                mapping)))
          rows))))

  ;; ---- CSV import ----
  ;;
  ;; Same semantics as import-parquet — reads CSV via DuckDB's auto-detect,
  ;; then maps columns to attributes.

  (def (import-csv ae path mapping)
    (let ([conn   (analytics-engine-duckdb-conn ae)]
          [schema (analytics-engine-schema-ref ae)]
          [tx-id  (+ (tx-log-latest-tx (analytics-engine-tx-log-ref ae)) 1)])
      (let ([rows (duckdb-read-csv conn path)])
        (for-each
          (lambda (row)
            (let ([eid (next-import-eid ae)])
              (for-each
                (lambda (col-mapping)
                  (let* ([col-name  (car col-mapping)]
                         [attr-ident (cdr col-mapping)]
                         [raw-val   (cdr (or (assoc col-name row) '(#f . #f)))]
                         [attr      (schema-lookup-by-ident schema attr-ident)]
                         [a-id      (if attr (db-attribute-id attr) #f)])
                    (when (and raw-val a-id)
                      (let ([vtype (if attr (db-attribute-value-type attr) #f)])
                        (let-values ([(v-long v-double v-string v-bool v-ref v-instant)
                                      (classify-value raw-val vtype)])
                          (duckdb-eval conn
                            "INSERT INTO datoms
                               (e, a, a_name, v_long, v_double, v_string,
                                v_bool, v_ref, v_instant, tx, added)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                            eid a-id
                            (if attr (symbol->string (db-attribute-ident attr)) #f)
                            v-long v-double v-string v-bool v-ref v-instant
                            tx-id #t))))))
                mapping)))
          rows))))

  ;; ---- Close ----

  (def (analytics-close ae)
    (duckdb-close (analytics-engine-duckdb-conn ae))
    (analytics-engine-duckdb-conn-set! ae #f))

  ;; ---- Internal helpers ----

  ;; Simple monotonic counter for import entity IDs.
  ;; Starts well above any normal entity range so imports don't collide.
  (def *import-eid-counter* (expt 2 48))

  (def (next-import-eid ae)
    (let ([eid *import-eid-counter*])
      (set! *import-eid-counter* (+ *import-eid-counter* 1))
      eid))

) ;; end library
