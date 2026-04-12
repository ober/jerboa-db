#!chezscheme
;;; (jerboa-db analytics) — DuckDB integration for OLAP queries
;;;
;;; Maintains a columnar replica of the datom store for analytical queries.
;;; Provides SQL over datoms, Parquet export/import, and async sync.

(library (jerboa-db analytics)
  (export
    new-analytics-engine analytics-engine?
    analytics-sync! analytics-query
    export-parquet import-parquet import-csv)

  (import (chezscheme)
          (jerboa-db datom)
          (jerboa-db schema)
          (jerboa-db tx-log))

  ;; ---- Analytics engine record ----

  (define-record-type analytics-engine
    (fields (mutable duckdb-conn)     ;; DuckDB connection handle
            (mutable last-synced-tx)  ;; last tx synced to DuckDB
            schema-ref                ;; reference to schema registry
            tx-log-ref))              ;; reference to transaction log

  (define (new-analytics-engine schema tx-log . opts)
    ;; Optional: path for persistent DuckDB file
    (let ([path (if (pair? opts) (car opts) ":memory:")])
      (let ([ae (make-analytics-engine
                  (init-duckdb path) 0 schema tx-log)])
        (create-datom-table! ae)
        ae)))

  ;; ---- DuckDB initialization ----
  ;; Uses (std db duckdb) when available; stubs for compilation.

  (define (init-duckdb path)
    ;; Placeholder: actual DuckDB connection via (std db duckdb)
    ;; Returns an opaque connection handle
    (list 'duckdb-conn path))

  (define (create-datom-table! ae)
    ;; CREATE TABLE datoms (
    ;;   e BIGINT, a INTEGER, v VARCHAR,
    ;;   v_long BIGINT, v_double DOUBLE, v_bool BOOLEAN,
    ;;   v_instant TIMESTAMP, v_ref BIGINT,
    ;;   tx BIGINT, added BOOLEAN
    ;; );
    (duckdb-exec! (analytics-engine-duckdb-conn ae)
      "CREATE TABLE IF NOT EXISTS datoms (
         e BIGINT, a INTEGER, a_name VARCHAR,
         v VARCHAR, v_long BIGINT, v_double DOUBLE,
         v_bool BOOLEAN, v_instant BIGINT, v_ref BIGINT,
         tx BIGINT, added BOOLEAN)"))

  ;; ---- Sync from transaction log ----

  (define (analytics-sync! ae)
    ;; Read transaction log entries since last-synced-tx
    ;; Batch-insert datoms into DuckDB
    (let* ([log (analytics-engine-tx-log-ref ae)]
           [schema (analytics-engine-schema-ref ae)]
           [last-tx (analytics-engine-last-synced-tx ae)]
           [entries (tx-log-range log last-tx (+ (tx-log-latest-tx log) 1))])
      (for-each
        (lambda (entry)
          (for-each
            (lambda (dv)
              (insert-datom-row! ae schema dv))
            (tx-log-entry-datoms entry)))
        entries)
      (when (pair? entries)
        (analytics-engine-last-synced-tx-set! ae
          (tx-log-entry-tx-id (car (reverse entries)))))))

  (define (insert-datom-row! ae schema dv)
    ;; dv is a vector: #(e a v tx added?)
    (let* ([e (vector-ref dv 0)]
           [a (vector-ref dv 1)]
           [v (vector-ref dv 2)]
           [tx (vector-ref dv 3)]
           [added? (vector-ref dv 4)]
           [attr (schema-lookup-by-id schema a)]
           [a-name (if attr (symbol->string (db-attribute-ident attr)) "")]
           [vtype (if attr (db-attribute-value-type attr) #f)])
      (duckdb-exec! (analytics-engine-duckdb-conn ae)
        (format "INSERT INTO datoms VALUES (~a, ~a, '~a', '~a', ~a, ~a, ~a, ~a, ~a, ~a, ~a)"
                e a a-name
                (if (string? v) (escape-sql v) (format "~a" v))
                (if (and vtype (eq? vtype 'db.type/long) (number? v)) v 'NULL)
                (if (and vtype (eq? vtype 'db.type/double) (number? v)) v 'NULL)
                (if (boolean? v) (if v 'TRUE 'FALSE) 'NULL)
                (if (and vtype (eq? vtype 'db.type/instant) (number? v)) v 'NULL)
                (if (and vtype (eq? vtype 'db.type/ref) (number? v)) v 'NULL)
                tx
                (if added? 'TRUE 'FALSE)))))

  ;; ---- SQL query ----

  (define (analytics-query ae sql-string . params)
    ;; Ensure synced, then execute SQL
    (analytics-sync! ae)
    (duckdb-query (analytics-engine-duckdb-conn ae) sql-string params))

  ;; ---- Parquet export ----

  (define (export-parquet ae path . opts)
    ;; Uses DuckDB's native Parquet writer
    (analytics-sync! ae)
    (duckdb-exec! (analytics-engine-duckdb-conn ae)
      (format "COPY datoms TO '~a' (FORMAT PARQUET)" path)))

  ;; ---- Parquet/CSV import ----

  (define (import-parquet conn path mapping)
    ;; mapping: alist of (column-name . attr-keyword)
    ;; Each row becomes an entity, each column an attribute
    (error 'import-parquet "Not yet implemented — requires DuckDB Parquet reader"))

  (define (import-csv conn path mapping)
    (error 'import-csv "Not yet implemented — requires DuckDB CSV reader"))

  ;; ---- DuckDB stubs ----
  ;; These will be replaced with actual (std db duckdb) calls.

  (define (duckdb-exec! conn sql)
    ;; Stub: will call duckdb-query from (std db duckdb)
    (void))

  (define (duckdb-query conn sql params)
    ;; Stub: returns list of alists
    '())

  (define (escape-sql s)
    (let loop ([i 0] [out '()])
      (if (>= i (string-length s))
          (list->string (reverse out))
          (let ([c (string-ref s i)])
            (if (char=? c #\')
                (loop (+ i 1) (cons #\' (cons #\' out)))
                (loop (+ i 1) (cons c out)))))))

) ;; end library
