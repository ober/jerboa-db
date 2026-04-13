#!chezscheme
;;; (jerboa-db core) — Public API
;;;
;;; This is the primary entry point for Jerboa-DB.
;;; connect, db, transact!, q, pull, entity, as-of, since, history

(library (jerboa-db core)
  (export
    ;; Connection
    connect close connection? db

    ;; Transactions
    transact! tempid tempid?
    tx-report? tx-report-db-before tx-report-db-after
    tx-report-tx-data tx-report-tempids

    ;; Query
    q explain-query

    ;; Pull API
    pull pull-many

    ;; Entity API
    entity touch

    ;; Time-travel
    as-of since history

    ;; Transaction log
    tx-range

    ;; Direct index access
    datoms

    ;; Utilities
    db-stats schema-for

    ;; Fulltext search
    fulltext-search

    ;; Garbage collection
    gc-collect! gc-stats

    ;; Entity specs
    validate-entity check-entity-spec define-spec

    ;; Value store
    make-value-store value-store-put! value-store-get
    value-store-has? value-store-close value-store-stats

    ;; Transaction log navigation
    log log? log-tx-range log-tx-list

    ;; Index range and seek
    index-range seek-datoms

    ;; Internal constructors (used by backup/restore)
    make-connection new-db-cache
    connection-current-db-set!
    connection-next-eid connection-next-eid-set!
    connection-fulltext-index
    connection-tx-log
    connection-db-stats

    ;; Analytics (DuckDB-backed OLAP)
    analytics-export! analytics-query analytics-close!
    new-analytics-engine analytics-sync!
    export-parquet import-parquet import-csv)

  (import (except (chezscheme)
                  make-hash-table hash-table?
                  sort sort!
                  printf fprintf
                  path-extension path-absolute?
                  with-input-from-string with-output-to-string
                  iota 1+ 1-
                  partition
                  make-date make-time
                  log
                atom? meta)
          (jerboa prelude)
          (jerboa-db datom)
          (jerboa-db schema)
          (jerboa-db index protocol)
          (jerboa-db index memory)
          (jerboa-db history)
          (jerboa-db cache)
          (jerboa-db tx)
          (jerboa-db query engine)
          (jerboa-db query pull)
          (jerboa-db entity)
          (jerboa-db fulltext)
          (jerboa-db gc)
          (jerboa-db spec)
          (jerboa-db value-store)
          (jerboa-db log)
          (jerboa-db stats))

  ;; ---- Connection ----
  ;; A connection holds the mutable state: current db-value, entity counter,
  ;; transaction log, and cache.

  (defstruct connection
    (current-db      ;; db-value
     next-eid        ;; next entity ID to assign (mutable cell)
     tx-log          ;; list of tx-reports (most recent first)
     db-cache        ;; LRU cache
     path            ;; storage path (":memory:" for in-memory)
     db-handles      ;; LevelDB handles for cleanup (#f for in-memory)
     fulltext-index  ;; in-memory fulltext inverted index
     db-stats))      ;; db-stats record: per-attribute datom counts

  ;; ---- connect ----
  ;; path = ":memory:" → in-memory RB-tree indices
  ;; path = anything else → LevelDB-backed persistent indices

  ;; Lazy loader for LevelDB backend — avoids loading the shared library
  ;; until a persistent connection is actually requested.
  (def leveldb-loaded? #f)
  (def leveldb-make-index-set #f)
  (def leveldb-close-index-set #f)

  (def (ensure-leveldb!)
    (unless leveldb-loaded?
      (eval '(import (jerboa-db index leveldb)))
      (set! leveldb-make-index-set
            (eval 'make-leveldb-index-set))
      (set! leveldb-close-index-set
            (eval 'close-leveldb-index-set))
      (set! leveldb-loaded? #t)))

  (def (connect path)
    (let-values ([(indices handles)
                  (if (string=? path ":memory:")
                      (values (make-mem-index-set) #f)
                      (begin
                        (ensure-leveldb!)
                        (leveldb-make-index-set path)))])
      (let* ([schema (new-schema-registry)]
             [stats (make-db-stats)]
             [initial-db (make-db-value 0 indices schema #f #f #f stats)]
             [conn (make-connection
                     initial-db
                     (list +first-user-attr-id+)
                     '()
                     (new-db-cache 10000)
                     path
                     handles
                     (make-fulltext-index)
                     stats)])
        conn)))

  ;; ---- close ----
  ;; Close a persistent connection. No-op for in-memory.

  (def (close conn)
    (let ([handles (connection-db-handles conn)])
      (when handles
        (when leveldb-close-index-set
          (leveldb-close-index-set handles))
        (connection-db-handles-set! conn #f))))

  ;; ---- db: get current database value ----

  (def (db conn)
    (connection-current-db conn))

  ;; ---- transact! ----

  (def (transact! conn tx-ops)
    (let* ([current (connection-current-db conn)]
           [eid-cell (connection-next-eid conn)]
           [report (process-transaction current tx-ops eid-cell)])
      ;; Update schema if schema attributes were transacted
      (materialize-schema-datoms! (db-value-schema (tx-report-db-after report))
                                   (tx-report-tx-data report))
      ;; Update per-attribute statistics incrementally
      (let* ([stats (connection-db-stats conn)]
             [db-after-raw (tx-report-db-after report)]
             ;; Attach live stats to the new db-value so queries can use them
             [db-after (make-db-value
                         (db-value-basis-tx db-after-raw)
                         (db-value-indices db-after-raw)
                         (db-value-schema db-after-raw)
                         (db-value-as-of-tx db-after-raw)
                         (db-value-since-tx db-after-raw)
                         (db-value-history? db-after-raw)
                         stats)])
        (db-stats-update! stats (tx-report-tx-data report))
        ;; Update connection state
        (connection-current-db-set! conn db-after)
        (connection-tx-log-set! conn (cons report (connection-tx-log conn)))
        ;; Clear entity cache (conservative — could be smarter)
        (cache-clear! (connection-db-cache conn))
        ;; Update fulltext index with new datoms
        (fulltext-index-datoms! (connection-fulltext-index conn)
                                 (db-value-schema db-after)
                                 (tx-report-tx-data report))
        report)))

  ;; ---- Schema materialization ----
  ;; When datoms define schema attributes, materialize them into the registry.

  (def (materialize-schema-datoms! schema datoms)
    ;; Collect datoms that define schema attributes (those with db/ident)
    ;; Group by entity, then build db-attribute records.
    (let ([ident-datoms (filter (lambda (d)
                                  (and (datom-added? d)
                                       (let ([attr (schema-lookup-by-id schema (datom-a d))])
                                         (and attr
                                              (eq? (db-attribute-ident attr) 'db/ident)))))
                                datoms)])
      (for-each
        (lambda (ident-datom)
          (let* ([eid (datom-e ident-datom)]
                 [attr-ident (datom-v ident-datom)]
                 ;; Find other schema datoms for this entity
                 [entity-datoms (filter (lambda (d)
                                          (and (= (datom-e d) eid)
                                               (datom-added? d)))
                                        datoms)]
                 [get-val (lambda (attr-name)
                            (let ([d (find (lambda (d)
                                            (let ([a (schema-lookup-by-id schema (datom-a d))])
                                              (and a (eq? (db-attribute-ident a) attr-name))))
                                          entity-datoms)])
                              (and d (datom-v d))))]
                 [vtype (or (get-val 'db/valueType) 'db.type/string)]
                 [card (or (get-val 'db/cardinality) 'db.cardinality/one)]
                 [uniq (get-val 'db/unique)]
                 [idx? (get-val 'db/index)]
                 [doc (get-val 'db/doc)]
                 [comp? (get-val 'db/isComponent)]
                 [no-hist? (get-val 'db/noHistory)]
                 ;; Assign or lookup attribute ID
                 [aid (schema-intern-attr! schema attr-ident)]
                 [tuple-attrs (get-val 'db/tupleAttrs)]
                 [ft? (get-val 'db/fulltext)]
                 [db-attr (make-db-attribute
                            attr-ident aid vtype card
                            uniq (or idx? (and uniq #t))
                            comp? doc no-hist?
                            tuple-attrs ft?)])
            (schema-install-attribute! schema db-attr)))
        ident-datoms)))

  ;; ---- q: Datalog query ----

  (def (q query-form db-val . inputs)
    (let ([parsed (parse-query query-form)])
      (apply query-db parsed db-val inputs)))

  ;; ---- pull ----

  (def (pull db-val pattern eid)
    (pull-entity db-val pattern eid))

  ;; pull-many re-exported from (jerboa-db query pull)

  ;; ---- entity ----

  (def (entity db-val eid)
    (new-entity-map eid db-val))

  (def (touch ent)
    (entity-touch ent))

  ;; ---- Time-travel (re-exported) ----
  ;; as-of, since, history are re-exported from (jerboa-db history)

  ;; ---- Transaction log access ----

  (def (tx-range conn start-tx end-tx)
    ;; Return datoms from the transaction log between start-tx and end-tx
    (let loop ([log (connection-tx-log conn)] [result '()])
      (if (null? log)
          result
          (let* ([report (car log)]
                 [tx-data (tx-report-tx-data report)])
            (let ([matching (filter (lambda (d)
                                      (let ([tx (datom-tx d)])
                                        (and (>= tx start-tx) (< tx end-tx))))
                                    tx-data)])
              (loop (cdr log) (append matching result)))))))

  ;; ---- Transaction log navigation (log API) ----

  (def (log conn)
    "Return a navigable log-handle wrapping conn's transaction history."
    (make-log-handle (connection-tx-log conn)))

  ;; log?, log-tx-list, log-tx-range are re-exported directly from (jerboa-db log)

  ;; ---- Utilities ----

  (def (db-stats conn)
    (let* ([current (db conn)]
           [indices (db-value-indices current)])
      `((basis-tx . ,(db-value-basis-tx current))
        (eavt-count . ,(length (dbi-datoms (index-set-eavt indices))))
        (aevt-count . ,(length (dbi-datoms (index-set-aevt indices))))
        (avet-count . ,(length (dbi-datoms (index-set-avet indices))))
        (vaet-count . ,(length (dbi-datoms (index-set-vaet indices))))
        (cache . ,(cache-stats (connection-db-cache conn))))))

  (def (schema-for conn)
    ;; Returns a list of all user-defined db-attribute records (id >= +first-user-attr-id+)
    ;; with full details: ident, value-type, cardinality, unique, index?, etc.
    (filter (lambda (attr) (>= (db-attribute-id attr) +first-user-attr-id+))
            (schema-all-attributes (db-value-schema (db conn)))))

  ;; ---- Direct index access ----
  ;; (datoms db index-name . components)
  ;; index-name: 'eavt, 'aevt, 'avet, or 'vaet
  ;; components: partial key values in index order
  ;;   eavt: [e a v tx]
  ;;   aevt: [a e v tx]
  ;;   avet: [a v e tx]
  ;;   vaet: [v a e tx]
  ;; Each component narrows the scan; omit trailing components for prefix scans.
  ;; Attribute ident symbols are automatically resolved to their integer IDs.

  (def (datoms db index-name . components)
    (let* ([schema (db-value-schema db)]
           [resolve-attr (lambda (x)
                           ;; If x is a symbol that looks like an attribute ident, resolve it
                           (if (symbol? x)
                               (let ([attr (schema-lookup-by-ident schema x)])
                                 (if attr (db-attribute-id attr) x))
                               x))]
           [idx (db-resolve-index db index-name)])
      (let-values ([(lo hi)
                    (case index-name
                      [(eavt)
                       ;; order: e, a, v, tx
                       (let* ([e  (and (>= (length components) 1) (list-ref components 0))]
                              [a  (and (>= (length components) 2) (resolve-attr (list-ref components 1)))]
                              [v  (and (>= (length components) 3) (list-ref components 2))]
                              [tx (and (>= (length components) 4) (list-ref components 3))])
                         (values
                           (make-datom (or e 0)
                                       (or a 0)
                                       (if v v +min-val+)
                                       (or tx 0)
                                       #t)
                           (make-datom (or e (greatest-fixnum))
                                       (or a (greatest-fixnum))
                                       (if v v +max-val+)
                                       (or tx (greatest-fixnum))
                                       #t)))]
                      [(aevt)
                       ;; order: a, e, v, tx
                       (let* ([a  (and (>= (length components) 1) (resolve-attr (list-ref components 0)))]
                              [e  (and (>= (length components) 2) (list-ref components 1))]
                              [v  (and (>= (length components) 3) (list-ref components 2))]
                              [tx (and (>= (length components) 4) (list-ref components 3))])
                         (values
                           (make-datom (or e 0)
                                       (or a 0)
                                       (if v v +min-val+)
                                       (or tx 0)
                                       #t)
                           (make-datom (or e (greatest-fixnum))
                                       (or a (greatest-fixnum))
                                       (if v v +max-val+)
                                       (or tx (greatest-fixnum))
                                       #t)))]
                      [(avet)
                       ;; order: a, v, e, tx
                       (let* ([a  (and (>= (length components) 1) (resolve-attr (list-ref components 0)))]
                              [v  (and (>= (length components) 2) (list-ref components 1))]
                              [e  (and (>= (length components) 3) (list-ref components 2))]
                              [tx (and (>= (length components) 4) (list-ref components 3))])
                         (values
                           (make-datom (or e 0)
                                       (or a 0)
                                       (if v v +min-val+)
                                       (or tx 0)
                                       #t)
                           (make-datom (or e (greatest-fixnum))
                                       (or a (greatest-fixnum))
                                       (if v v +max-val+)
                                       (or tx (greatest-fixnum))
                                       #t)))]
                      [(vaet)
                       ;; order: v, a, e, tx
                       (let* ([v  (and (>= (length components) 1) (list-ref components 0))]
                              [a  (and (>= (length components) 2) (resolve-attr (list-ref components 1)))]
                              [e  (and (>= (length components) 3) (list-ref components 2))]
                              [tx (and (>= (length components) 4) (list-ref components 3))])
                         (values
                           (make-datom (or e 0)
                                       (or a 0)
                                       (if v v +min-val+)
                                       (or tx 0)
                                       #t)
                           (make-datom (or e (greatest-fixnum))
                                       (or a (greatest-fixnum))
                                       (if v v +max-val+)
                                       (or tx (greatest-fixnum))
                                       #t)))]
                      [else
                       (error 'datoms "unknown index name" index-name)])])
        (let ([raw (dbi-range idx lo hi)])
          (if (db-value-history? db)
              (filter (lambda (d) (db-filter-datom? db d)) raw)
              ;; Non-history: return only current (added) datoms
              (filter (lambda (d)
                        (and (db-filter-datom? db d)
                             (datom-added? d)))
                      raw))))))

  ;; ---- index-range ----
  ;; (index-range db attr-ident start end)
  ;; Returns AVET datoms for attr whose value is in [start, end] (both inclusive).
  ;; Either start or end may be #f for open-ended ranges.

  (def (index-range db attr-ident start end)
    (let* ([schema (db-value-schema db)]
           [attr   (schema-lookup-by-ident schema attr-ident)]
           [aid    (if attr (db-attribute-id attr)
                       (error 'index-range "Unknown attribute" attr-ident))]
           [idx    (db-resolve-index db 'avet)]
           [lo     (make-datom 0 aid (if start start +min-val+) 0 #t)]
           [hi     (make-datom (greatest-fixnum) aid
                               (if end end +max-val+) (greatest-fixnum) #t)]
           [raw    (dbi-range idx lo hi)])
      (filter (lambda (d)
                (and (db-filter-datom? db d)
                     (datom-added? d)))
              raw)))

  ;; ---- seek-datoms ----
  ;; (seek-datoms db index-name components...)
  ;; Like datoms but semantically a positioned scan — delegates to datoms.
  ;; Returns all datoms matching the given component prefix.

  (def (seek-datoms db index-name . components)
    (apply datoms db index-name components))

  ;; ---- Fulltext search ----
  ;; Search for text in fulltext-indexed attributes on this connection.

  (def (fulltext-search conn attr-ident text)
    (ft-search (connection-fulltext-index conn)
               (db-value-schema (db conn))
               attr-ident
               text))

  ;; ---- Garbage collection ----

  (def (gc-collect! conn . opts)
    (apply gc-collect-db! (connection-current-db conn) opts))

  (def (gc-stats conn . opts)
    (apply gc-stats-db (connection-current-db conn) opts))

  ;; ---- Analytics (DuckDB-backed OLAP) ----
  ;;
  ;; Loaded lazily so DuckDB is not required for normal operation.

  (def analytics-loaded? #f)
  (def analytics-new-engine-fn #f)
  (def analytics-sync-fn #f)
  (def analytics-query-fn #f)
  (def analytics-close-fn #f)
  (def analytics-export-parquet-fn #f)
  (def analytics-import-parquet-fn #f)
  (def analytics-import-csv-fn #f)

  (def (ensure-analytics!)
    (unless analytics-loaded?
      (eval '(import (jerboa-db analytics)))
      (set! analytics-new-engine-fn  (eval 'new-analytics-engine))
      (set! analytics-sync-fn        (eval 'analytics-sync!))
      (set! analytics-query-fn       (eval 'analytics-query))
      (set! analytics-close-fn       (eval 'analytics-close))
      (set! analytics-export-parquet-fn (eval 'export-parquet))
      (set! analytics-import-parquet-fn (eval 'import-parquet))
      (set! analytics-import-csv-fn  (eval 'import-csv))
      (set! analytics-loaded? #t)))

  (def (analytics-export! db-val . opts)
    (ensure-analytics!)
    (let* ([schema (db-value-schema db-val)]
           [path   (if (pair? opts) (car opts) ":memory:")]
           [ae     (analytics-new-engine-fn schema path)])
      (analytics-sync-fn ae db-val)
      ae))

  (def (analytics-query ae sql-string . params)
    (ensure-analytics!)
    (apply analytics-query-fn ae sql-string params))

  (def (analytics-close! ae)
    (ensure-analytics!)
    (analytics-close-fn ae))

  (def (new-analytics-engine schema . opts)
    (ensure-analytics!)
    (apply analytics-new-engine-fn schema opts))

  (def (analytics-sync! ae db-val)
    (ensure-analytics!)
    (analytics-sync-fn ae db-val))

  (def (export-parquet ae path . opts)
    (ensure-analytics!)
    (apply analytics-export-parquet-fn ae path opts))

  (def (import-parquet ae path mapping)
    (ensure-analytics!)
    (analytics-import-parquet-fn ae path mapping))

  (def (import-csv ae path mapping)
    (ensure-analytics!)
    (analytics-import-csv-fn ae path mapping))

) ;; end library
