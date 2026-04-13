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

    ;; Internal constructors (used by backup/restore)
    make-connection new-db-cache
    connection-current-db-set!
    connection-next-eid connection-next-eid-set!
    connection-fulltext-index)

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
          (jerboa-db value-store))

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
     fulltext-index)) ;; in-memory fulltext inverted index

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
             [initial-db (make-db-value 0 indices schema #f #f #f)]
             [conn (make-connection
                     initial-db
                     (list +first-user-attr-id+)
                     '()
                     (new-db-cache 10000)
                     path
                     handles
                     (make-fulltext-index))])
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
      ;; Update connection state
      (connection-current-db-set! conn (tx-report-db-after report))
      (connection-tx-log-set! conn (cons report (connection-tx-log conn)))
      ;; Clear entity cache (conservative — could be smarter)
      (cache-clear! (connection-db-cache conn))
      ;; Update fulltext index with new datoms
      (fulltext-index-datoms! (connection-fulltext-index conn)
                               (db-value-schema (tx-report-db-after report))
                               (tx-report-tx-data report))
      report))

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
    (schema-all-attributes (db-value-schema (db conn))))

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

) ;; end library
