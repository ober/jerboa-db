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

    ;; Internal constructors (used by backup/restore)
    make-connection new-db-cache
    connection-current-db-set!
    connection-next-eid connection-next-eid-set!)

  (import (chezscheme)
          (jerboa-db datom)
          (jerboa-db schema)
          (jerboa-db index protocol)
          (jerboa-db index memory)
          (jerboa-db history)
          (jerboa-db cache)
          (jerboa-db tx)
          (jerboa-db query engine)
          (jerboa-db query pull)
          (jerboa-db entity))

  ;; ---- Connection ----
  ;; A connection holds the mutable state: current db-value, entity counter,
  ;; transaction log, and cache.

  (define-record-type connection
    (fields (mutable current-db)   ;; db-value
            (mutable next-eid)     ;; next entity ID to assign (mutable cell)
            (mutable tx-log)       ;; list of tx-reports (most recent first)
            (mutable db-cache)     ;; LRU cache
            path                   ;; storage path (":memory:" for in-memory)
            (mutable db-handles))) ;; LevelDB handles for cleanup (#f for in-memory)

  ;; ---- connect ----
  ;; path = ":memory:" → in-memory RB-tree indices
  ;; path = anything else → LevelDB-backed persistent indices

  ;; Lazy loader for LevelDB backend — avoids loading the shared library
  ;; until a persistent connection is actually requested.
  (define leveldb-loaded? #f)
  (define leveldb-make-index-set #f)
  (define leveldb-close-index-set #f)

  (define (ensure-leveldb!)
    (unless leveldb-loaded?
      (eval '(import (jerboa-db index leveldb)))
      (set! leveldb-make-index-set
            (eval 'make-leveldb-index-set))
      (set! leveldb-close-index-set
            (eval 'close-leveldb-index-set))
      (set! leveldb-loaded? #t)))

  (define (connect path)
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
                     handles)])
        conn)))

  ;; ---- close ----
  ;; Close a persistent connection. No-op for in-memory.

  (define (close conn)
    (let ([handles (connection-db-handles conn)])
      (when handles
        (when leveldb-close-index-set
          (leveldb-close-index-set handles))
        (connection-db-handles-set! conn #f))))

  ;; ---- db: get current database value ----

  (define (db conn)
    (connection-current-db conn))

  ;; ---- transact! ----

  (define (transact! conn tx-ops)
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
      report))

  ;; ---- Schema materialization ----
  ;; When datoms define schema attributes, materialize them into the registry.

  (define (materialize-schema-datoms! schema datoms)
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
                 [db-attr (make-db-attribute
                            attr-ident aid vtype card
                            uniq (or idx? (and uniq #t))
                            comp? doc no-hist?)])
            (schema-install-attribute! schema db-attr)))
        ident-datoms)))

  ;; ---- q: Datalog query ----

  (define (q query-form db-val . inputs)
    (let ([parsed (parse-query query-form)])
      (apply query-db parsed db-val inputs)))

  ;; ---- pull ----

  (define (pull db-val pattern eid)
    (pull-entity db-val pattern eid))

  ;; pull-many re-exported from (jerboa-db query pull)

  ;; ---- entity ----

  (define (entity db-val eid)
    (new-entity-map eid db-val))

  (define (touch ent)
    (entity-touch ent))

  ;; ---- Time-travel (re-exported) ----
  ;; as-of, since, history are re-exported from (jerboa-db history)

  ;; ---- Transaction log access ----

  (define (tx-range conn start-tx end-tx)
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

  (define (db-stats conn)
    (let* ([current (db conn)]
           [indices (db-value-indices current)])
      `((basis-tx . ,(db-value-basis-tx current))
        (eavt-count . ,(length (dbi-datoms (index-set-eavt indices))))
        (aevt-count . ,(length (dbi-datoms (index-set-aevt indices))))
        (avet-count . ,(length (dbi-datoms (index-set-avet indices))))
        (vaet-count . ,(length (dbi-datoms (index-set-vaet indices))))
        (cache . ,(cache-stats (connection-db-cache conn))))))

  (define (schema-for conn)
    (schema-all-attributes (db-value-schema (db conn))))

) ;; end library
