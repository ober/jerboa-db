#!chezscheme
;;; (jerboa-db migrate) — Schema migration utilities
;;;
;;; Adding new attributes is always safe (additive schema).
;;; Renaming/removing requires migration operations.

(library (jerboa-db migrate)
  (export
    migrate! migration? make-migration
    make-rename-attr make-add-index make-remove-index
    make-merge-attr make-split-attr
    make-retype-attr make-delete-attr
    migration-plan migration-dry-run

    ;; Online reindexing
    reindex! reindex-attribute!)

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
          (jerboa-db core))

  ;; ---- Migration operation types ----

  (define-record-type migration
    (fields operations))  ;; list of migration ops

  (define-record-type rename-op
    (fields from-attr to-attr))

  (define-record-type add-index-op
    (fields attr-ident))

  (define-record-type remove-index-op
    (fields attr-ident))

  (define-record-type merge-op
    (fields from-attr into-attr merge-fn))

  (define-record-type split-op
    (fields from-attr into-a into-b split-fn))

  (define-record-type retype-op
    (fields attr-ident new-type coerce-fn))

  (define-record-type delete-attr-op
    (fields attr-ident))

  ;; ---- Convenience constructors ----

  (def (make-rename-attr from to) (make-rename-op from to))
  (def (make-add-index attr) (make-add-index-op attr))
  (def (make-remove-index attr) (make-remove-index-op attr))
  (def (make-merge-attr from into fn) (make-merge-op from into fn))
  (def (make-split-attr from a b fn) (make-split-op from a b fn))
  (def (make-retype-attr attr new-type coerce-fn)
    (make-retype-op attr new-type coerce-fn))
  (def (make-delete-attr attr)
    (make-delete-attr-op attr))

  ;; ---- Execute migration ----

  (def (migrate! conn migration-obj)
    (let ([ops (migration-operations migration-obj)]
          [current-db (db conn)]
          [schema (db-value-schema (db conn))])
      (for-each
        (lambda (op)
          (cond
            [(rename-op? op)
             (migrate-rename! conn schema
               (rename-op-from-attr op) (rename-op-to-attr op))]
            [(add-index-op? op)
             (migrate-add-index! conn schema (add-index-op-attr-ident op))]
            [(remove-index-op? op)
             (migrate-remove-index! conn schema (remove-index-op-attr-ident op))]
            [(merge-op? op)
             (migrate-merge! conn schema
               (merge-op-from-attr op) (merge-op-into-attr op)
               (merge-op-merge-fn op))]
            [(split-op? op)
             (migrate-split! conn schema
               (split-op-from-attr op) (split-op-into-a op)
               (split-op-into-b op) (split-op-split-fn op))]
            [(retype-op? op)
             (migrate-retype! conn schema
               (retype-op-attr-ident op)
               (retype-op-new-type op)
               (retype-op-coerce-fn op))]
            [(delete-attr-op? op)
             (migrate-delete-attr! conn schema (delete-attr-op-attr-ident op))]
            [else (error 'migrate! "Unknown migration op" op)]))
        ops)))

  ;; ---- Individual migration operations ----

  (def (migrate-rename! conn schema from-ident to-ident)
    ;; 1. Create new attribute with same schema as old
    ;; 2. Copy all datoms from old attr to new attr
    ;; 3. Retract all datoms on old attr
    (let ([old-attr (schema-lookup-by-ident schema from-ident)])
      (unless old-attr
        (error 'migrate-rename! "Attribute not found" from-ident))
      ;; Install new schema attribute
      (transact! conn
        (list `((db/ident . ,to-ident)
                (db/valueType . ,(db-attribute-value-type old-attr))
                (db/cardinality . ,(db-attribute-cardinality old-attr)))))
      ;; Query all entities with old attribute and copy
      (let ([results (q `((find ?e ?v)
                          (where (?e ,from-ident ?v)))
                        (db conn))])
        (when (pair? results)
          (transact! conn
            (append
              ;; Assert new attr values
              (map (lambda (row)
                     `((db/id . ,(car row))
                       (,to-ident . ,(cadr row))))
                   results)
              ;; Retract old attr values
              (map (lambda (row)
                     `(db/retract ,(car row) ,from-ident ,(cadr row)))
                   results)))))))

  (def (migrate-add-index! conn schema attr-ident)
    ;; Update the schema attribute to set :db/index true
    (let ([attr (schema-lookup-by-ident schema attr-ident)])
      (unless attr (error 'migrate-add-index! "Attribute not found" attr-ident))
      ;; Re-transact schema with index flag
      (transact! conn
        (list `((db/ident . ,attr-ident)
                (db/valueType . ,(db-attribute-value-type attr))
                (db/cardinality . ,(db-attribute-cardinality attr))
                (db/index . #t))))))

  (def (migrate-remove-index! conn schema attr-ident)
    (let ([attr (schema-lookup-by-ident schema attr-ident)])
      (unless attr (error 'migrate-remove-index! "Attribute not found" attr-ident))
      (transact! conn
        (list `((db/ident . ,attr-ident)
                (db/valueType . ,(db-attribute-value-type attr))
                (db/cardinality . ,(db-attribute-cardinality attr))
                (db/index . #f))))))

  (def (migrate-merge! conn schema from-ident into-ident merge-fn)
    ;; Merge values from one attribute into another using merge-fn
    (let ([results (q `((find ?e ?v1 ?v2)
                        (where (?e ,from-ident ?v1)
                               (?e ,into-ident ?v2)))
                      (db conn))])
      (when (pair? results)
        (transact! conn
          (map (lambda (row)
                 (let ([merged (merge-fn (cadr row) (caddr row))])
                   `((db/id . ,(car row))
                     (,into-ident . ,merged))))
               results)))))

  (def (migrate-split! conn schema from-ident into-a into-b split-fn)
    ;; Split one attribute into two using split-fn (returns (values a b))
    (let ([results (q `((find ?e ?v) (where (?e ,from-ident ?v)))
                      (db conn))])
      (when (pair? results)
        (transact! conn
          (map (lambda (row)
                 (let-values ([(va vb) (split-fn (cadr row))])
                   `((db/id . ,(car row))
                     (,into-a . ,va)
                     (,into-b . ,vb))))
               results)))))

  ;; ---- Schema entity lookup helper ----

  (def (find-schema-entity-eid conn ident)
    ;; Find the entity ID for a schema attribute by its db/ident value.
    ;; Returns the EID or #f if not found.
    (let ([results (q `((find ?e)
                         (in $ ?ident)
                         (where (?e db/ident ?ident)))
                      (db conn) ident)])
      (and (pair? results) (caar results))))

  ;; ---- migrate-retype! ----

  (def (migrate-retype! conn schema attr-ident new-type coerce-fn)
    ;; Step 1: Validate the attribute exists
    (let ([attr (schema-lookup-by-ident schema attr-ident)])
      (unless attr
        (error 'migrate-retype! "Attribute not found" attr-ident))
      ;; Step 2: Query all current (entity, value) pairs for this attribute
      (let ([results (q `((find ?e ?v)
                           (where (?e ,attr-ident ?v)))
                        (db conn))])
        ;; Step 3: Validate that coerce-fn works for all values (safety check before
        ;; any data is changed — fail fast before touching anything)
        (for-each
          (lambda (row)
            (let ([v (cadr row)])
              (guard (exn [#t (error 'migrate-retype!
                                (format "coerce-fn failed for value ~a: ~a"
                                        v (with-output-to-string
                                            (lambda () (display-condition exn))))
                                attr-ident)])
                (coerce-fn v))))
          results)
        ;; Step 4: Retract all existing values (using the old type, which is fine
        ;; since db/retract does not validate value types)
        (when (pair? results)
          (transact! conn
            (map (lambda (row)
                   `(db/retract ,(car row) ,attr-ident ,(cadr row)))
                 results)))
        ;; Step 5: Update the schema attribute's value-type BEFORE asserting new values.
        ;; We must include db/ident in this transaction so that materialize-schema-datoms!
        ;; fires and updates the in-memory schema registry (it only triggers on db/ident datoms).
        (transact! conn
          (list `((db/ident . ,attr-ident)
                  (db/valueType . ,new-type)
                  (db/cardinality . ,(db-attribute-cardinality attr)))))
        ;; Step 6: Assert new coerced values (schema is now the new type)
        (when (pair? results)
          (transact! conn
            (map (lambda (row)
                   `((db/id . ,(car row))
                     (,attr-ident . ,(coerce-fn (cadr row)))))
                 results)))
        ;; Step 7: Rebuild AVET for the attribute if it is indexed
        (let* ([new-schema (db-value-schema (db conn))]
               [new-attr   (schema-lookup-by-ident new-schema attr-ident)])
          (when (and new-attr
                     (or (db-attribute-index? new-attr)
                         (db-attribute-unique new-attr)))
            (reindex-attribute! conn attr-ident)))
        (length results))))

  ;; ---- migrate-delete-attr! ----

  (def (migrate-delete-attr! conn schema attr-ident)
    ;; Step 1: Validate the attribute exists
    (let ([attr (schema-lookup-by-ident schema attr-ident)])
      (unless attr
        (error 'migrate-delete-attr! "Attribute not found" attr-ident))
      ;; Step 2: Query all entities that have this attribute
      (let ([results (q `((find ?e ?v) (where (?e ,attr-ident ?v)))
                        (db conn))])
        ;; Step 3: Retract all data datoms for this attribute
        (when (pair? results)
          (transact! conn
            (map (lambda (row)
                   `(db/retract ,(car row) ,attr-ident ,(cadr row)))
                 results)))
        ;; Step 4: Retract the schema entity for this attribute
        (let ([schema-eid (find-schema-entity-eid conn attr-ident)])
          (when schema-eid
            (transact! conn (list `(db/retractEntity ,schema-eid)))))
        (length results))))

  ;; ---- Planning and dry-run ----

  (def (migration-plan migration-obj)
    ;; Return a human-readable plan of what the migration will do
    (map (lambda (op)
           (cond
             [(rename-op? op)
              (format "RENAME ~a -> ~a" (rename-op-from-attr op) (rename-op-to-attr op))]
             [(add-index-op? op)
              (format "ADD INDEX on ~a" (add-index-op-attr-ident op))]
             [(remove-index-op? op)
              (format "REMOVE INDEX on ~a" (remove-index-op-attr-ident op))]
             [(merge-op? op)
              (format "MERGE ~a into ~a" (merge-op-from-attr op) (merge-op-into-attr op))]
             [(split-op? op)
              (format "SPLIT ~a into ~a, ~a"
                      (split-op-from-attr op) (split-op-into-a op) (split-op-into-b op))]
             [(retype-op? op)
              (format "RETYPE ~a -> ~a" (retype-op-attr-ident op) (retype-op-new-type op))]
             [(delete-attr-op? op)
              (format "DELETE ATTRIBUTE ~a (all datoms will be retracted)"
                      (delete-attr-op-attr-ident op))]
             [else "UNKNOWN"]))
         (migration-operations migration-obj)))

  (def (migration-dry-run conn migration-obj)
    ;; Report what would change without executing.
    ;; Returns a list of descriptors; each descriptor includes an 'affects N datoms'
    ;; count for operations that touch data.
    (map
      (lambda (op)
        (cond
          [(rename-op? op)
           (let ([count (length (q `((find ?e ?v)
                                      (where (?e ,(rename-op-from-attr op) ?v)))
                                   (db conn)))])
             (list 'rename (rename-op-from-attr op) '-> (rename-op-to-attr op)
                   'affects count 'datoms))]
          [(retype-op? op)
           (let ([count (length (q `((find ?e ?v)
                                      (where (?e ,(retype-op-attr-ident op) ?v)))
                                   (db conn)))])
             (list 'retype (retype-op-attr-ident op) '-> (retype-op-new-type op)
                   'affects count 'datoms))]
          [(delete-attr-op? op)
           (let ([count (length (q `((find ?e ?v)
                                      (where (?e ,(delete-attr-op-attr-ident op) ?v)))
                                   (db conn)))])
             (list 'delete (delete-attr-op-attr-ident op)
                   'affects count 'datoms))]
          [(add-index-op? op)
           (list 'add-index (add-index-op-attr-ident op))]
          [(remove-index-op? op)
           (list 'remove-index (remove-index-op-attr-ident op))]
          [(merge-op? op)
           (list 'merge (merge-op-from-attr op) 'into (merge-op-into-attr op))]
          [(split-op? op)
           (list 'split (split-op-from-attr op) 'into
                 (split-op-into-a op) (split-op-into-b op))]
          [else (list 'unknown op)]))
      (migration-operations migration-obj)))


  ;; ---- Online reindexing ----

  ;; reindex! rebuilds AEVT, AVET, and VAET from EAVT.
  ;; EAVT is the source of truth; the other three indices are derived.
  ;; This is useful after manual data manipulation or to compact indices.

  (def (reindex! conn)
    (let* ([current-db (db conn)]
           [indices    (db-value-indices current-db)]
           [schema     (db-value-schema current-db)]
           [eavt       (index-set-eavt indices)]
           [aevt       (index-set-aevt indices)]
           [avet       (index-set-avet indices)]
           [vaet       (index-set-vaet indices)]
           [all-datoms (dbi-datoms eavt)])
      ;; Rebuild fresh AEVT, AVET, VAET by creating new memory indices
      ;; and copying them into the current index-set's underlying cells.
      ;; Since dbi is a closure over a mutable tree-cell, we drain the old
      ;; indices first then re-insert all datoms from EAVT.

      ;; Step 1: drain AEVT, AVET, VAET
      (for-each (lambda (d)
                  (dbi-remove! aevt d)
                  (dbi-remove! avet d)
                  (dbi-remove! vaet d))
                all-datoms)

      ;; Step 2: re-insert all EAVT datoms into derived indices
      (for-each
        (lambda (d)
          (dbi-add! aevt d)
          (let ([attr (schema-lookup-by-id schema (datom-a d))])
            (when (and attr (indexed-attr? attr))
              (dbi-add! avet d))
            (when (and attr (ref-type? attr))
              (dbi-add! vaet d))))
        all-datoms)

      ;; Return count of datoms reindexed
      (length all-datoms)))

  ;; reindex-attribute! rebuilds AVET and VAET entries for a single attribute.
  ;; Useful after adding or removing :db/index or :db/unique on an attribute.

  (def (reindex-attribute! conn attr-ident)
    (let* ([current-db (db conn)]
           [indices    (db-value-indices current-db)]
           [schema     (db-value-schema current-db)]
           [attr       (schema-lookup-by-ident schema attr-ident)])
      (unless attr
        (error 'reindex-attribute! "Unknown attribute" attr-ident))
      (let* ([aid  (db-attribute-id attr)]
             [eavt (index-set-eavt indices)]
             [avet (index-set-avet indices)]
             [vaet (index-set-vaet indices)]
             ;; Find all datoms for this attribute in EAVT
             [lo   (make-datom 0 aid +min-val+ 0 #t)]
             [hi   (make-datom (greatest-fixnum) aid +max-val+ (greatest-fixnum) #t)]
             [attr-datoms (dbi-range eavt lo hi)])
        ;; Remove old AVET/VAET entries for this attribute
        (for-each (lambda (d)
                    (dbi-remove! avet d)
                    (dbi-remove! vaet d))
                  attr-datoms)
        ;; Re-insert according to current schema flags
        (for-each
          (lambda (d)
            (when (indexed-attr? attr)
              (dbi-add! avet d))
            (when (ref-type? attr)
              (dbi-add! vaet d)))
          attr-datoms)
        (length attr-datoms))))

) ;; end library
