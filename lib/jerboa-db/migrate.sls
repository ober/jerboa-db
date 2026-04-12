#!chezscheme
;;; (jerboa-db migrate) — Schema migration utilities
;;;
;;; Adding new attributes is always safe (additive schema).
;;; Renaming/removing requires migration operations.

(library (jerboa-db migrate)
  (export
    migrate! migration?
    make-rename-attr make-add-index make-remove-index
    make-merge-attr make-split-attr
    migration-plan migration-dry-run)

  (import (chezscheme)
          (jerboa-db datom)
          (jerboa-db schema)
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

  ;; ---- Convenience constructors ----

  (define (make-rename-attr from to) (make-rename-op from to))
  (define (make-add-index attr) (make-add-index-op attr))
  (define (make-remove-index attr) (make-remove-index-op attr))
  (define (make-merge-attr from into fn) (make-merge-op from into fn))
  (define (make-split-attr from a b fn) (make-split-op from a b fn))

  ;; ---- Execute migration ----

  (define (migrate! conn migration-obj)
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
            [else (error 'migrate! "Unknown migration op" op)]))
        ops)))

  ;; ---- Individual migration operations ----

  (define (migrate-rename! conn schema from-ident to-ident)
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

  (define (migrate-add-index! conn schema attr-ident)
    ;; Update the schema attribute to set :db/index true
    (let ([attr (schema-lookup-by-ident schema attr-ident)])
      (unless attr (error 'migrate-add-index! "Attribute not found" attr-ident))
      ;; Re-transact schema with index flag
      (transact! conn
        (list `((db/ident . ,attr-ident)
                (db/valueType . ,(db-attribute-value-type attr))
                (db/cardinality . ,(db-attribute-cardinality attr))
                (db/index . #t))))))

  (define (migrate-remove-index! conn schema attr-ident)
    (let ([attr (schema-lookup-by-ident schema attr-ident)])
      (unless attr (error 'migrate-remove-index! "Attribute not found" attr-ident))
      (transact! conn
        (list `((db/ident . ,attr-ident)
                (db/valueType . ,(db-attribute-value-type attr))
                (db/cardinality . ,(db-attribute-cardinality attr))
                (db/index . #f))))))

  (define (migrate-merge! conn schema from-ident into-ident merge-fn)
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

  (define (migrate-split! conn schema from-ident into-a into-b split-fn)
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

  ;; ---- Planning and dry-run ----

  (define (migration-plan migration-obj)
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
             [else "UNKNOWN"]))
         (migration-operations migration-obj)))

  (define (migration-dry-run conn migration-obj)
    ;; Report what would change without executing
    (let ([plan (migration-plan migration-obj)])
      (display "Migration plan:\n")
      (for-each (lambda (step) (display (format "  ~a\n" step))) plan)
      plan))

) ;; end library
