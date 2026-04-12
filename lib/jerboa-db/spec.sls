#!chezscheme
;;; (jerboa-db spec) — Entity specs and attribute predicate validation
;;;
;;; A spec defines required attributes for an entity type.
;;; Define a spec by transacting:
;;;   ((spec/ident . person-spec) (spec/attrs . (person/name person/email)))
;;;
;;; Then validate:
;;;   (validate-entity db eid 'person-spec)  → #t or raises error
;;;   (check-entity-spec db eid 'person-spec) → (ok . eid) or (err . missing-list)

(library (jerboa-db spec)
  (export define-spec validate-entity check-entity-spec)

  (import (chezscheme)
          (jerboa-db datom)
          (jerboa-db schema)
          (jerboa-db index protocol)
          (jerboa-db history))

  ;; ---- define-spec ----
  ;; Convenience: transact a spec entity using the connection.
  ;; Returns a tx-op list suitable for passing to transact!

  (define (define-spec spec-ident required-attrs)
    ;; Returns a tx-op (entity map alist) ready for transact!
    (list (cons 'spec/ident spec-ident)
          (cons 'spec/attrs required-attrs)))

  ;; ---- Spec lookup ----
  ;; Find the spec entity with spec/ident = spec-ident.
  ;; Returns a list of required attribute ident symbols, or #f if not found.

  (define (find-spec-attrs db-val spec-ident)
    (let* ([schema  (db-value-schema db-val)]
           [indices (db-value-indices db-val)]
           [avet    (index-set-avet indices)]
           [spec-attr (schema-lookup-by-ident schema 'spec/ident)]
           [attrs-attr (schema-lookup-by-ident schema 'spec/attrs)])
      (unless spec-attr  (error 'find-spec-attrs "spec/ident not in schema"))
      (unless attrs-attr (error 'find-spec-attrs "spec/attrs not in schema"))
      (let* ([spec-aid (db-attribute-id spec-attr)]
             [lo (make-datom 0 spec-aid spec-ident 0 #t)]
             [hi (make-datom (greatest-fixnum) spec-aid spec-ident (greatest-fixnum) #t)]
             [datoms (filter (lambda (d) (db-filter-datom? db-val d))
                             (dbi-range avet lo hi))]
             [spec-entities (resolve-current-eids datoms)])
        (if (null? spec-entities)
            #f
            ;; Use the first matching spec entity
            (let* ([spec-eid (car spec-entities)]
                   [attrs-aid (db-attribute-id attrs-attr)]
                   [eavt (index-set-eavt indices)]
                   [lo2 (make-datom spec-eid attrs-aid +min-val+ 0 #t)]
                   [hi2 (make-datom spec-eid attrs-aid +max-val+ (greatest-fixnum) #t)]

                   [attr-datoms (filter (lambda (d) (db-filter-datom? db-val d))
                                        (dbi-range eavt lo2 hi2))]
                   [current-attr-datoms (resolve-current-datoms attr-datoms)])
              (if (null? current-attr-datoms)
                  '()
                  (datom-v (car current-attr-datoms))))))))

  ;; ---- Entity value check ----
  ;; Check if entity eid has a current value for attr-ident.

  (define (entity-has-attr? db-val eid attr-ident)
    (let* ([schema  (db-value-schema db-val)]
           [indices (db-value-indices db-val)]
           [attr (schema-lookup-by-ident schema attr-ident)])
      (and attr
           (let* ([aid (db-attribute-id attr)]
                  [eavt (index-set-eavt indices)]
                  [lo (make-datom eid aid +min-val+ 0 #t)]
                  [hi (make-datom eid aid +max-val+ (greatest-fixnum) #t)]
                  [datoms (filter (lambda (d) (db-filter-datom? db-val d))
                                   (dbi-range eavt lo hi))]
                  [current (resolve-current-datoms datoms)])
             (not (null? current))))))

  ;; ---- validate-entity ----
  ;; Checks that eid has all required attributes from spec.
  ;; Returns #t on success, raises error on failure.

  (define (validate-entity db-val eid spec-ident)
    (let ([required (find-spec-attrs db-val spec-ident)])
      (unless required
        (error 'validate-entity "Spec not found" spec-ident))
      (let ([missing (filter (lambda (attr-ident)
                               (not (entity-has-attr? db-val eid attr-ident)))
                             required)])
        (if (null? missing)
            #t
            (error 'validate-entity
              (format #f "Entity ~a missing required attributes for spec ~a: ~a"
                      eid spec-ident missing))))))

  ;; ---- check-entity-spec ----
  ;; Returns (ok . eid) on success or (err . missing-attr-list) on failure.

  (define (check-entity-spec db-val eid spec-ident)
    (let ([required (find-spec-attrs db-val spec-ident)])
      (if (not required)
          (cons 'err (list 'spec-not-found spec-ident))
          (let ([missing (filter (lambda (attr-ident)
                                   (not (entity-has-attr? db-val eid attr-ident)))
                                 required)])
            (if (null? missing)
                (cons 'ok eid)
                (cons 'err missing))))))

  ;; ---- Helpers ----

  ;; Get entity IDs from assertion datoms (current state)
  (define (resolve-current-eids datoms)
    (let ([ht (make-hashtable equal-hash equal?)])
      (for-each
        (lambda (d)
          (let ([key (list (datom-e d) (datom-a d) (datom-v d))])
            (let ([existing (hashtable-ref ht key #f)])
              (when (or (not existing) (> (datom-tx d) (datom-tx existing)))
                (hashtable-set! ht key d)))))
        datoms)
      (let-values ([(keys vals) (hashtable-entries ht)])
        (filter-map
          (lambda (d) (and (datom-added? d) (datom-e d)))
          (vector->list vals)))))

  (define (resolve-current-datoms datoms)
    (let ([ht (make-hashtable equal-hash equal?)])
      (for-each
        (lambda (d)
          (let ([key (list (datom-e d) (datom-a d) (datom-v d))])
            (let ([existing (hashtable-ref ht key #f)])
              (when (or (not existing) (> (datom-tx d) (datom-tx existing)))
                (hashtable-set! ht key d)))))
        datoms)
      (let-values ([(keys vals) (hashtable-entries ht)])
        (filter datom-added? (vector->list vals)))))

  (define (filter-map f lst)
    (let loop ([lst lst] [acc '()])
      (if (null? lst) (reverse acc)
          (let ([r (f (car lst))])
            (loop (cdr lst) (if r (cons r acc) acc))))))


) ;; end library
