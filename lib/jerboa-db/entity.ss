#!chezscheme
;;; (jerboa-db entity) — Lazy navigable entity maps
;;;
;;; An entity is a lazy, navigable view of all datoms for a given entity ID.
;;; Attributes are loaded on demand. Ref attributes return other entity objects.

(library (jerboa-db entity)
  (export
    new-entity-map entity-map? entity-map-eid
    entity-get entity-touch entity-keys)

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
          (jerboa-db datom)
          (jerboa-db schema)
          (jerboa-db index protocol)
          (jerboa-db history))

  ;; ---- Entity map ----
  ;; Lazy: attributes are fetched from the index on first access.
  ;; The cache stores materialized attribute values.

  (defstruct entity-map
    (eid            ;; integer
     db             ;; db-value (for index access)
     cache          ;; hashtable: attr-ident -> value(s)
     touched?))     ;; #t if all attributes loaded

  (def (new-entity-map eid db)
    (make-entity-map eid db (make-eq-hashtable) #f))

  ;; ---- Attribute access ----

  (def (entity-get ent attr-ident)
    (let ([cache (entity-map-cache ent)])
      (if (hashtable-contains? cache attr-ident)
          (hashtable-ref cache attr-ident #f)
          ;; Load from index
          (let* ([db (entity-map-db ent)]
                 [schema (db-value-schema db)]
                 [attr (schema-lookup-by-ident schema attr-ident)])
            (if (not attr)
                #f
                (let* ([eid (entity-map-eid ent)]
                       [aid (db-attribute-id attr)]
                       [eavt (db-resolve-index db 'eavt)]
                       [lo (make-datom eid aid +min-val+ 0 #t)]
                       [hi (make-datom eid aid +max-val+ (greatest-fixnum) #t)]
                       [raw (dbi-range eavt lo hi)]
                       [filtered (filter (lambda (d) (db-filter-datom? db d)) raw)]
                       ;; Resolve current state: group by value, keep highest-tx
                       [datoms (let ([ht (make-hashtable equal-hash equal?)])
                                 (for-each
                                   (lambda (d)
                                     (let ([v (datom-v d)])
                                       (let ([ex (hashtable-ref ht v #f)])
                                         (when (or (not ex) (> (datom-tx d) (datom-tx ex)))
                                           (hashtable-set! ht v d)))))
                                   filtered)
                                 (let-values ([(ks vs) (hashtable-entries ht)])
                                   (filter datom-added? (vector->list vs))))]
                       [values (map datom-v datoms)]
                       [result (cond
                                 [(null? values) #f]
                                 [(eq? (db-attribute-cardinality attr)
                                       'db.cardinality/one)
                                  ;; Return single value; for refs, wrap in entity
                                  (let ([v (car values)])
                                    (if (eq? (db-attribute-value-type attr) 'db.type/ref)
                                        (new-entity-map v db)
                                        v))]
                                 [else
                                  ;; Cardinality many: return list
                                  (if (eq? (db-attribute-value-type attr) 'db.type/ref)
                                      (map (lambda (v) (new-entity-map v db)) values)
                                      values)])])
                  (hashtable-set! cache attr-ident result)
                  result))))))

  ;; ---- Touch: eagerly load all attributes ----

  (def (entity-touch ent)
    (when (not (entity-map-touched? ent))
      (let* ([db (entity-map-db ent)]
             [schema (db-value-schema db)]
             [eid (entity-map-eid ent)]
             [eavt (db-resolve-index db 'eavt)]
             [lo (make-datom eid 0 +min-val+ 0 #t)]
             [hi (make-datom eid (greatest-fixnum) +max-val+ (greatest-fixnum) #t)]
             [raw (dbi-range eavt lo hi)]
             [filtered (filter (lambda (d) (db-filter-datom? db d)) raw)]
             ;; Resolve current state per (a, v)
             [datoms (let ([ht (make-hashtable equal-hash equal?)])
                       (for-each
                         (lambda (d)
                           (let ([key (cons (datom-a d) (datom-v d))])
                             (let ([ex (hashtable-ref ht key #f)])
                               (when (or (not ex) (> (datom-tx d) (datom-tx ex)))
                                 (hashtable-set! ht key d)))))
                         filtered)
                       (let-values ([(ks vs) (hashtable-entries ht)])
                         (filter datom-added? (vector->list vs))))]
             [cache (entity-map-cache ent)])
        ;; Group by attribute and store
        (for-each
          (lambda (d)
            (let ([attr (schema-lookup-by-id schema (datom-a d))])
              (when attr
                (let ([ident (db-attribute-ident attr)]
                      [v (datom-v d)])
                  (if (eq? (db-attribute-cardinality attr) 'db.cardinality/one)
                      (hashtable-set! cache ident v)
                      (hashtable-update! cache ident
                        (lambda (existing) (cons v existing))
                        '()))))))
          datoms)
        (entity-map-touched?-set! ent #t)))
    ;; Return as alist
    (let ([cache (entity-map-cache ent)]
          [result '()])
      (let-values ([(keys vals) (hashtable-entries cache)])
        (cons (cons 'db/id (entity-map-eid ent))
              (map cons (vector->list keys) (vector->list vals))))))

  ;; ---- Get all attribute keys ----

  (def (entity-keys ent)
    (entity-touch ent)  ;; ensure loaded
    (let-values ([(keys vals) (hashtable-entries (entity-map-cache ent))])
      (vector->list keys)))

) ;; end library
