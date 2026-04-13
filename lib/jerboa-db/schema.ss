#!chezscheme
;;; (jerboa-db schema) — Attribute definitions, validation, and interning
;;;
;;; Every attribute has a schema: type, cardinality, uniqueness, indexing.
;;; Attributes are interned from keywords (symbols) to integer IDs for
;;; efficient index key comparison.

(library (jerboa-db schema)
  (export
    ;; Attribute record
    make-db-attribute db-attribute?
    db-attribute-ident db-attribute-id db-attribute-value-type
    db-attribute-cardinality db-attribute-unique db-attribute-index?
    db-attribute-is-component? db-attribute-doc db-attribute-no-history?
    db-attribute-tuple-attrs db-attribute-fulltext?

    ;; Schema registry
    new-schema-registry schema-registry?
    schema-intern-attr! schema-lookup-by-ident schema-lookup-by-id
    schema-install-attribute! schema-all-attributes
    schema-next-attr-id

    ;; Built-in attribute idents and IDs
    +db/ident+ +db/valueType+ +db/cardinality+ +db/unique+
    +db/index+ +db/doc+ +db/isComponent+ +db/noHistory+
    +db/txInstant+ +db/id+
    +db/tupleAttrs+ +db/ensure+ +db/fulltext+
    +spec/ident+ +spec/attrs+
    +first-user-attr-id+

    ;; Value type predicates
    valid-value-type? value-matches-type? coerce-value
    cardinality-one? cardinality-many? ref-type? indexed-attr? avet-eligible?

    ;; Bootstrap
    bootstrap-schema!)

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
          (jerboa prelude))

  ;; ---- Attribute record ----

  (define-record-type db-attribute
    (fields ident          ;; symbol (e.g., person/name)
            id             ;; integer (interned for index keys)
            value-type     ;; symbol: db.type/string, db.type/long, etc.
            cardinality    ;; symbol: db.cardinality/one or db.cardinality/many
            unique         ;; #f, db.unique/value, or db.unique/identity
            index?         ;; boolean — populate AVET?
            is-component?  ;; boolean — cascade retractions?
            doc            ;; string or #f
            no-history?    ;; boolean — skip history?
            tuple-attrs    ;; list of component attr ident symbols for composite, or #f
            fulltext?))    ;; boolean — enable fulltext indexing

  ;; ---- Schema registry ----
  ;; Maps ident (symbol) <-> id (integer), and id -> db-attribute.

  (define-record-type schema-registry
    (fields ident->id          ;; hashtable: symbol -> integer
            id->attr           ;; hashtable: integer -> db-attribute
            (mutable next-id)  ;; integer: next attribute ID to assign
            ))

  ;; The raw constructor from defstruct is make-schema-registry.
  ;; We wrap it to add bootstrapping.
  (def (new-schema-registry)
    (let ([r (make-schema-registry
               (make-hashtable symbol-hash eq?)
               (make-eq-hashtable)
               0)])
      (bootstrap-schema! r)
      r))

  (def (schema-next-attr-id reg)
    (schema-registry-next-id reg))

  ;; Intern a new attribute ident -> id mapping
  (def (schema-intern-attr! reg ident)
    (let ([ht (schema-registry-ident->id reg)])
      (or (hashtable-ref ht ident #f)
          (let ([id (schema-registry-next-id reg)])
            (hashtable-set! ht ident id)
            (schema-registry-next-id-set! reg (+ id 1))
            id))))

  ;; Install a fully defined attribute
  (def (schema-install-attribute! reg attr)
    (let ([ident (db-attribute-ident attr)]
          [id (db-attribute-id attr)])
      (hashtable-set! (schema-registry-ident->id reg) ident id)
      (hashtable-set! (schema-registry-id->attr reg) id attr)
      (when (>= id (schema-registry-next-id reg))
        (schema-registry-next-id-set! reg (+ id 1)))))

  (def (schema-lookup-by-ident reg ident)
    (let ([id (hashtable-ref (schema-registry-ident->id reg) ident #f)])
      (and id (hashtable-ref (schema-registry-id->attr reg) id #f))))

  (def (schema-lookup-by-id reg id)
    (hashtable-ref (schema-registry-id->attr reg) id #f))

  (def (schema-all-attributes reg)
    (let-values ([(keys vals) (hashtable-entries (schema-registry-id->attr reg))])
      (vector->list vals)))

  ;; ---- Built-in attribute idents ----

  (def +db/ident+       'db/ident)
  (def +db/valueType+   'db/valueType)
  (def +db/cardinality+ 'db/cardinality)
  (def +db/unique+      'db/unique)
  (def +db/index+       'db/index)
  (def +db/doc+         'db/doc)
  (def +db/isComponent+ 'db/isComponent)
  (def +db/noHistory+   'db/noHistory)
  (def +db/txInstant+   'db/txInstant)
  (def +db/id+          'db/id)
  (def +db/tupleAttrs+  'db/tupleAttrs)
  (def +db/ensure+      'db/ensure)
  (def +db/fulltext+    'db/fulltext)
  (def +spec/ident+     'spec/ident)
  (def +spec/attrs+     'spec/attrs)

  (def +first-user-attr-id+ 20)

  ;; ---- Bootstrap built-in schema attributes ----

  (def (bootstrap-schema! reg)
    (define (install! id ident vtype card)
      (let ([attr (make-db-attribute ident id vtype card #f #t #f #f #f #f #f)])
        (schema-install-attribute! reg attr)))
    ;; Reserve IDs 0-19 for system attributes
    (install! 0  +db/ident+       'db.type/keyword 'db.cardinality/one)
    (install! 1  +db/valueType+   'db.type/keyword 'db.cardinality/one)
    (install! 2  +db/cardinality+ 'db.type/keyword 'db.cardinality/one)
    (install! 3  +db/unique+      'db.type/keyword 'db.cardinality/one)
    (install! 4  +db/index+       'db.type/boolean 'db.cardinality/one)
    (install! 5  +db/doc+         'db.type/string  'db.cardinality/one)
    (install! 6  +db/isComponent+ 'db.type/boolean 'db.cardinality/one)
    (install! 7  +db/noHistory+   'db.type/boolean 'db.cardinality/one)
    (install! 8  +db/txInstant+   'db.type/instant 'db.cardinality/one)
    (install! 9  +db/tupleAttrs+  'db.type/any     'db.cardinality/one)
    (install! 10 +db/ensure+      'db.type/keyword 'db.cardinality/one)
    (install! 11 +db/fulltext+    'db.type/boolean 'db.cardinality/one)
    (install! 12 +spec/ident+     'db.type/keyword 'db.cardinality/one)
    (install! 13 +spec/attrs+     'db.type/any     'db.cardinality/one)
    (schema-registry-next-id-set! reg +first-user-attr-id+))

  ;; ---- Value type validation ----

  (def (valid-value-type? vtype)
    (memq vtype '(db.type/string db.type/long db.type/double
                  db.type/boolean db.type/instant db.type/uuid
                  db.type/ref db.type/keyword db.type/bytes
                  db.type/symbol db.type/tuple db.type/any)))

  (def (value-matches-type? vtype value)
    (case vtype
      [(db.type/string)  (string? value)]
      [(db.type/long)    (and (integer? value) (exact? value))]
      [(db.type/double)  (flonum? value)]
      [(db.type/boolean) (boolean? value)]
      [(db.type/instant) (or (integer? value) (time? value))]
      [(db.type/uuid)    (string? value)]  ;; UUID stored as string
      [(db.type/ref)     (and (integer? value) (> value 0))]
      [(db.type/keyword) (symbol? value)]
      [(db.type/bytes)   (bytevector? value)]
      [(db.type/symbol)  (symbol? value)]
      [(db.type/tuple)   (list? value)]
      [(db.type/any)     #t]
      [else #f]))

  (def (coerce-value vtype raw)
    ;; Attempt basic coercion; return raw if it already matches.
    (if (value-matches-type? vtype raw)
        raw
        (case vtype
          [(db.type/long)
           (cond [(flonum? raw) (exact (truncate raw))]
                 [(string? raw) (or (string->number raw) raw)]
                 [else raw])]
          [(db.type/double)
           (cond [(fixnum? raw) (inexact raw)]
                 [(string? raw) (or (string->number raw) raw)]
                 [else raw])]
          [(db.type/string)
           (cond [(symbol? raw) (symbol->string raw)]
                 [(number? raw) (number->string raw)]
                 [else raw])]
          [(db.type/keyword)
           (cond [(string? raw) (string->symbol raw)]
                 [else raw])]
          [else raw])))

  ;; ---- Convenience predicates ----

  (def (cardinality-one? attr)
    (eq? (db-attribute-cardinality attr) 'db.cardinality/one))

  (def (cardinality-many? attr)
    (eq? (db-attribute-cardinality attr) 'db.cardinality/many))

  (def (ref-type? attr)
    (eq? (db-attribute-value-type attr) 'db.type/ref))

  (def (indexed-attr? attr)
    (or (db-attribute-index? attr)
        (db-attribute-unique attr)))

  ;; avet-eligible?: should this attribute be stored in the AVET index?
  ;; True for all scalar (non-ref, non-tuple) attributes.
  ;; Ref types use VAET for reverse lookups; tuple/any values are not AVET-indexable.
  (def (avet-eligible? attr)
    (and attr
         (not (memq (db-attribute-value-type attr)
                    '(db.type/ref db.type/tuple db.type/any)))))

) ;; end library
