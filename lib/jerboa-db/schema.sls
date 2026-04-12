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
    cardinality-one? cardinality-many? ref-type? indexed-attr?

    ;; Bootstrap
    bootstrap-schema!)

  (import (chezscheme))

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
    (fields (mutable ident->id)    ;; hashtable: symbol -> integer
            (mutable id->attr)     ;; hashtable: integer -> db-attribute
            (mutable next-id)))    ;; integer: next attribute ID to assign

  ;; The raw constructor from define-record-type is make-schema-registry.
  ;; We wrap it to add bootstrapping.
  (define (new-schema-registry)
    (let ([r (make-schema-registry
               (make-hashtable symbol-hash eq?)
               (make-eq-hashtable)
               0)])
      (bootstrap-schema! r)
      r))

  (define (schema-next-attr-id reg)
    (schema-registry-next-id reg))

  ;; Intern a new attribute ident -> id mapping
  (define (schema-intern-attr! reg ident)
    (let ([ht (schema-registry-ident->id reg)])
      (or (hashtable-ref ht ident #f)
          (let ([id (schema-registry-next-id reg)])
            (hashtable-set! ht ident id)
            (schema-registry-next-id-set! reg (+ id 1))
            id))))

  ;; Install a fully defined attribute
  (define (schema-install-attribute! reg attr)
    (let ([ident (db-attribute-ident attr)]
          [id (db-attribute-id attr)])
      (hashtable-set! (schema-registry-ident->id reg) ident id)
      (hashtable-set! (schema-registry-id->attr reg) id attr)
      (when (>= id (schema-registry-next-id reg))
        (schema-registry-next-id-set! reg (+ id 1)))))

  (define (schema-lookup-by-ident reg ident)
    (let ([id (hashtable-ref (schema-registry-ident->id reg) ident #f)])
      (and id (hashtable-ref (schema-registry-id->attr reg) id #f))))

  (define (schema-lookup-by-id reg id)
    (hashtable-ref (schema-registry-id->attr reg) id #f))

  (define (schema-all-attributes reg)
    (let-values ([(keys vals) (hashtable-entries (schema-registry-id->attr reg))])
      (vector->list vals)))

  ;; ---- Built-in attribute idents ----

  (define +db/ident+       'db/ident)
  (define +db/valueType+   'db/valueType)
  (define +db/cardinality+ 'db/cardinality)
  (define +db/unique+      'db/unique)
  (define +db/index+       'db/index)
  (define +db/doc+         'db/doc)
  (define +db/isComponent+ 'db/isComponent)
  (define +db/noHistory+   'db/noHistory)
  (define +db/txInstant+   'db/txInstant)
  (define +db/id+          'db/id)
  (define +db/tupleAttrs+  'db/tupleAttrs)
  (define +db/ensure+      'db/ensure)
  (define +db/fulltext+    'db/fulltext)
  (define +spec/ident+     'spec/ident)
  (define +spec/attrs+     'spec/attrs)

  (define +first-user-attr-id+ 20)

  ;; ---- Bootstrap built-in schema attributes ----

  (define (bootstrap-schema! reg)
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

  (define (valid-value-type? vtype)
    (memq vtype '(db.type/string db.type/long db.type/double
                  db.type/boolean db.type/instant db.type/uuid
                  db.type/ref db.type/keyword db.type/bytes
                  db.type/symbol db.type/tuple db.type/any)))

  (define (value-matches-type? vtype value)
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

  (define (coerce-value vtype raw)
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

  (define (cardinality-one? attr)
    (eq? (db-attribute-cardinality attr) 'db.cardinality/one))

  (define (cardinality-many? attr)
    (eq? (db-attribute-cardinality attr) 'db.cardinality/many))

  (define (ref-type? attr)
    (eq? (db-attribute-value-type attr) 'db.type/ref))

  (define (indexed-attr? attr)
    (or (db-attribute-index? attr)
        (db-attribute-unique attr)))

) ;; end library
