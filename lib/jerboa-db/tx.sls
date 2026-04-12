#!chezscheme
;;; (jerboa-db tx) — Transaction processing
;;;
;;; Processes batches of assertions and retractions atomically.
;;; Handles: tempid resolution, cardinality/one auto-retraction,
;;; unique/identity upsert, compare-and-swap, entity retraction,
;;; schema validation, and transaction metadata.

(library (jerboa-db tx)
  (export
    make-tx-report tx-report? tx-report-db-before tx-report-db-after
    tx-report-tx-data tx-report-tempids

    process-transaction tempid tempid?)

  (import (chezscheme)
          (jerboa-db datom)
          (jerboa-db schema)
          (jerboa-db index protocol)
          (jerboa-db history))

  ;; ---- Transaction report ----

  (define-record-type tx-report
    (fields db-before  ;; db-value before transaction
            db-after   ;; db-value after transaction
            tx-data    ;; list of datoms produced
            tempids))  ;; alist: tempid -> permanent eid

  ;; ---- Tempids ----
  ;; Negative integers serve as temporary IDs within a transaction.

  (define tempid-counter 0)

  (define (tempid)
    (set! tempid-counter (- tempid-counter 1))
    tempid-counter)

  (define (tempid? x)
    (and (integer? x) (< x 0)))

  ;; ---- Transaction processing ----

  ;; Process a transaction against a db-value.
  ;; tx-ops: list of operations (alists for entity maps, lists for commands)
  ;; Returns a tx-report.
  ;;
  ;; State threaded through processing:
  ;;   - next-eid: next entity ID to assign
  ;;   - next-tx: the transaction entity ID
  ;;   - tempid-map: alist of tempid -> permanent eid
  ;;   - datoms: accumulated datoms to write

  (define (process-transaction db tx-ops next-eid-cell)
    (let* ([schema (db-value-schema db)]
           [indices (db-value-indices db)]
           [tx-id (+ (db-value-basis-tx db) 1)]
           [next-eid (car next-eid-cell)]
           [tempid-map '()]
           [produced-datoms '()])

      ;; --- Schema lookup cache ---
      ;; Avoids repeated registry scans for the same attribute within one transaction.
      (define schema-cache (make-hashtable symbol-hash eq?))
      (define (lookup-attr ident)
        (or (hashtable-ref schema-cache ident #f)
            (let ([attr (schema-lookup-by-ident schema ident)])
              (when attr (hashtable-set! schema-cache ident attr))
              attr)))

      ;; --- Helpers ---

      (define (resolve-eid raw-eid)
        ;; Resolve a tempid to a permanent eid, or return raw-eid if already permanent.
        ;; Also handles lookup refs: (attr-ident value) — resolves to existing entity.
        (cond
          [(and (pair? raw-eid)
                (symbol? (car raw-eid))
                (= (length raw-eid) 2))
           ;; Lookup ref: (attr-ident value)
           (let* ([attr-ident (car raw-eid)]
                  [val (cadr raw-eid)]
                  [attr (lookup-attr attr-ident)])
             (unless attr
               (error 'transact! "Unknown attribute in lookup ref" attr-ident))
             (unless (db-attribute-unique attr)
               (error 'transact! "Lookup ref attribute must be unique" attr-ident))
             (let ([found-eid (find-entity-by-unique attr val)])
               (unless found-eid
                 (error 'transact!
                   (format "Lookup ref found no entity: ~a = ~a" attr-ident val)))
               found-eid))]
          [(tempid? raw-eid)
           (let ([found (assv raw-eid tempid-map)])
             (if found
                 (cdr found)
                 ;; Check for upsert before allocating new eid
                 (let ([eid next-eid])
                   (set! next-eid (+ next-eid 1))
                   (set! tempid-map (cons (cons raw-eid eid) tempid-map))
                   eid)))]
          [(not raw-eid)
           ;; No db/id provided — allocate fresh
           (let ([eid next-eid])
             (set! next-eid (+ next-eid 1))
             eid)]
          [else raw-eid]))

      ;; Check for upsert: if an attribute is :db.unique/identity and a value
      ;; already exists, return the existing entity ID.
      (define (check-upsert alist)
        (let loop ([pairs alist])
          (if (null? pairs)
              #f
              (let* ([pair (car pairs)]
                     [attr-ident (car pair)]
                     [val (cdr pair)]
                     [attr (lookup-attr attr-ident)])
                (if (and attr (eq? (db-attribute-unique attr) 'db.unique/identity))
                    (let ([existing (find-entity-by-unique attr val)])
                      (or existing (loop (cdr pairs))))
                    (loop (cdr pairs)))))))

      ;; Find an entity that has a unique attribute with the given value.
      ;; Scans AVET index. Resolves current state: the value must not
      ;; have been subsequently retracted.
      (define (find-entity-by-unique attr val)
        (let* ([aid (db-attribute-id attr)]
               [avet (index-set-avet indices)]
               [lo (make-datom 0 aid val 0 #t)]
               [hi (make-datom (greatest-fixnum) aid val (greatest-fixnum) #t)]
               [found (dbi-range avet lo hi)])
          ;; Group by entity, keep only those whose highest-tx datom is an assertion
          (let ([ht (make-hashtable equal-hash equal?)])
            (for-each
              (lambda (d)
                (when (db-filter-datom? db d)
                  (let ([eid (datom-e d)])
                    (let ([existing (hashtable-ref ht eid #f)])
                      (when (or (not existing)
                                (> (datom-tx d) (datom-tx existing)))
                        (hashtable-set! ht eid d))))))
              found)
            (let-values ([(keys vals) (hashtable-entries ht)])
              (let loop ([i 0])
                (if (>= i (vector-length vals))
                    #f
                    (let ([d (vector-ref vals i)])
                      (if (datom-added? d)
                          (datom-e d)
                          (loop (+ i 1))))))))))

      ;; Find current value for entity + attribute (cardinality/one)
      ;; Returns the latest assertion datom if the value is still live
      ;; (i.e., not subsequently retracted), or #f if no current value exists.
      (define (current-value eid aid)
        (let* ([eavt (index-set-eavt indices)]
               [lo (make-datom eid aid +min-val+ 0 #t)]
               [hi (make-datom eid aid +max-val+ (greatest-fixnum) #t)]
               [found (dbi-range eavt lo hi)])
          ;; Group by value, find the highest-tx datom per value.
          ;; If that datom is an assertion and passes time-filter, it's current.
          (let ([ht (make-hashtable equal-hash equal?)])
            (for-each
              (lambda (d)
                (when (db-filter-datom? db d)
                  (let ([v (datom-v d)])
                    (let ([existing (hashtable-ref ht v #f)])
                      (when (or (not existing)
                                (> (datom-tx d) (datom-tx existing)))
                        (hashtable-set! ht v d))))))
              found)
            ;; Return the first live (asserted) value we find
            (let-values ([(keys vals) (hashtable-entries ht)])
              (let loop ([i 0])
                (if (>= i (vector-length vals))
                    #f
                    (let ([d (vector-ref vals i)])
                      (if (datom-added? d)
                          d
                          (loop (+ i 1))))))))))

      (define (emit-datom! e a v added?)
        (set! produced-datoms
              (cons (make-datom e a v tx-id added?) produced-datoms)))

      ;; Check uniqueness constraint (db.unique/value)
      (define (check-unique-value! attr eid val)
        (when (eq? (db-attribute-unique attr) 'db.unique/value)
          (let ([existing (find-entity-by-unique attr val)])
            (when (and existing (not (= existing eid)))
              (error 'transact!
                (format "Unique constraint violation: ~a = ~a already on entity ~a"
                        (db-attribute-ident attr) val existing))))))

      ;; --- Process individual operations ---

      (define (process-entity-map! emap)
        ;; emap is an alist: ((attr-ident . value) ...)
        ;; Check for upsert first
        (let* ([raw-eid (cond [(assq 'db/id emap) => cdr] [else #f])]
               [other-pairs (filter (lambda (p) (not (eq? (car p) 'db/id))) emap)]
               [upsert-eid (check-upsert other-pairs)]
               [eid (cond
                      [upsert-eid
                       ;; Upsert: use existing entity, map tempid if needed
                       (when (and raw-eid (tempid? raw-eid))
                         (set! tempid-map (cons (cons raw-eid upsert-eid) tempid-map)))
                       upsert-eid]
                      [else (resolve-eid raw-eid)])])
          ;; Process each attribute
          (for-each
            (lambda (pair)
              (let* ([attr-ident (car pair)]
                     [val (cdr pair)]
                     [attr (lookup-attr attr-ident)])
                (unless attr
                  ;; Auto-intern unknown attributes as schema if they look like schema
                  ;; For now, error on unknown attributes
                  (error 'transact!
                    (format "Unknown attribute: ~a" attr-ident)))
                (let ([aid (db-attribute-id attr)]
                      [cval (coerce-value (db-attribute-value-type attr) val)])
                  ;; Validate
                  (unless (value-matches-type? (db-attribute-value-type attr) cval)
                    (error 'transact!
                      (format "Type mismatch for ~a: expected ~a, got ~a"
                              attr-ident (db-attribute-value-type attr) cval)))
                  ;; Check uniqueness
                  (check-unique-value! attr eid cval)
                  ;; Cardinality/one: auto-retract old value
                  (when (cardinality-one? attr)
                    (let ([old (current-value eid aid)])
                      (when (and old (not (equal? (datom-v old) cval)))
                        (emit-datom! eid aid (datom-v old) #f))))
                  ;; Handle ref values:
                  ;; - tempids: resolve to permanent eid
                  ;; - nested maps (alists): recursively process, use child eid
                  ;; - lookup refs: resolve via unique attribute
                  (let ([final-val
                          (cond
                            ;; Nested map: ref-type + component + value is an alist
                            [(and (ref-type? attr)
                                  (db-attribute-is-component? attr)
                                  (pair? cval)
                                  (pair? (car cval))
                                  (symbol? (caar cval)))
                             ;; Process nested entity map, get its eid
                             (let ([child-eid (resolve-eid (cond [(assq 'db/id cval) => cdr]
                                                                  [else #f]))])
                               ;; Process the nested map
                               (process-entity-map!
                                 (cons (cons 'db/id child-eid)
                                       (filter (lambda (p) (not (eq? (car p) 'db/id))) cval)))
                               child-eid)]
                            ;; Lookup ref in value position
                            [(and (ref-type? attr) (pair? cval)
                                  (symbol? (car cval)) (= (length cval) 2))
                             (resolve-eid cval)]
                            ;; Tempid in value position
                            [(and (ref-type? attr) (tempid? cval))
                             (resolve-eid cval)]
                            [else cval])])
                    (emit-datom! eid aid final-val #t)))))
            other-pairs)))

      (define (process-add! eid-raw attr-ident val)
        (let* ([eid (resolve-eid eid-raw)]
               [attr (lookup-attr attr-ident)])
          (unless attr (error 'transact! "Unknown attribute" attr-ident))
          (let* ([aid (db-attribute-id attr)]
                 [cval (coerce-value (db-attribute-value-type attr) val)]
                 ;; Handle lookup refs and tempids in ref values
                 [final-val (cond
                              [(and (ref-type? attr) (pair? cval)
                                    (symbol? (car cval)) (= (length cval) 2))
                               (resolve-eid cval)]
                              [(and (ref-type? attr) (tempid? cval))
                               (resolve-eid cval)]
                              [else cval])])
            (check-unique-value! attr eid final-val)
            (when (cardinality-one? attr)
              (let ([old (current-value eid aid)])
                (when (and old (not (equal? (datom-v old) final-val)))
                  (emit-datom! eid aid (datom-v old) #f))))
            (emit-datom! eid aid final-val #t))))

      (define (process-retract! eid attr-ident val)
        (let ([attr (lookup-attr attr-ident)])
          (unless attr (error 'transact! "Unknown attribute" attr-ident))
          (emit-datom! eid (db-attribute-id attr) val #f)))

      (define (process-retract-entity! eid)
        ;; Retract all current datoms for this entity.
        ;; Resolve current state: only retract values whose latest datom is an assertion.
        (let* ([eavt (index-set-eavt indices)]
               [lo (make-datom eid 0 +min-val+ 0 #t)]
               [hi (make-datom eid (greatest-fixnum) +max-val+ (greatest-fixnum) #t)]
               [found (dbi-range eavt lo hi)])
          ;; Group by (a, v), find latest tx per group
          (let ([ht (make-hashtable equal-hash equal?)])
            (for-each
              (lambda (d)
                (when (db-filter-datom? db d)
                  (let ([key (cons (datom-a d) (datom-v d))])
                    (let ([existing (hashtable-ref ht key #f)])
                      (when (or (not existing)
                                (> (datom-tx d) (datom-tx existing)))
                        (hashtable-set! ht key d))))))
              found)
            ;; Retract only live values
            (let-values ([(keys vals) (hashtable-entries ht)])
              (vector-for-each
                (lambda (d)
                  (when (datom-added? d)
                    (emit-datom! (datom-e d) (datom-a d) (datom-v d) #f)))
                vals)))))

      (define (process-cas! eid attr-ident old-val new-val)
        (let* ([attr (lookup-attr attr-ident)]
               [aid (db-attribute-id attr)]
               [cur (current-value eid aid)])
          (unless (and cur (equal? (datom-v cur) old-val))
            (error 'transact!
              (format "CAS failed for entity ~a attr ~a: expected ~a, got ~a"
                      eid attr-ident old-val
                      (and cur (datom-v cur)))))
          (emit-datom! eid aid old-val #f)
          (emit-datom! eid aid new-val #t)))

      ;; Effective value for composite tuple generation:
      ;; checks pending produced-datoms first, then falls back to current index value.
      (define (effective-value-for-entity eid attr-ident)
        (let ([attr (lookup-attr attr-ident)])
          (and attr
               (let* ([aid (db-attribute-id attr)]
                      [pending (filter (lambda (d)
                                         (and (= (datom-e d) eid)
                                              (= (datom-a d) aid)
                                              (datom-added? d)))
                                       produced-datoms)])
                 (if (pair? pending)
                     (datom-v (car (reverse pending)))
                     (let ([cur (current-value eid aid)])
                       (and cur (datom-v cur))))))))

      ;; --- Main processing loop ---

      ;; Process each operation
      (for-each
        (lambda (op)
          (cond
            ;; Entity map (alist with pairs)
            [(and (pair? op) (pair? (car op)) (symbol? (caar op)))
             (process-entity-map! op)]
            ;; Command list
            [(and (pair? op) (symbol? (car op)))
             (case (car op)
               [(db/add)
                (process-add! (cadr op) (caddr op) (cadddr op))]
               [(db/retract)
                (process-retract! (cadr op) (caddr op) (cadddr op))]
               [(db/retractEntity)
                (process-retract-entity! (cadr op))]
               [(db/cas)
                (process-cas! (cadr op) (caddr op) (cadddr op) (car (cddddr op)))]
               [else (error 'transact! "Unknown operation" (car op))])]
            [else (error 'transact! "Invalid transaction operation" op)]))
        tx-ops)

      ;; Add transaction metadata: :db/txInstant
      (let ([tx-instant-attr (lookup-attr +db/txInstant+)])
        (when tx-instant-attr
          (emit-datom! tx-id (db-attribute-id tx-instant-attr)
                       (time-second (current-time)) #t)))

      ;; ---- Composite tuple generation ----
      ;; For each entity that had changes, check if any changed attribute is a
      ;; component of a composite tuple attribute. If so, gather all component
      ;; values and emit the composite datom.

      (let* ([affected-eids
              (let ([seen (make-hashtable equal-hash equal?)])
                (for-each (lambda (d) (hashtable-set! seen (datom-e d) #t))
                          produced-datoms)
                (let-values ([(keys _) (hashtable-entries seen)])
                  (vector->list keys)))]
             [composite-attrs
              (filter (lambda (a) (db-attribute-tuple-attrs a))
                      (schema-all-attributes schema))])
        (for-each
          (lambda (eid)
            (for-each
              (lambda (comp-attr)
                (let* ([components (db-attribute-tuple-attrs comp-attr)]
                       ;; Collect component attribute IDs
                       [comp-aids
                        (let loop ([l components] [acc '()])
                          (if (null? l) (reverse acc)
                              (let ([ca (lookup-attr (car l))])
                                (loop (cdr l)
                                      (if ca (cons (db-attribute-id ca) acc) acc)))))]
                       ;; Check if any component was changed for this entity
                       [changed?
                        (let loop ([ds produced-datoms])
                          (and (pair? ds)
                               (or (and (= (datom-e (car ds)) eid)
                                        (memv (datom-a (car ds)) comp-aids))
                                   (loop (cdr ds)))))])
                  (when changed?
                    ;; Gather all component values
                    (let ([vals (map (lambda (cid)
                                      (effective-value-for-entity eid cid))
                                    components)])
                      ;; Only emit if ALL components have values
                      (when (not (memv #f vals))
                        ;; Retract old composite value if any
                        (let* ([caid (db-attribute-id comp-attr)]
                               [old (current-value eid caid)])
                          (when old (emit-datom! eid caid (datom-v old) #f)))
                        ;; Emit new composite tuple (list of values)
                        (emit-datom! eid (db-attribute-id comp-attr) vals #t))))))
              composite-attrs))
          affected-eids))

      ;; Write all datoms to indices
      (let ([final-datoms (reverse produced-datoms)])
        (for-each
          (lambda (d)
            ;; Always write to EAVT and AEVT
            (dbi-add! (index-set-eavt indices) d)
            (dbi-add! (index-set-aevt indices) d)
            ;; Write to AVET for all scalar attributes (enables fast value lookups)
            (let ([attr (schema-lookup-by-id schema (datom-a d))])
              (when (avet-eligible? attr)
                (dbi-add! (index-set-avet indices) d))
              ;; Write to VAET if attribute is ref type
              (when (and attr (ref-type? attr))
                (dbi-add! (index-set-vaet indices) d))))
          final-datoms)

        ;; Update entity ID counter
        (set-car! next-eid-cell next-eid)

        ;; Build new db-value
        (let ([db-after (make-db-value tx-id indices schema #f #f #f)])
          (make-tx-report db db-after final-datoms tempid-map)))))

  ;; ---- Schema transaction handling ----
  ;; When transacting schema attributes (db/ident, db/valueType, etc.),
  ;; materialize them into the schema registry.
  ;; This is called by core.sls after process-transaction.

) ;; end library
