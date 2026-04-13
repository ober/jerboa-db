#!chezscheme
;;; (jerboa-db query pull) — Pull API implementation
;;;
;;; Retrieves entity data as nested alists, walking relationships
;;; according to a pull pattern. Replaces the need for ORMs.

(library (jerboa-db query pull)
  (export pull-entity pull-many)

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

  ;; ---- Pull pattern syntax ----
  ;; pattern := (attr-spec ...)
  ;; attr-spec := ident                       — simple attribute
  ;;            | *                            — wildcard (all attributes)
  ;;            | (ident sub-pattern)          — nested (follow refs)
  ;;            | (ident :as alias)            — rename
  ;;            | (ident :limit n)             — limit cardinality/many
  ;;            | (ident :default val)         — default value
  ;;            | (ident :limit n sub-pattern) — nested with limit

  ;; ---- Resolve current values for an entity ----

  (def (entity-datoms db eid schema)
    ;; Get all current datoms for entity eid from EAVT index.
    ;; Resolves current state: for each (e,a,v) triple, keeps only
    ;; the highest-tx datom, and only if it's an assertion.
    (let* ([eavt (db-resolve-index db 'eavt)]
           [lo (make-datom eid 0 +min-val+ 0 #t)]
           [hi (make-datom eid (greatest-fixnum) +max-val+ (greatest-fixnum) #t)]
           [raw (dbi-range eavt lo hi)]
           [filtered (filter (lambda (d) (db-filter-datom? db d)) raw)])
      (if (db-value-history? db)
          filtered
          ;; Resolve: group by (a, v), keep highest-tx, only if assertion
          (let ([ht (make-hashtable equal-hash equal?)])
            (for-each
              (lambda (d)
                (let ([key (cons (datom-a d) (datom-v d))])
                  (let ([existing (hashtable-ref ht key #f)])
                    (when (or (not existing)
                              (> (datom-tx d) (datom-tx existing)))
                      (hashtable-set! ht key d)))))
              filtered)
            (let-values ([(keys vals) (hashtable-entries ht)])
              (filter datom-added? (vector->list vals)))))))

  ;; ---- Group datoms by attribute ----

  (def (group-by-attr datoms schema)
    ;; Returns: alist of (attr-ident . (values ...))
    (let ([ht (make-eq-hashtable)])
      (for-each
        (lambda (d)
          (let ([attr (schema-lookup-by-id schema (datom-a d))])
            (when attr
              (let ([ident (db-attribute-ident attr)])
                (hashtable-update! ht ident
                  (lambda (existing) (cons (datom-v d) existing))
                  '())))))
        datoms)
      (let-values ([(keys vals) (hashtable-entries ht)])
        (map cons (vector->list keys) (vector->list vals)))))

  ;; ---- Pull implementation ----

  (def (pull-entity db pattern eid)
    (pull-entity* db pattern eid (make-eq-hashtable) 0))

  ;; Recursive pull with cycle detection and depth limit
  (def (pull-entity* db pattern eid seen depth)
    (when (> depth 50)
      (error 'pull "Maximum pull depth exceeded (cycle?)"))
    (when (hashtable-contains? seen eid)
      ;; Cycle detected — return just the db/id
      (list (cons 'db/id eid)))
    (hashtable-set! seen eid #t)

    (let* ([schema (db-value-schema db)]
           [datoms (entity-datoms db eid schema)]
           [grouped (group-by-attr datoms schema)])
      (let ([result (cons (cons 'db/id eid)
                          (pull-pattern db pattern eid grouped schema seen depth))])
        (hashtable-delete! seen eid)
        result)))

  ;; pull-pattern: eid is the current entity (needed for reverse attrs)
  (def (pull-pattern db pattern eid grouped schema seen depth)
    (if (null? pattern)
        '()
        ;; Check if wildcard is present in pattern
        (let ([has-wildcard? (memq '* pattern)]
              [extra-specs (filter (lambda (s) (not (eq? s '*))) pattern)])
          (if has-wildcard?
              ;; Wildcard: all user attributes + any extra specs
              (let ([wildcard-pairs
                     (filter-map
                       (lambda (pair)
                         (let* ([ident (car pair)]
                                [values (cdr pair)]
                                [attr (schema-lookup-by-ident schema ident)])
                           ;; Skip system attributes (id < +first-user-attr-id+)
                           (if (and attr (>= (db-attribute-id attr) +first-user-attr-id+))
                               (if (cardinality-one? attr)
                                   (cons ident (if (null? values) #f (car values)))
                                   (cons ident values))
                               #f)))
                       grouped)]
                    [extra-pairs
                     (apply append
                       (map (lambda (spec)
                              (pull-attr-spec db spec eid grouped schema seen depth))
                            extra-specs))])
                ;; Merge: extras override wildcard where they overlap
                (let ([extra-keys (map car extra-pairs)])
                  (append (filter (lambda (p) (not (memq (car p) extra-keys)))
                                  wildcard-pairs)
                          extra-pairs)))
              ;; No wildcard: process each spec normally
              (apply append
                (map (lambda (spec)
                       (pull-attr-spec db spec eid grouped schema seen depth))
                     pattern))))))

  (def (pull-attr-spec db spec eid grouped schema seen depth)
    (cond
      ;; Wildcard handled in pull-pattern; if we reach here alone, do full wildcard
      [(eq? spec '*)
       (filter-map
         (lambda (pair)
           (let* ([ident (car pair)]
                  [values (cdr pair)]
                  [attr (schema-lookup-by-ident schema ident)])
             (if (and attr (>= (db-attribute-id attr) +first-user-attr-id+))
                 (if (cardinality-one? attr)
                     (cons ident (if (null? values) #f (car values)))
                     (cons ident values))
                 #f)))
         grouped)]

      ;; Simple attribute (symbol)
      [(symbol? spec)
       (let* ([reverse? (reverse-attr? spec)]
              [real-ident (if reverse? (unreverse-attr spec) spec)])
         (if reverse?
             (pull-reverse-attr db real-ident spec eid schema seen depth #f #f)
             (let ([attr (schema-lookup-by-ident schema spec)]
                   [values (cond [(assq spec grouped) => cdr] [else '()])])
               (if (and attr (cardinality-one? attr))
                   (list (cons spec (if (null? values) #f (car values))))
                   (list (cons spec values))))))]

      ;; Nested pull: (attr sub-pattern) or (attr :option val ...)
      [(and (pair? spec) (symbol? (car spec)))
       (pull-nested-spec db spec eid grouped schema seen depth)]

      [else (error 'pull "Invalid pull spec" spec)]))

  ;; ---- Nested pull specs ----

  (def (pull-nested-spec db spec eid grouped schema seen depth)
    (let* ([ident (car spec)]
           [rest (cdr spec)]
           [reverse? (reverse-attr? ident)]
           [real-ident (if reverse? (unreverse-attr ident) ident)])
      ;; Parse options from rest
      (let-values ([(sub-pattern limit default alias)
                    (parse-pull-options rest)])
        (let ([out-key (or alias ident)])
          (if reverse?
              (pull-reverse-attr db real-ident out-key eid schema seen depth sub-pattern limit)
              (let* ([attr (schema-lookup-by-ident schema real-ident)]
                     [values (cond [(assq real-ident grouped) => cdr] [else '()])]
                     [limited (if limit (take-at-most values limit) values)])
                (cond
                  ;; Nested sub-pattern — follow refs
                  [(and sub-pattern attr (ref-type? attr))
                   (let ([nested (map (lambda (ref-eid)
                                        (pull-entity* db sub-pattern ref-eid
                                                      seen (+ depth 1)))
                                      limited)])
                     (list (cons out-key
                                 (if (and attr (cardinality-one? attr))
                                     (if (null? nested) (or default #f) (car nested))
                                     nested))))]
                  ;; No sub-pattern or not a ref — plain values
                  [else
                   (let ([vals (if (null? limited) (if default (list default) '())
                                   limited)])
                     (list (cons out-key
                                 (if (and attr (cardinality-one? attr))
                                     (if (null? vals) (or default #f) (car vals))
                                     vals))))])))))))

  ;; ---- Reverse attributes ----
  ;; :person/_friends means "entities that reference me via :person/friends"

  (def (reverse-attr? ident)
    (let ([s (symbol->string ident)])
      (let ([slash-pos (string-index s #\/)])
        (and slash-pos
             (< (+ slash-pos 1) (string-length s))
             (char=? (string-ref s (+ slash-pos 1)) #\_)))))

  (def (unreverse-attr ident)
    ;; person/_friends -> person/friends
    (let* ([s (symbol->string ident)]
           [slash-pos (string-index s #\/)])
      (string->symbol
        (string-append (substring s 0 (+ slash-pos 1))
                       (substring s (+ slash-pos 2) (string-length s))))))

  (def (find-referencing-entities db attr-ident current-eid schema)
    ;; Use VAET index to find all entities that reference current-eid via attr-ident.
    ;; VAET is sorted by (v, a, e, tx).
    (let* ([attr (schema-lookup-by-ident schema attr-ident)]
           [vaet (db-resolve-index db 'vaet)])
      (if (not attr)
          '()
          (let* ([aid (db-attribute-id attr)]
                 ;; We scan for datoms where v=current-eid and a=aid
                 ;; Build lo/hi bracketing the exact (v=current-eid, a=aid) range
                 [lo (make-datom 0 aid current-eid 0 #t)]
                 [hi (make-datom (greatest-fixnum) aid current-eid (greatest-fixnum) #t)]
                 ;; VAET cmp: v first, then a, then e, then tx
                 ;; So for v=current-eid and a=aid, we need probes with v and a fixed
                 ;; Use a broad scan and filter:
                 [broad-lo (make-datom 0 0 current-eid 0 #t)]
                 [broad-hi (make-datom (greatest-fixnum) (greatest-fixnum) current-eid (greatest-fixnum) #t)]
                 [raw (dbi-range vaet broad-lo broad-hi)]
                 [filtered (filter (lambda (d)
                                     (and (= (datom-a d) aid)
                                          (equal? (datom-v d) current-eid)
                                          (db-filter-datom? db d)))
                                   raw)])
            ;; Resolve current state: for each referencing entity, keep latest
            ;; assertion per entity
            (let ([ht (make-eq-hashtable)])
              (for-each
                (lambda (d)
                  (let* ([ref-eid (datom-e d)]
                         [ex (hashtable-ref ht ref-eid #f)])
                    (when (or (not ex) (> (datom-tx d) (datom-tx ex)))
                      (hashtable-set! ht ref-eid d))))
                filtered)
              (let-values ([(ks vs) (hashtable-entries ht)])
                (map datom-e (filter datom-added? (vector->list vs)))))))))

  (def (pull-reverse-attr db attr-ident out-key current-eid schema seen depth
                          sub-pattern limit)
    ;; Find all entities that have attr-ident pointing at current-eid
    (let* ([ref-eids (find-referencing-entities db attr-ident current-eid schema)]
           [limited (if limit (take-at-most ref-eids limit) ref-eids)]
           [result (if sub-pattern
                       (map (lambda (ref-eid)
                              (pull-entity* db sub-pattern ref-eid seen (+ depth 1)))
                            limited)
                       limited)])
      (list (cons out-key result))))

  ;; ---- Option parsing ----

  (def (parse-pull-options rest)
    ;; Returns: (values sub-pattern limit default alias)
    (let loop ([r rest] [sub #f] [lim #f] [def #f] [alias #f])
      (cond
        [(null? r) (values sub lim def alias)]
        [(eq? (car r) ':limit)
         (loop (cddr r) sub (cadr r) def alias)]
        [(eq? (car r) ':default)
         (loop (cddr r) sub lim (cadr r) alias)]
        [(eq? (car r) ':as)
         (loop (cddr r) sub lim def (cadr r))]
        [(list? (car r))
         ;; Sub-pattern
         (loop (cdr r) (car r) lim def alias)]
        [else (loop (cdr r) sub lim def alias)])))

  ;; ---- Utilities ----

  (def (take-at-most lst n)
    (let loop ([l lst] [i 0] [out '()])
      (if (or (null? l) (>= i n))
          (reverse out)
          (loop (cdr l) (+ i 1) (cons (car l) out)))))

  (def (pull-many db pattern eids)
    (map (lambda (eid) (pull-entity db pattern eid)) eids))

) ;; end library
