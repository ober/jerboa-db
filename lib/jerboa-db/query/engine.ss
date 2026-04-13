#!chezscheme
;;; (jerboa-db query engine) — Datalog query compiler and executor
;;;
;;; Parses Datomic-style query syntax, plans execution, and evaluates
;;; against database indices. Supports data patterns, predicates,
;;; function clauses, aggregation, and rules.

(library (jerboa-db query engine)
  (export query-db parse-query explain-query)

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
          (jerboa-db history)
          (jerboa-db query functions)
          (jerboa-db query aggregates)
          (jerboa-db query planner)
          (jerboa-db query rules))

  ;; ---- Logic variable helpers ----

  (def (logic-var? x)
    (and (symbol? x)
         (> (string-length (symbol->string x)) 0)
         (char=? (string-ref (symbol->string x) 0) #\?)))

  ;; ---- Binding environment ----
  ;; A binding is an eq? hashtable: var-symbol → value.
  ;; O(1) lookup via binding-ref is critical for large-output queries (Q4: 428K rows).
  ;; Copy-on-extend preserves immutability across the binding-set list used during
  ;; clause evaluation.
  ;;
  ;; Note: we explored alist bindings (O(1) binding-set, O(n_vars) binding-ref)
  ;; which are faster for small-output aggregation (Q8: 2×), but slower for
  ;; large-output queries (Q4: 2× slower due to extraction cost). Hashtable wins
  ;; overall for mixed workloads. Vector-indexed bindings would be ideal (O(1)
  ;; both ways) but require query-compile-time var-index assignment.

  (def (make-empty-bindings) (make-hashtable symbol-hash eq?))

  (def (binding-ref bindings var)
    (hashtable-ref bindings var #f))

  (def (binding-set bindings var val)
    (let ([new-ht (hashtable-copy bindings #t)])
      (hashtable-set! new-ht var val)
      new-ht))

  (def (resolve-in-bindings bindings x)
    (if (logic-var? x)
        (binding-ref bindings x)
        x))

  ;; ---- Projection helpers (projection pushdown) ----
  ;; Strip dead variables from bindings after each clause to reduce allocation.

  (def (project-bindings bindings live-vars n-live)
    ;; Return a hashtable containing only keys in live-vars.
    ;; Fast path: if count matches, bindings are already minimal — return as-is.
    ;; Uses hashtable-size (O(1)) instead of hashtable-keys (allocates vector).
    (if (= (hashtable-size bindings) n-live)
        bindings   ;; fast path: already minimal, no allocation
        (let ([new-ht (make-hashtable symbol-hash eq?)])
          (for-each (lambda (v)
                      (let ([val (hashtable-ref bindings v #f)])
                        (when val (hashtable-set! new-ht v val))))
                    live-vars)
          new-ht)))

  (def (project-bindings-list bindings-list live-vars)
    ;; Project every binding in the list to live-vars.
    ;; Skip entirely if nothing to project (n-live = 0 or list is empty).
    (let ([n-live (length live-vars)])
      (if (= n-live 0)
          bindings-list
          (map (lambda (b) (project-bindings b live-vars n-live)) bindings-list))))

  ;; ---- Query parsing ----
  ;; Query format: ((find vars...) (in inputs...) (where clauses...))
  ;; All sections are identified by their car symbol.

  (define-record-type parsed-query
    (fields find-vars      ;; list of symbols or (agg ?var) forms
            in-vars        ;; list of input symbols ($ = db)
            where-clauses  ;; list of clause forms
            rules))

  (def (parse-query form)
    (let ([find-clause #f]
          [in-clause '($)]
          [where-clause '()]
          [rule-defs #f])
      (for-each
        (lambda (section)
          (case (car section)
            [(find) (set! find-clause (cdr section))]
            [(in) (set! in-clause (cdr section))]
            [(where) (set! where-clause (cdr section))]
            [else (void)]))
        form)
      (make-parsed-query
        (or find-clause '())
        in-clause
        where-clause
        rule-defs)))

  ;; ---- Current-state resolution ----
  ;; In a non-history view, datoms are append-only: both assertions (added?=#t)
  ;; and retractions (added?=#f) coexist in the index. For each unique (e,a,v)
  ;; triple, the datom with the highest tx determines current state. If it's an
  ;; assertion, the value is live; if a retraction, the value is gone.

  (def (resolve-current-datoms datoms)
    ;; Fast path: if no retractions in the scanned range, every datom is a current
    ;; assertion — skip the hashtable entirely. This is the common case for
    ;; freshly-written or lightly-updated databases.
    (if (for-all datom-added? datoms)
        datoms
        ;; Slow path: build (e,a,v) → latest-datom map and filter for assertions.
        (let ([ht (make-hashtable equal-hash equal?)])
          (for-each
            (lambda (d)
              (let ([key (list (datom-e d) (datom-a d) (datom-v d))])
                (let ([existing (hashtable-ref ht key #f)])
                  (when (or (not existing)
                            (> (datom-tx d) (datom-tx existing)))
                    (hashtable-set! ht key d)))))
            datoms)
          (let-values ([(keys vals) (hashtable-entries ht)])
            (filter datom-added? (vector->list vals))))))

  ;; ---- Data pattern evaluation ----
  ;; A data pattern like (?e person/name ?name) is matched against an index.

  ;; Try to unify a datom with the pattern, extending bindings.
  ;; Returns extended bindings or #f if unification fails.
  (def (try-unify-datom d e-spec a-spec v-spec tx-spec op-spec bindings)
    (define (unify spec actual binds)
      (cond
        [(eq? spec '_) binds]  ;; wildcard
        [(logic-var? spec)
         (let ([existing (binding-ref binds spec)])
           (if existing
               (if (equal? existing actual) binds #f)  ;; already bound: must match
               (binding-set binds spec actual)))]       ;; new binding
        [(equal? spec actual) binds]                    ;; literal match
        [else #f]))                                      ;; mismatch
    (let* ([b1 (unify e-spec (datom-e d) bindings)]
           [b2 (and b1 (unify v-spec (datom-v d) b1))]
           [b3 (and b2 (unify tx-spec (datom-tx d) b2))]
           [b4 (and b3 (unify op-spec (datom-added? d) b3))])
      b4))

  (def (evaluate-data-pattern db pattern bindings schema)
    ;; pattern: (e-spec a-spec v-spec) or (e-spec a-spec v-spec tx-spec)
    ;; or (e-spec a-spec v-spec tx-spec op-spec)
    (let* ([e-spec (car pattern)]
           [a-spec (cadr pattern)]
           [v-spec (if (>= (length pattern) 3) (caddr pattern) '_)]
           [tx-spec (if (>= (length pattern) 4) (cadddr pattern) '_)]
           [op-spec (if (>= (length pattern) 5) (list-ref pattern 4) '_)]
           [e-val (resolve-in-bindings bindings e-spec)]
           [a-ident a-spec]  ;; attribute is always a literal symbol
           [v-val (resolve-in-bindings bindings v-spec)]
           [tx-val (resolve-in-bindings bindings tx-spec)]
           [attr (schema-lookup-by-ident schema a-ident)])
      (unless attr
        (error 'query "Unknown attribute in pattern" a-ident))
      (let* ([aid (db-attribute-id attr)]
             ;; Choose index based on what's bound
             [index-name (cond
                           [e-val 'eavt]
                           [(and v-val (ref-type? attr)) 'vaet]
                           ;; Use AVET for all scalar attrs — tx.sls now indexes them all
                           [(and v-val (avet-eligible? attr)) 'avet]
                           [else 'aevt])]
             [idx (db-resolve-index db index-name)]
             ;; Build range boundaries
             [lo (case index-name
                   [(eavt) (make-datom (or e-val 0) aid
                             (if v-val v-val +min-val+) 0 #t)]
                   [(aevt) (make-datom (or e-val 0) aid
                             (if v-val v-val +min-val+) 0 #t)]
                   [(avet) (make-datom 0 aid
                             (if v-val v-val +min-val+) 0 #t)]
                   [(vaet) (make-datom 0 aid
                             (if v-val v-val +min-val+) 0 #t)])]
             [hi (case index-name
                   [(eavt) (make-datom (or e-val (greatest-fixnum)) aid
                             (if v-val v-val +max-val+)
                             (greatest-fixnum) #t)]
                   [(aevt) (make-datom (or e-val (greatest-fixnum)) aid
                             (if v-val v-val +max-val+)
                             (greatest-fixnum) #t)]
                   [(avet) (make-datom (greatest-fixnum) aid
                             (if v-val v-val +max-val+)
                             (greatest-fixnum) #t)]
                   [(vaet) (make-datom (greatest-fixnum) aid
                             (if v-val v-val +max-val+)
                             (greatest-fixnum) #t)])]
             [raw-datoms (dbi-range idx lo hi)]
             ;; Apply db-level filters (as-of, since, history)
             [datoms (filter (lambda (d) (db-filter-datom? db d)) raw-datoms)])
        ;; For non-history queries, resolve to current state:
        ;; Group by (e, a, v), keep only those whose highest-tx datom is added?=#t.
        ;; In history mode, return all datoms.
        (let ([effective-datoms
                (if (db-value-history? db)
                    datoms
                    (resolve-current-datoms datoms))])
          ;; Produce new bindings for each matching datom
          (let loop ([ds effective-datoms] [results '()])
            (if (null? ds)
                (reverse results)
                (let* ([d (car ds)]
                       [new-bindings (try-unify-datom d e-spec a-spec v-spec
                                                      tx-spec op-spec bindings)])
                  (if new-bindings
                      (loop (cdr ds) (cons new-bindings results))
                      (loop (cdr ds) results)))))))))

  ;; ---- Predicate clause evaluation ----
  ;; ((pred arg1 arg2 ...)) — returns filtered bindings

  (def (evaluate-predicate-clause clause bindings)
    (let* ([form (car clause)]
           [pred-name (car form)]
           [args (cdr form)]
           [resolved (map (lambda (a) (resolve-in-bindings bindings a)) args)])
      (if (and (for-all (lambda (x) x) resolved)  ;; all args resolved
               (apply-builtin-predicate pred-name resolved))
          (list bindings)   ;; predicate passed
          '())))            ;; predicate failed

  ;; ---- Function clause evaluation ----
  ;; ((fn arg1 arg2 ...) ?result) — computes and binds result

  (def (evaluate-function-clause clause bindings)
    (let* ([form (car clause)]
           [fn-name (car form)]
           [args (cdr form)]
           [result-var (cadr clause)]
           [resolved (map (lambda (a) (resolve-in-bindings bindings a)) args)])
      (if (for-all (lambda (x) x) resolved)
          (let ([result (apply-builtin-function fn-name resolved)])
            (list (binding-set bindings result-var result)))
          '())))

  ;; ---- Clause classification ----

  (def (data-pattern? clause)
    ;; A data pattern: (something attr-symbol something ...)
    ;; First element is a var or literal, second is a symbol (attribute)
    (and (pair? clause)
         (not (pair? (car clause)))
         (>= (length clause) 3)
         (symbol? (cadr clause))
         (not (logic-var? (cadr clause)))))

  (def (predicate-clause? clause)
    ;; ((pred args...)) — single nested list, no result binding
    (and (pair? clause) (pair? (car clause)) (null? (cdr clause))))

  (def (function-clause? clause)
    ;; ((fn args...) ?result) — nested list + result variable
    (and (pair? clause) (pair? (car clause)) (pair? (cdr clause))))

  (def (not-clause? clause)
    ;; (not sub-clause ...) — negation
    (and (pair? clause) (eq? (car clause) 'not)))

  (def (not-join-clause? clause)
    ;; (not-join [?e ?x ...] sub-clause ...)
    (and (pair? clause)
         (eq? (car clause) 'not-join)
         (pair? (cdr clause))
         (list? (cadr clause))   ;; join-vars list
         (pair? (cddr clause)))) ;; at least one sub-clause

  (def (or-clause? clause)
    ;; (or alt1 alt2 ...) — disjunction
    (and (pair? clause) (eq? (car clause) 'or)))

  (def (rule-invocation? clause rules-ht)
    ;; (rule-name args...) where rule-name is in the rule registry
    (and (pair? clause)
         (symbol? (car clause))
         (not (logic-var? (car clause)))
         rules-ht
         (rule-name? (car clause) rules-ht)))

  ;; ---- Range predicate pushdown helpers ----
  ;; Detect (?e attr ?v) [(cmp ?v const)] and fuse into a single AVET range scan.

  ;; Parse a comparison form (op a b) where one arg matches v-spec and the other
  ;; is a literal constant.  Returns (lo hi lo-strict? hi-strict?) or #f.
  (def (parse-range-predicate pred-form v-spec)
    (and (pair? pred-form)
         (memq (car pred-form) '(< <= > >=))
         (= (length pred-form) 3)
         (let ([op (car pred-form)]
               [a  (cadr pred-form)]
               [b  (caddr pred-form)])
           (cond
             ;; (?v op const)
             [(and (equal? a v-spec) (not (logic-var? b)))
              (case op
                [(<)  (list #f b #f #t)]
                [(<=) (list #f b #f #f)]
                [(>)  (list b #f #t #f)]
                [(>=) (list b #f #f #f)])]
             ;; (const op ?v) — reversed direction
             [(and (equal? b v-spec) (not (logic-var? a)))
              (case op
                [(<)  (list a #f #t #f)]
                [(<=) (list a #f #f #f)]
                [(>)  (list #f a #f #t)]
                [(>=) (list #f a #f #f)])]
             [else #f]))))

  ;; Combine two range bound specs.  Takes the tighter of each bound.
  (def (merge-range-bounds b1 b2)
    (if (not b2) b1
        (list (or (car b1)   (car b2))
              (or (cadr b1)  (cadr b2))
              (or (caddr b1) (caddr b2))
              (or (cadddr b1)(cadddr b2)))))

  ;; Evaluate a data pattern using a pre-computed AVET range instead of a
  ;; full attribute scan.  bounds = (lo hi lo-strict? hi-strict?).
  (def (evaluate-data-pattern-in-range db pattern bindings schema bounds)
    (let* ([e-spec    (car pattern)]
           [a-spec    (cadr pattern)]
           [v-spec    (caddr pattern)]
           [tx-spec   (if (>= (length pattern) 4) (cadddr pattern) '_)]
           [op-spec   (if (>= (length pattern) 5) (list-ref pattern 4) '_)]
           [attr      (schema-lookup-by-ident schema a-spec)]
           [aid       (db-attribute-id attr)]
           [lo-val    (car bounds)]
           [hi-val    (cadr bounds)]
           [lo-strict? (caddr bounds)]
           [hi-strict? (cadddr bounds)]
           [idx    (db-resolve-index db 'avet)]
           [lo     (make-datom 0 aid (if lo-val lo-val +min-val+) 0 #t)]
           [hi     (make-datom (greatest-fixnum) aid
                     (if hi-val hi-val +max-val+) (greatest-fixnum) #t)]
           [raw    (dbi-range idx lo hi)]
           [filtered (filter (lambda (d) (db-filter-datom? db d)) raw)]
           [effective (if (db-value-history? db)
                          filtered
                          (resolve-current-datoms filtered))]
           ;; Post-filter strict inequality endpoints
           [ranged (filter
                     (lambda (d)
                       (let ([v (datom-v d)])
                         (and (if (and lo-val lo-strict?) (not (equal? v lo-val)) #t)
                              (if (and hi-val hi-strict?) (not (equal? v hi-val)) #t))))
                     effective)])
      (let loop ([ds ranged] [results '()])
        (if (null? ds)
            (reverse results)
            (let ([new-b (try-unify-datom (car ds) e-spec a-spec v-spec
                                          tx-spec op-spec bindings)])
              (loop (cdr ds) (if new-b (cons new-b results) results)))))))

  ;; Resolve bound logic-vars in a predicate form to concrete values.
  ;; Used so that ((> ?v ?input-var)) pushes down even when ?input-var
  ;; is a bound logic-variable rather than a literal constant.
  (def (resolve-pred-form-against bindings pred-form)
    (map (lambda (x)
           (if (logic-var? x)
               (let ([v (binding-ref bindings x)])
                 (if v v x))
               x))
         pred-form))

  ;; ---- Main query evaluation ----

  ;; evaluate-where-clauses: main recursive engine.
  ;; live-vars-steps: a list of (N+1) symbol lists from compute-live-vars-at-each-step,
  ;;   or #f to disable projection (used by rule/not/or sub-evaluations).
  ;; Element i = vars that must be live BEFORE clause[i]; last element = find-used-vars.
  ;; After evaluating clause[i] we project to live-vars-steps[i+1] (cadr live-vars-steps).
  (def (evaluate-where-clauses db clauses bindings-list schema rules-ht . rest-args)
    (let ([live-vars-steps (if (null? rest-args) #f (car rest-args))])
      (if (null? clauses)
          bindings-list
          (let* ([clause   (car clauses)]
                 [rest     (cdr clauses)]
                 ;; Range-predicate pushdown: fuse (?e attr ?v) [(cmp ?v const)] into
                 ;; a single AVET range scan.  pushdown = (bounds . new-rest) or (#f . rest).
                 ;; ONLY fires when both entity AND value are unbound — if entity is bound,
                 ;; EAVT point-lookup (one datom) is always faster than an AVET range scan.
                 [pushdown
                   (if (and (data-pattern? clause)
                            (pair? rest)
                            (predicate-clause? (car rest)))
                       (let* ([e-spec (car clause)]
                              [v-spec (and (>= (length clause) 3) (caddr clause))]
                              [a-spec (cadr clause)]
                              [attr   (schema-lookup-by-ident schema a-spec)])
                         (if (and (logic-var? v-spec)
                                  attr
                                  (avet-eligible? attr)
                                  (pair? bindings-list)
                                  (not (binding-ref (car bindings-list) v-spec))
                                  ;; Entity must be unbound; if bound, EAVT is faster
                                  (logic-var? e-spec)
                                  (not (binding-ref (car bindings-list) e-spec)))
                             (let* ([;; Resolve bound logic-vars (e.g. input params) so
                                     ;; ((> ?v ?input)) pushes down even when ?input is a
                                     ;; bound logic-var rather than a literal constant.
                                     resolved-pred1
                                       (if (pair? bindings-list)
                                           (resolve-pred-form-against (car bindings-list) (caar rest))
                                           (caar rest))]
                                    [b1 (parse-range-predicate resolved-pred1 v-spec)]
                                    ;; Absorb a second consecutive range predicate too
                                    [b2 (and b1
                                             (pair? (cdr rest))
                                             (predicate-clause? (cadr rest))
                                             (let ([resolved-pred2
                                                     (if (pair? bindings-list)
                                                         (resolve-pred-form-against (car bindings-list) (caadr rest))
                                                         (caadr rest))])
                                               (parse-range-predicate resolved-pred2 v-spec)))]
                                    [bounds (and b1 (merge-range-bounds b1 b2))]
                                    [skip-n (if b2 2 (if b1 1 0))])
                               (if bounds
                                   (cons bounds (list-tail rest skip-n))
                                   (cons #f rest)))
                             (cons #f rest)))
                       (cons #f rest))]
                 [bounds    (car pushdown)]
                 [eval-rest (cdr pushdown)]
                 ;; Inline flatmap: avoids building an intermediate list-of-lists.
                 ;; Early termination: if no bindings survive a clause, stop immediately.
                 [new-bindings-list
                   (let loop ([lst bindings-list] [acc '()])
                     (if (null? lst)
                         (reverse acc)
                         (let inner ([rs (if bounds
                                             (evaluate-data-pattern-in-range
                                              db clause (car lst) schema bounds)
                                             (evaluate-single-clause
                                              db clause (car lst) schema rules-ht))]
                                     [a acc])
                           (if (null? rs)
                               (loop (cdr lst) a)
                               (inner (cdr rs) (cons (car rs) a))))))])
            (if (null? new-bindings-list)
                '()
                ;; Projection pushdown: strip dead variables before recursing.
                ;; live-vars-steps[0] = live before clause[0]
                ;; live-vars-steps[1] = live after clause[0] (= before clause[1]) — project to this.
                ;; The range-predicate pushdown may have consumed extra clauses, so the
                ;; number of steps consumed may be > 1. We advance live-vars-steps by the
                ;; same number of clauses consumed (1 + number of fused predicates).
                (let* ([steps-consumed
                         (if (not live-vars-steps) 0
                             (- (+ 1 (length clauses))
                                (+ 1 (length eval-rest))))]
                       [next-steps
                         (and live-vars-steps
                              (list-tail live-vars-steps steps-consumed))]
                       [live-after
                         (and next-steps (pair? next-steps) (car next-steps))]
                       ;; live-before = live-vars-steps[0] — vars that were live before clause[0].
                       ;; If live-after has the same count as live-before, no variables went dead
                       ;; and we can skip projection entirely (avoid map over bindings-list).
                       [live-before (and live-vars-steps (car live-vars-steps))]
                       [projected
                         (if (and live-after live-before
                                  (< (length live-after) (length live-before)))
                             (project-bindings-list new-bindings-list live-after)
                             new-bindings-list)])
                  (evaluate-where-clauses db eval-rest projected schema rules-ht next-steps)))))))

  (def (evaluate-single-clause db clause bindings schema rules-ht)
    (cond
      [(not-clause? clause)
       (evaluate-not-clause db clause bindings schema rules-ht)]
      [(not-join-clause? clause)
       (evaluate-not-join-clause db clause bindings schema rules-ht)]
      [(or-clause? clause)
       (evaluate-or-clause db clause bindings schema rules-ht)]
      [(data-pattern? clause)
       (evaluate-data-pattern db clause bindings schema)]
      [(predicate-clause? clause)
       (evaluate-predicate-clause clause bindings)]
      [(function-clause? clause)
       (evaluate-function-clause clause bindings)]
      [(rule-invocation? clause rules-ht)
       (evaluate-rule-clause db clause bindings schema rules-ht)]
      [else (error 'query "Unknown clause type" clause)]))

  ;; ---- Rule evaluation ----

  (def (evaluate-rule-clause db clause bindings schema rules-ht)
    (let* ([name (car clause)]
           [args (cdr clause)]
           [alternatives (expand-rule-invocation name args rules-ht)])
      ;; Each alternative is a list of clauses. Evaluate each and union results.
      (apply append
        (map (lambda (alt-clauses)
               (evaluate-where-clauses db alt-clauses (list bindings) schema rules-ht))
             alternatives))))

  ;; ---- Not clause evaluation ----
  ;; (not sub-clause ...) — exclude bindings for which sub-clauses match.

  (def (evaluate-not-clause db clause bindings schema rules-ht)
    (let* ([sub-clauses (cdr clause)]
           ;; Evaluate sub-clauses starting from current bindings
           [results (evaluate-where-clauses db sub-clauses (list bindings) schema rules-ht)])
      ;; If sub-query produces any results, this binding is excluded
      (if (null? results)
          (list bindings)   ;; not matched -> keep this binding
          '())))            ;; matched -> exclude

  ;; ---- Not-join clause evaluation ----
  ;; (not-join [?e ?x ...] sub-clause ...) — like not, but only the listed
  ;; join vars cross into the sub-query; other vars inside are local.

  (def (evaluate-not-join-clause db clause bindings schema rules-ht)
    (let* ([join-vars   (cadr clause)]
           [sub-clauses (cddr clause)]
           ;; Build restricted bindings containing ONLY the join vars
           [restricted  (let ([new-ht (make-hashtable symbol-hash eq?)])
                          (for-each
                            (lambda (v)
                              (let ([val (binding-ref bindings v)])
                                (when val (hashtable-set! new-ht v val))))
                            join-vars)
                          new-ht)]
           ;; Evaluate sub-clauses from the restricted binding set only
           [results (evaluate-where-clauses db sub-clauses
                      (list restricted) schema rules-ht)])
      ;; If sub-query produced results → exclude this outer binding
      (if (null? results)
          (list bindings)   ;; not matched → keep
          '())))

  ;; ---- Or clause evaluation ----
  ;; (or alt1 alt2 ...) — union of bindings from each alternative.

  (def (evaluate-or-clause db clause bindings schema rules-ht)
    (let ([alternatives (cdr clause)])
      (apply append
        (map (lambda (alt)
               ;; Each alternative is a single clause
               (evaluate-single-clause db alt bindings schema rules-ht))
             alternatives))))

  ;; ---- Find clause processing ----
  ;; Extract result tuples from bindings based on :find specification.

  (def (extract-find-results find-vars bindings-list)
    (let ([has-aggregates? (any-aggregates? find-vars)])
      (if has-aggregates?
          (apply-aggregates find-vars bindings-list)
          (deduplicate-results
            (map (lambda (bindings)
                   (map (lambda (var)
                          (if (logic-var? var)
                              (binding-ref bindings var)
                              var))  ;; literal in find
                        find-vars))
                 bindings-list)))))

  (def (any-aggregates? find-vars)
    (exists (lambda (v) (and (pair? v) (aggregate? (car v)))) find-vars))

  ;; ---- Streaming aggregation helpers ----
  ;; Accumulators are mutable vectors updated in place — no allocation per row
  ;; for count/sum/avg/min/max (only 3 group objects for Q8's 655K rows).

  (def (streamable-aggregate? name)
    (memq name '(count sum avg min max variance variance-sample stddev stddev-sample)))

  (def (init-agg-acc name)
    (case name
      [(count) (vector 0)]
      [(sum)   (vector 0)]
      [(avg)   (vector 0 0)]   ;; #(sum count)
      [(min)   (vector #f)]
      [(max)   (vector #f)]
      [(variance variance-sample stddev stddev-sample)
       (vector 0 0.0 0.0)]))

  (def (update-agg-acc! name acc val)
    (case name
      [(count)
       (vector-set! acc 0 (+ (vector-ref acc 0) 1))]
      [(sum)
       (when (number? val)
         (vector-set! acc 0 (+ (vector-ref acc 0) val)))]
      [(avg)
       (when (number? val)
         (vector-set! acc 0 (+ (vector-ref acc 0) val))
         (vector-set! acc 1 (+ (vector-ref acc 1) 1)))]
      [(min)
       (let ([cur (vector-ref acc 0)])
         (when (or (not cur)
                   (and (number? val) (number? cur) (< val cur))
                   (and (string? val) (string? cur) (string<? val cur)))
           (vector-set! acc 0 val)))]
      [(max)
       (let ([cur (vector-ref acc 0)])
         (when (or (not cur)
                   (and (number? val) (number? cur) (> val cur))
                   (and (string? val) (string? cur) (string>? val cur)))
           (vector-set! acc 0 val)))]
      [(variance variance-sample stddev stddev-sample)
       (when (number? val)
         (let* ([n     (+ (vector-ref acc 0) 1)]
                [x     (inexact val)]
                [delta (- x (vector-ref acc 1))]
                [mean  (+ (vector-ref acc 1) (/ delta n))]
                [delta2 (- x mean)]
                [M2    (+ (vector-ref acc 2) (* delta delta2))])
           (vector-set! acc 0 n)
           (vector-set! acc 1 mean)
           (vector-set! acc 2 M2)))]))

  (def (finalize-agg-acc name acc)
    (case name
      [(avg)
       (let ([cnt (vector-ref acc 1)])
         (if (= cnt 0) 0
             (inexact (/ (vector-ref acc 0) cnt))))]
      [(variance)
       (let ([n (vector-ref acc 0)] [M2 (vector-ref acc 2)])
         (if (< n 1) 0.0 (/ M2 n)))]
      [(variance-sample)
       (let ([n (vector-ref acc 0)] [M2 (vector-ref acc 2)])
         (if (< n 2) 0.0 (/ M2 (- n 1))))]
      [(stddev)
       (let ([n (vector-ref acc 0)] [M2 (vector-ref acc 2)])
         (if (< n 1) 0.0 (sqrt (/ M2 n))))]
      [(stddev-sample)
       (let ([n (vector-ref acc 0)] [M2 (vector-ref acc 2)])
         (if (< n 2) 0.0 (sqrt (/ M2 (- n 1)))))]
      [else (vector-ref acc 0)]))

  (def (streaming-aggregate find-vars grouping-vars agg-specs bindings-list)
    ;; Single-pass aggregation: accum-ht maps group-key → outer-vector.
    ;; outer-vector[i] = inner accumulator for agg-specs[i] (mutable, updated in place).
    ;; For Q8: 655K rows, 3 groups → allocates exactly 3 outer-vectors + 3*2 inner-vectors.
    ;;
    ;; Pre-compute a flat dispatch table so extraction avoids mutation-based indexing:
    ;; find-spec-slots: vector of (symbol . info) where info is either
    ;;   ('agg agg-name agg-index) or ('grp grp-index)
    (let* ([accum-ht (make-hashtable equal-hash equal?)]
           [n-aggs (length agg-specs)]
           ;; Build slot table for extraction — explicit left-to-right loop avoids
           ;; depending on map's evaluation order (Chez applies lambda right-to-left).
           [slots (let loop ([fvs find-vars] [ai 0] [gi 0] [acc '()])
                    (if (null? fvs)
                        (reverse acc)
                        (let ([fv (car fvs)])
                          (cond
                            [(and (pair? fv) (aggregate? (car fv)))
                             (loop (cdr fvs) (+ ai 1) gi
                                   (cons (list 'agg (car fv) ai) acc))]
                            [(logic-var? fv)
                             (loop (cdr fvs) ai (+ gi 1)
                                   (cons (list 'grp gi) acc))]
                            [else
                             (loop (cdr fvs) ai gi
                                   (cons (list 'lit fv) acc))]))))])
      (for-each
        (lambda (bindings)
          (let* ([key (map (lambda (v) (binding-ref bindings v)) grouping-vars)]
                 [outer (hashtable-ref accum-ht key #f)])
            (if outer
                ;; Hot path: update existing accumulators in place
                (let loop ([specs agg-specs] [i 0])
                  (unless (null? specs)
                    (update-agg-acc! (caar specs) (vector-ref outer i)
                                     (binding-ref bindings (cadar specs)))
                    (loop (cdr specs) (+ i 1))))
                ;; Cold path: first row for this group key
                (let ([outer (make-vector n-aggs)])
                  (let loop ([specs agg-specs] [i 0])
                    (unless (null? specs)
                      (let ([acc (init-agg-acc (caar specs))])
                        (update-agg-acc! (caar specs) acc
                                         (binding-ref bindings (cadar specs)))
                        (vector-set! outer i acc))
                      (loop (cdr specs) (+ i 1))))
                  (hashtable-set! accum-ht key outer)))))
        bindings-list)
      ;; Extract final results — one row per group using pre-computed slots
      (let-values ([(keys outers) (hashtable-entries accum-ht)])
        (map (lambda (group-key outer)
               (map (lambda (slot)
                      (case (car slot)
                        [(agg) (finalize-agg-acc (cadr slot)
                                                  (vector-ref outer (caddr slot)))]
                        [(grp) (list-ref group-key (cadr slot))]
                        [(lit) (cadr slot)]))
                    slots))
             (vector->list keys)
             (vector->list outers)))))

  (def (apply-aggregates find-vars bindings-list)
    ;; Fast path: single (count ?x) with no grouping vars — just count rows.
    (if (and (= 1 (length find-vars))
             (pair? (car find-vars))
             (eq? 'count (caar find-vars))
             (logic-var? (cadar find-vars)))
        (list (list (length bindings-list)))
        ;; Dispatch: streaming path for count/sum/avg/min/max (no allocation per row);
        ;; fallback materializes all bindings for count-distinct/distinct/median/etc.
        (let* ([grouping-vars (filter (lambda (v) (not (and (pair? v) (aggregate? (car v)))))
                                      find-vars)]
               [agg-specs (filter (lambda (v) (and (pair? v) (aggregate? (car v))))
                                  find-vars)])
          (if (for-all (lambda (spec) (streamable-aggregate? (car spec))) agg-specs)
              (streaming-aggregate find-vars grouping-vars agg-specs bindings-list)
              ;; Fallback: materialize per-group binding lists
              (let ([groups (group-bindings grouping-vars bindings-list)])
                (map (lambda (group)
                       (let ([sample-binding (car (cdr group))]
                             [group-bindings (cdr group)])
                         (map (lambda (fv)
                                (if (and (pair? fv) (aggregate? (car fv)))
                                    (let* ([agg-name (car fv)]
                                           [agg-var (cadr fv)]
                                           [values (map (lambda (b) (binding-ref b agg-var))
                                                        group-bindings)])
                                      (aggregate-apply agg-name values))
                                    (if (logic-var? fv)
                                        (binding-ref sample-binding fv)
                                        fv)))
                              find-vars)))
                     groups))))))

  (def (group-bindings grouping-vars bindings-list)
    ;; Fallback: group bindings by grouping-vars values.
    ;; Returns: list of (group-key . bindings-list)
    (let ([ht (make-hashtable equal-hash equal?)])
      (for-each
        (lambda (bindings)
          (let ([key (map (lambda (v)
                            (if (logic-var? v) (binding-ref bindings v) v))
                          grouping-vars)])
            (hashtable-update! ht key
              (lambda (existing) (cons bindings existing))
              '())))
        bindings-list)
      (let-values ([(keys vals) (hashtable-entries ht)])
        (map cons (vector->list keys) (vector->list vals)))))

  ;; Remove duplicate result tuples
  (def (deduplicate-results results)
    (let ([ht (make-hashtable equal-hash equal?)])
      (let loop ([rs results] [out '()])
        (if (null? rs)
            (reverse out)
            (let ([r (car rs)])
              (if (hashtable-contains? ht r)
                  (loop (cdr rs) out)
                  (begin
                    (hashtable-set! ht r #t)
                    (loop (cdr rs) (cons r out)))))))))

  ;; ---- Hash-join execution ----

  (def (merge-bindings b1 b2)
    ;; Merge two binding hashtables; combined keys from both.
    ;; Values in b2 override b1 on conflict (shouldn't conflict on non-join vars).
    (let ([new-ht (hashtable-copy b1 #t)])
      (let-values ([(keys vals) (hashtable-entries b2)])
        (vector-for-each (lambda (k v) (hashtable-set! new-ht k v)) keys vals))
      new-ht))

  (def (evaluate-hash-join db ca cb join-vars bindings-list schema)
    ;; Step 1: evaluate ca from current bindings → side-A
    (let* ([side-a (let loop ([lst bindings-list] [acc '()])
                     (if (null? lst)
                         (reverse acc)
                         (loop (cdr lst)
                               (append (reverse
                                         (evaluate-single-clause db ca (car lst) schema #f))
                                       acc))))]
           ;; Step 2: build probe table HT[join-key] → list of side-A bindings
           [ht (make-hashtable equal-hash equal?)])
      (for-each
        (lambda (b)
          (let ([key (map (lambda (v) (binding-ref b v)) join-vars)])
            (hashtable-update! ht key
              (lambda (existing) (cons b existing))
              '())))
        side-a)
      ;; Step 3: evaluate cb from current bindings → side-B
      (let ([side-b (let loop ([lst bindings-list] [acc '()])
                      (if (null? lst)
                          (reverse acc)
                          (loop (cdr lst)
                                (append (reverse
                                          (evaluate-single-clause db cb (car lst) schema #f))
                                        acc))))])
        ;; Step 4: probe — for each side-B binding, find matching side-A bindings
        (let loop ([lst side-b] [acc '()])
          (if (null? lst)
              (reverse acc)
              (let* ([b-binding (car lst)]
                     [key (map (lambda (v) (binding-ref b-binding v)) join-vars)]
                     [matches (hashtable-ref ht key '())])
                (loop (cdr lst)
                      (append (reverse
                                (map (lambda (a-binding)
                                       (merge-bindings a-binding b-binding))
                                     matches))
                              acc))))))))

  (def (evaluate-plan db hj-plan bindings-list schema rules-ht)
    (if (null? hj-plan)
        bindings-list
        (let* ([step (car hj-plan)]
               [rest (cdr hj-plan)]
               [step-type (car step)]
               [new-bindings
                 (cond
                   [(eq? step-type 'sequential)
                    (let ([clause (cadr step)])
                      (let loop ([lst bindings-list] [acc '()])
                        (if (null? lst)
                            (reverse acc)
                            (loop (cdr lst)
                                  (append (reverse
                                            (evaluate-single-clause
                                             db clause (car lst) schema rules-ht))
                                          acc)))))]
                   [(eq? step-type 'hash-join)
                    (evaluate-hash-join db (cadr step) (caddr step) (cadddr step)
                                        bindings-list schema)]
                   [else (error 'evaluate-plan "Unknown step type" step-type)])])
          (if (null? new-bindings)
              '()
              (evaluate-plan db rest new-bindings schema rules-ht)))))

  ;; ---- Top-level query function ----

  (def (query-db parsed db . inputs)
    (let* ([schema (db-value-schema db)]
           [find-vars (parsed-query-find-vars parsed)]
           [in-vars (parsed-query-in-vars parsed)]
           [where-clauses (parsed-query-where-clauses parsed)]
           [rules-ht (parsed-query-rules parsed)]
           ;; Build initial bindings from inputs
           ;; $ = db (implicit), % = rules, other = user inputs
           ;; Input var specs:
           ;;   ?x       — scalar: bind ?x to the input value
           ;;   (?x ...) — collection: input is a list, produce one binding per value
           ;;   [?x ?y]  — tuple: input is a single tuple, bind multiple vars
           ;;   [[?x ?y]]— relation: input is a list of tuples
           [input-bindings
             (let loop ([ivs in-vars] [inps inputs]
                        [bindings-list (list (make-empty-bindings))])
               (cond
                 [(null? ivs) bindings-list]
                 [(eq? (car ivs) '$) (loop (cdr ivs) inps bindings-list)]
                 [(eq? (car ivs) '%)
                  (loop (cdr ivs) (cdr inps) bindings-list)]
                 ;; Collection binding: (?x ...) — spec is a list ending with ...
                 [(and (pair? (car ivs))
                       (>= (length (car ivs)) 2)
                       (eq? (list-ref (car ivs) (- (length (car ivs)) 1)) '...))
                  (let* ([var (caar ivs)]
                         [vals (car inps)]
                         [new-bindings-list
                           (apply append
                             (map (lambda (bindings)
                                    (map (lambda (v) (binding-set bindings var v)) vals))
                                  bindings-list))])
                    (loop (cdr ivs) (cdr inps) new-bindings-list))]
                 ;; Relation binding: ((?x ?y ...)) — spec is a list of one list of vars
                 [(and (pair? (car ivs))
                       (= (length (car ivs)) 1)
                       (pair? (caar ivs))
                       (for-all logic-var? (caar ivs)))
                  (let* ([vars (caar ivs)]
                         [tuples (car inps)]
                         [new-bindings-list
                           (apply append
                             (map (lambda (bindings)
                                    (map (lambda (tuple)
                                           (let add-vars ([vs vars] [ts tuple] [b bindings])
                                             (if (null? vs) b
                                                 (add-vars (cdr vs) (cdr ts)
                                                           (binding-set b (car vs) (car ts))))))
                                         tuples))
                                  bindings-list))])
                    (loop (cdr ivs) (cdr inps) new-bindings-list))]
                 ;; Tuple binding: (?x ?y ...) — spec is a list of vars (no ... suffix)
                 [(and (pair? (car ivs))
                       (for-all logic-var? (car ivs)))
                  (let* ([vars (car ivs)]
                         [tuple (car inps)]
                         [new-bindings-list
                           (map (lambda (bindings)
                                  (let add-vars ([vs vars] [ts tuple] [b bindings])
                                    (if (null? vs) b
                                        (add-vars (cdr vs) (cdr ts)
                                                  (binding-set b (car vs) (car ts))))))
                                bindings-list)])
                    (loop (cdr ivs) (cdr inps) new-bindings-list))]
                 ;; Scalar binding: ?x
                 [else
                  (let ([new-bindings-list
                          (map (lambda (bindings)
                                 (binding-set bindings (car ivs) (car inps)))
                               bindings-list)])
                    (loop (cdr ivs) (cdr inps) new-bindings-list))]))]
           ;; Parse rules from % input if present
           [rules-from-input
             (let ([%-pos (list-index (lambda (v) (eq? v '%)) in-vars)])
               (if %-pos
                   (parse-rules (list-ref inputs (- %-pos 1)))  ;; -1 for $
                   rules-ht))]
           ;; Reorder clauses for optimal execution
           ;; input-bindings is now a list of binding-sets
           [bound-at-start (if (null? input-bindings) '()
                               (vector->list (hashtable-keys (car input-bindings))))]
           [db-stats (db-value-stats db)]
           [ordered-clauses (reorder-clauses where-clauses bound-at-start schema db-stats)]
           ;; Compute live variable sets for projection pushdown
           [live-vars-steps (compute-live-vars-at-each-step ordered-clauses find-vars)]
           ;; Compute hash-join plan for top-level clause execution
           [hj-plan (hash-join-plan ordered-clauses bound-at-start schema)]
           ;; Execute: use evaluate-plan for top-level (hash-join aware),
           ;; evaluate-where-clauses is kept for recursive sub-query calls.
           ;; If hash-join plan has any hash-join steps, use evaluate-plan.
           ;; Otherwise fall through to the projection-aware evaluate-where-clauses.
           [has-hash-joins? (exists (lambda (s) (eq? (car s) 'hash-join)) hj-plan)]
           [result-bindings
             (if has-hash-joins?
                 (evaluate-plan db hj-plan input-bindings schema rules-from-input)
                 (evaluate-where-clauses db ordered-clauses
                                         input-bindings schema rules-from-input
                                         live-vars-steps))])
      (extract-find-results find-vars result-bindings)))

  ;; Helper: find position of element in list
  (def (list-index pred lst)
    (let loop ([l lst] [i 0])
      (cond [(null? l) #f]
            [(pred (car l)) i]
            [else (loop (cdr l) (+ i 1))])))

  ;; ---- Query explain ----
  ;; Returns the query plan (clause ordering, chosen indices) without executing.

  (def (explain-query query-form db-val . inputs)
    (let* ([parsed (parse-query query-form)]
           [schema (db-value-schema db-val)]
           [find-vars (parsed-query-find-vars parsed)]
           [in-vars (parsed-query-in-vars parsed)]
           [where-clauses (parsed-query-where-clauses parsed)]
           ;; Determine initially bound variables from inputs
           [bound-at-start
             (let loop ([ivs in-vars] [inps inputs] [bound '()])
               (cond
                 [(null? ivs) bound]
                 [(eq? (car ivs) '$) (loop (cdr ivs) inps bound)]
                 [(eq? (car ivs) '%)  (loop (cdr ivs) (cdr inps) bound)]
                 [(pair? (car ivs))
                  ;; Collection/tuple/relation: extract var names
                  (let ([vars (filter logic-var?
                                (let flatten ([x (car ivs)])
                                  (cond [(pair? x) (append (flatten (car x)) (flatten (cdr x)))]
                                        [(null? x) '()]
                                        [else (list x)])))])
                    (loop (cdr ivs) (cdr inps) (append vars bound)))]
                 [else
                  (loop (cdr ivs) (cdr inps) (cons (car ivs) bound))]))]
           [db-stats (db-value-stats db-val)]
           [ordered-clauses (reorder-clauses where-clauses bound-at-start schema db-stats)])
      ;; Build plan: for each clause, describe it and the chosen index
      (let build-plan ([clauses ordered-clauses] [bound bound-at-start] [plan '()])
        (if (null? clauses)
            `((find ,@find-vars)
              (in ,@in-vars)
              (plan ,@(reverse plan)))
            (let* ([clause (car clauses)]
                   [step
                     (cond
                       [(not-clause? clause)
                        `(not-filter ,clause)]
                       [(or-clause? clause)
                        `(or-union ,clause)]
                       [(data-pattern? clause)
                        (let ([idx (choose-index clause bound schema)])
                          `(scan ,idx ,clause))]
                       [(predicate-clause? clause)
                        `(filter ,clause)]
                       [(function-clause? clause)
                        `(compute ,clause)]
                       [else `(unknown ,clause)])]
                   [new-bound (append (clause-bound-vars clause bound) bound)])
              (build-plan (cdr clauses) new-bound (cons step plan)))))))

) ;; end library
