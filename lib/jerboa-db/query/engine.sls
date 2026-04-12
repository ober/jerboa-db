#!chezscheme
;;; (jerboa-db query engine) — Datalog query compiler and executor
;;;
;;; Parses Datomic-style query syntax, plans execution, and evaluates
;;; against database indices. Supports data patterns, predicates,
;;; function clauses, aggregation, and rules.

(library (jerboa-db query engine)
  (export query-db parse-query)

  (import (chezscheme)
          (jerboa-db datom)
          (jerboa-db schema)
          (jerboa-db index protocol)
          (jerboa-db history)
          (jerboa-db query functions)
          (jerboa-db query aggregates)
          (jerboa-db query planner)
          (jerboa-db query rules))

  ;; ---- Logic variable helpers ----

  (define (logic-var? x)
    (and (symbol? x)
         (> (string-length (symbol->string x)) 0)
         (char=? (string-ref (symbol->string x) 0) #\?)))

  ;; ---- Binding environment ----
  ;; A binding is an alist: ((var . value) ...)

  (define empty-bindings '())

  (define (binding-ref bindings var)
    (let ([found (assq var bindings)])
      (and found (cdr found))))

  (define (binding-set bindings var val)
    (cons (cons var val) bindings))

  (define (resolve-in-bindings bindings x)
    (if (logic-var? x)
        (binding-ref bindings x)
        x))

  ;; ---- Query parsing ----
  ;; Query format: ((find vars...) (in inputs...) (where clauses...))
  ;; All sections are identified by their car symbol.

  (define-record-type parsed-query
    (fields find-vars    ;; list of symbols or (agg ?var) forms
            in-vars      ;; list of input symbols ($ = db)
            where-clauses ;; list of clause forms
            rules))      ;; parsed rule definitions or #f

  (define (parse-query form)
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

  (define (resolve-current-datoms datoms)
    ;; datoms come sorted from an index with tx as last key, so within each
    ;; (e,a,v) group, later entries have higher tx. Walk the list, tracking
    ;; the latest datom per (e,a,v).
    (let ([ht (make-hashtable equal-hash equal?)])
      (for-each
        (lambda (d)
          (let ([key (list (datom-e d) (datom-a d) (datom-v d))])
            (let ([existing (hashtable-ref ht key #f)])
              (when (or (not existing)
                        (> (datom-tx d) (datom-tx existing)))
                (hashtable-set! ht key d)))))
        datoms)
      ;; Keep only triples whose latest datom is an assertion
      (let-values ([(keys vals) (hashtable-entries ht)])
        (let ([vlist (vector->list vals)])
          (filter datom-added? vlist)))))

  ;; ---- Data pattern evaluation ----
  ;; A data pattern like (?e person/name ?name) is matched against an index.

  (define (evaluate-data-pattern db pattern bindings schema)
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
                           [(and v-val (indexed-attr? attr)) 'avet]
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

  ;; Try to unify a datom with the pattern, extending bindings.
  ;; Returns extended bindings or #f if unification fails.
  (define (try-unify-datom d e-spec a-spec v-spec tx-spec op-spec bindings)
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

  ;; ---- Predicate clause evaluation ----
  ;; ((pred arg1 arg2 ...)) — returns filtered bindings

  (define (evaluate-predicate-clause clause bindings)
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

  (define (evaluate-function-clause clause bindings)
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

  (define (data-pattern? clause)
    ;; A data pattern: (something attr-symbol something ...)
    ;; First element is a var or literal, second is a symbol (attribute)
    (and (pair? clause)
         (not (pair? (car clause)))
         (>= (length clause) 3)
         (symbol? (cadr clause))
         (not (logic-var? (cadr clause)))))

  (define (predicate-clause? clause)
    ;; ((pred args...)) — single nested list, no result binding
    (and (pair? clause) (pair? (car clause)) (null? (cdr clause))))

  (define (function-clause? clause)
    ;; ((fn args...) ?result) — nested list + result variable
    (and (pair? clause) (pair? (car clause)) (pair? (cdr clause))))

  (define (rule-invocation? clause rules-ht)
    ;; (rule-name args...) where rule-name is in the rule registry
    (and (pair? clause)
         (symbol? (car clause))
         (not (logic-var? (car clause)))
         rules-ht
         (rule-name? (car clause) rules-ht)))

  ;; ---- Main query evaluation ----

  (define (evaluate-where-clauses db clauses bindings-list schema rules-ht)
    (if (null? clauses)
        bindings-list
        (let* ([clause (car clauses)]
               [rest (cdr clauses)]
               [new-bindings-list
                 (apply append
                   (map (lambda (bindings)
                          (evaluate-single-clause db clause bindings schema rules-ht))
                        bindings-list))])
          (evaluate-where-clauses db rest new-bindings-list schema rules-ht))))

  (define (evaluate-single-clause db clause bindings schema rules-ht)
    (cond
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

  (define (evaluate-rule-clause db clause bindings schema rules-ht)
    (let* ([name (car clause)]
           [args (cdr clause)]
           [alternatives (expand-rule-invocation name args rules-ht)])
      ;; Each alternative is a list of clauses. Evaluate each and union results.
      (apply append
        (map (lambda (alt-clauses)
               (evaluate-where-clauses db alt-clauses (list bindings) schema rules-ht))
             alternatives))))

  ;; ---- Find clause processing ----
  ;; Extract result tuples from bindings based on :find specification.

  (define (extract-find-results find-vars bindings-list)
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

  (define (any-aggregates? find-vars)
    (exists (lambda (v) (and (pair? v) (aggregate? (car v)))) find-vars))

  (define (apply-aggregates find-vars bindings-list)
    ;; Group non-aggregate vars, then aggregate within each group.
    (let* ([grouping-vars (filter (lambda (v) (not (and (pair? v) (aggregate? (car v)))))
                                  find-vars)]
           [agg-specs (filter (lambda (v) (and (pair? v) (aggregate? (car v))))
                              find-vars)]
           ;; Group bindings by grouping vars
           [groups (group-bindings grouping-vars bindings-list)])
      ;; For each group, compute aggregates
      (map (lambda (group)
             (let ([sample-binding (car (cdr group))]
                   [group-bindings (cdr group)])
               (map (lambda (fv)
                      (if (and (pair? fv) (aggregate? (car fv)))
                          ;; Aggregate: collect values and apply
                          (let* ([agg-name (car fv)]
                                 [agg-var (cadr fv)]
                                 [values (map (lambda (b) (binding-ref b agg-var))
                                              group-bindings)])
                            (aggregate-apply agg-name values))
                          ;; Non-aggregate: extract from sample
                          (if (logic-var? fv)
                              (binding-ref sample-binding fv)
                              fv)))
                    find-vars)))
           groups)))

  (define (group-bindings grouping-vars bindings-list)
    ;; Group bindings by the values of grouping-vars.
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
  (define (deduplicate-results results)
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

  ;; ---- Top-level query function ----

  (define (query-db parsed db . inputs)
    (let* ([schema (db-value-schema db)]
           [find-vars (parsed-query-find-vars parsed)]
           [in-vars (parsed-query-in-vars parsed)]
           [where-clauses (parsed-query-where-clauses parsed)]
           [rules-ht (parsed-query-rules parsed)]
           ;; Build initial bindings from inputs
           ;; $ = db (implicit), % = rules, other = user inputs
           [input-bindings
             (let loop ([ivs in-vars] [inps inputs] [bindings empty-bindings])
               (cond
                 [(null? ivs) bindings]
                 [(eq? (car ivs) '$) (loop (cdr ivs) inps bindings)]
                 [(eq? (car ivs) '%)
                  ;; % means rules are provided as the next input
                  (loop (cdr ivs) (cdr inps) bindings)]
                 [else
                  (loop (cdr ivs) (cdr inps)
                        (binding-set bindings (car ivs) (car inps)))]))]
           ;; Parse rules from % input if present
           [rules-from-input
             (let ([%-pos (list-index (lambda (v) (eq? v '%)) in-vars)])
               (if %-pos
                   (parse-rules (list-ref inputs (- %-pos 1)))  ;; -1 for $
                   rules-ht))]
           ;; Reorder clauses for optimal execution
           [bound-at-start (map car input-bindings)]
           [ordered-clauses (reorder-clauses where-clauses bound-at-start schema)]
           ;; Execute
           [result-bindings
             (evaluate-where-clauses db ordered-clauses
                                     (list input-bindings) schema rules-from-input)])
      (extract-find-results find-vars result-bindings)))

  ;; Helper: find position of element in list
  (define (list-index pred lst)
    (let loop ([l lst] [i 0])
      (cond [(null? l) #f]
            [(pred (car l)) i]
            [else (loop (cdr l) (+ i 1))])))

) ;; end library
