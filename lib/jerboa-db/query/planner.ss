#!chezscheme
;;; (jerboa-db query planner) — Query plan optimization
;;;
;;; Reorders clauses to bind variables early (most selective first).
;;; Chooses the correct index for each data pattern based on bound variables.

(library (jerboa-db query planner)
  (export
    reorder-clauses choose-index
    clause-bound-vars clause-used-vars
    score-clause
    compute-live-vars-at-each-step
    hash-join-plan)

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
          (jerboa-db schema)
          (jerboa-db stats))

  ;; ---- Variable detection ----

  (def (logic-var? x)
    (and (symbol? x)
         (let ([s (symbol->string x)])
           (and (> (string-length s) 0)
                (char=? (string-ref s 0) #\?)))))

  ;; A data pattern: (something attr-symbol something ...)
  ;; First element is a var or literal, second is a non-logic-var symbol (attribute)
  (def (data-pattern? clause)
    (and (pair? clause)
         (not (pair? (car clause)))
         (>= (length clause) 3)
         (symbol? (cadr clause))
         (not (logic-var? (cadr clause)))))

  ;; Variables that a data-pattern clause binds (introduces)
  (def (clause-bound-vars clause already-bound)
    (filter (lambda (v) (and (logic-var? v) (not (memq v already-bound))))
            (cond
              ;; Not clause: (not sub-clause ...) — binds nothing (filter only)
              [(and (pair? clause) (eq? (car clause) 'not))
               '()]
              ;; Not-join clause: (not-join [vars...] sub-clause ...) — binds nothing
              [(and (pair? clause) (eq? (car clause) 'not-join))
               '()]
              ;; Or clause: (or alt ...) — binds the union of what alternatives bind
              [(and (pair? clause) (eq? (car clause) 'or))
               (if (null? (cdr clause)) '()
                   ;; Take the intersection of bound vars across alternatives
                   (let ([alt-vars (map (lambda (alt) (clause-bound-vars alt already-bound))
                                        (cdr clause))])
                     (fold-left (lambda (acc vars)
                                  (filter (lambda (v) (memq v vars)) acc))
                                (car alt-vars) (cdr alt-vars))))]
              ;; Data pattern: (?e attr ?v) or (?e attr ?v ?tx) or (?e attr ?v ?tx ?op)
              [(and (pair? clause) (not (pair? (car clause))))
               (filter logic-var? clause)]
              ;; Predicate: ((pred args...)) — binds nothing
              [(and (pair? clause) (pair? (car clause))
                    (null? (cdr clause)))
               '()]
              ;; Function: ((fn args...) ?result) — binds ?result
              [(and (pair? clause) (pair? (car clause))
                    (pair? (cdr clause)))
               (filter (lambda (v) (and (logic-var? v) (not (memq v already-bound))))
                       (cdr clause))]
              [else '()])))

  ;; Variables used (referenced) by a clause
  (def (clause-used-vars clause)
    (cond
      ;; Not/or: collect vars from sub-clauses
      [(and (pair? clause) (memq (car clause) '(not or)))
       (apply append (map clause-used-vars (cdr clause)))]
      ;; Not-join: uses explicit join vars plus vars in sub-clauses
      [(and (pair? clause) (eq? (car clause) 'not-join))
       (let ([join-vars (cadr clause)]
             [sub-vars  (apply append (map clause-used-vars (cddr clause)))])
         (append join-vars sub-vars))]
      [(and (pair? clause) (not (pair? (car clause))))
       (filter logic-var? clause)]
      [(and (pair? clause) (pair? (car clause)))
       (filter logic-var? (append (car clause) (cdr clause)))]
      [else '()]))

  ;; ---- Selectivity scoring ----
  ;; Higher score = more selective = should come first.
  ;; Optional db-stats argument enables attribute-count-based scoring.

  (def (score-clause clause bound-vars schema . rest)
    (let ([db-stats (and (pair? rest) (car rest))])
      (cond
        ;; Not clause: should come after its sub-clause vars are bound.
        ;; Score low so it's placed late (it's a filter).
        [(and (pair? clause) (eq? (car clause) 'not))
         (let ([used (clause-used-vars clause)])
           (if (for-all (lambda (v) (memq v bound-vars)) used) 3 -100))]
        ;; Not-join clause: score 3 when all join vars are bound, -100 otherwise.
        [(and (pair? clause) (eq? (car clause) 'not-join))
         (let ([join-vars (cadr clause)])
           (if (for-all (lambda (v) (memq v bound-vars)) join-vars) 3 -100))]
        ;; Or clause: score like the best alternative
        [(and (pair? clause) (eq? (car clause) 'or))
         (let ([alt-scores (map (lambda (alt) (score-clause alt bound-vars schema db-stats))
                                 (cdr clause))])
           (if (null? alt-scores) 0 (apply max alt-scores)))]
        ;; Data pattern: (?e attr ?v ...)
        [(and (pair? clause) (not (pair? (car clause)))
              (>= (length clause) 3))
         (let* ([e-pos (car clause)]
                [a-pos (cadr clause)]
                [v-pos (caddr clause)]
                [e-bound? (or (not (logic-var? e-pos)) (memq e-pos bound-vars))]
                [a-bound? (not (logic-var? a-pos))]  ;; attrs are always concrete
                [v-bound? (or (not (logic-var? v-pos)) (memq v-pos bound-vars))])
           (let* ([attr (and a-bound? (schema-lookup-by-ident schema a-pos))]
                  [aid  (and attr (db-attribute-id attr))]
                  ;; Stats-based bonus: smaller attribute -> higher score.
                  ;; Only applies when entity is unbound (otherwise EAVT point-lookup
                  ;; dominates regardless of attribute cardinality).
                  ;; Divide by 10: contributes 0-90, below entity-bound (100) and
                  ;; unique-attr (90) but above plain attribute-bound (20).
                  [stat-bonus (if (and db-stats aid (not e-bound?))
                                  (quotient (db-stats-selectivity-score db-stats aid) 10)
                                  0)])
             (+
               (if e-bound? 100 0)        ;; entity bound: very selective
               (if (and attr (db-attribute-unique attr)) 90 0)  ;; unique attr: point lookup
               ;; V-bound + scalar attr: AVET range scan (fast).
               ;; V-bound + ref attr: VAET reverse lookup (also fast).
               (if v-bound? (if (and attr (avet-eligible? attr)) 60 50) 0)
               (if a-bound? 20 0)
               stat-bonus)))]  ;; 0-90 based on attribute selectivity
        ;; Predicate/function clauses: if ALL vars are bound, score maximally so
        ;; the clause fires as an early filter immediately after its last dep.
        ;; Otherwise score proportionally to bound vars.
        [(and (pair? clause) (pair? (car clause)))
         (let ([used (clause-used-vars clause)])
           (if (for-all (lambda (v) (memq v bound-vars)) used)
               1000
               (* 5 (length (filter (lambda (v) (memq v bound-vars)) used)))))]
        [else 0])))

  ;; ---- Clause reordering ----
  ;; Greedy algorithm: pick the highest-scoring clause, add its bindings,
  ;; repeat until all clauses are placed.
  ;; Optional db-stats argument enables attribute-count-based ordering.

  (def (reorder-clauses clauses initial-bound-vars schema . rest)
    (let ([db-stats (and (pair? rest) (car rest))])
      (let loop ([remaining clauses]
                 [bound-vars initial-bound-vars]
                 [result '()])
        (if (null? remaining)
            (reverse result)
            (let* ([scored (map (lambda (c)
                                  (cons (score-clause c bound-vars schema db-stats) c))
                                remaining)]
                   [sorted (sort scored (lambda (a b) (> (car a) (car b))))]
                   [best (cdar sorted)]
                   [new-bound (clause-bound-vars best bound-vars)])
              (loop (remq best remaining)
                    (append new-bound bound-vars)
                    (cons best result)))))))

  ;; ---- Live variable analysis (projection pushdown support) ----

  ;; Union two lists using eq? identity, without duplicates.
  (def (lset-union* lst1 lst2)
    (fold-left (lambda (acc x) (if (memq x acc) acc (cons x acc))) lst1 lst2))

  ;; Extract logic vars referenced in a find-var spec.
  ;; Plain var → (var). Aggregate form (agg ?var) → (?var). Literal → ().
  (def (find-var-used-vars fv)
    (cond
      [(and (pair? fv) (not (null? (cdr fv)))) ; (agg ?var) or (agg ?var1 ?var2 ...)
       (filter logic-var? (cdr fv))]
      [(logic-var? fv) (list fv)]
      [else '()]))

  ;; Compute for each step i (0..N) which variables must be live BEFORE clause[i].
  ;; Returns a list of N+1 symbol lists.
  ;; live-after[i] = find-used-vars ∪ clause-used-vars for all clauses[i+1..N-1].
  ;; live-before[i] = live-after[i] ∪ clause-used-vars(clause[i]).
  ;; The full list returned is: (live-before[0] live-before[1] ... live-before[N-1] find-used).
  (def (compute-live-vars-at-each-step planned-clauses find-vars)
    (let* ([find-used (apply append (map find-var-used-vars find-vars))]
           [n (length planned-clauses)]
           [clauses-vec (list->vector planned-clauses)])
      ;; Walk right-to-left, accumulating live vars.
      ;; result starts as (find-used) and we prepend live-before[i] at each step.
      (let loop ([i (- n 1)] [live find-used] [result (list find-used)])
        (if (< i 0)
            result
            (let* ([clause (vector-ref clauses-vec i)]
                   [used (clause-used-vars clause)]
                   [new-live (lset-union* live used)])
              (loop (- i 1) new-live (cons new-live result)))))))

  ;; ---- Index selection ----
  ;; Choose which index to scan for a data pattern.

  (def (choose-index pattern bound-vars schema)
    ;; pattern: (?e attr ?v) or similar
    (let* ([e-pos (car pattern)]
           [a-pos (cadr pattern)]
           [v-pos (if (>= (length pattern) 3) (caddr pattern) #f)]
           [e-bound? (or (not (logic-var? e-pos)) (memq e-pos bound-vars))]
           [v-bound? (and v-pos (or (not (logic-var? v-pos)) (memq v-pos bound-vars)))]
           [attr (schema-lookup-by-ident schema a-pos)])
      (cond
        ;; Entity known -> EAVT (fastest: point lookup by entity)
        [e-bound? 'eavt]
        ;; Value known + ref type -> VAET (reverse ref lookup)
        [(and v-bound? attr (ref-type? attr)) 'vaet]
        ;; Value known + scalar attr -> AVET (all scalar attrs now indexed)
        [(and v-bound? attr (avet-eligible? attr)) 'avet]
        ;; Default: scan by attribute -> AEVT
        [else 'aevt])))

  ;; ---- Hash-join plan ----
  ;; Analyze a list of reordered clauses and group consecutive data-pattern pairs
  ;; that form a binary join (share an unbound variable, both are data patterns,
  ;; and the shared var isn't already bound from outside).
  ;; Returns a list of plan steps, each one of:
  ;;   (hash-join clause-A clause-B join-vars) — execute as hash-join
  ;;   (sequential clause)                     — execute normally

  (def (hash-join-plan clauses initial-bound-vars schema)
    (let loop ([clauses clauses] [bound initial-bound-vars] [result '()])
      (cond
        [(null? clauses)
         (reverse result)]
        [(null? (cdr clauses))
         ;; Last clause — always sequential
         (reverse (cons (list 'sequential (car clauses)) result))]
        [else
         (let* ([ca     (car clauses)]
                [cb     (cadr clauses)]
                [vars-a (filter logic-var? (clause-used-vars ca))]
                [vars-b (filter logic-var? (clause-used-vars cb))]
                ;; Shared vars that are NOT already bound from outside
                [shared (filter (lambda (v)
                                  (and (memq v vars-a)
                                       (memq v vars-b)
                                       (not (memq v bound))))
                                vars-a)]
                ;; Only hash-join if: both are bare data patterns, share ≥1 unbound var,
                ;; and both patterns have no entity pre-bound (otherwise EAVT point-lookup
                ;; is always faster than a hash-join).
                ;; Correctness requirement: neither clause may reference a bound var that
                ;; the other doesn't also reference — otherwise the two sides would see
                ;; different external constraints and produce incorrect cross-product rows.
                [bound-in-a (filter (lambda (v) (memq v bound)) vars-a)]
                [bound-in-b (filter (lambda (v) (memq v bound)) vars-b)]
                ;; Each bound-var used by A must also appear in B, and vice versa.
                [symmetric-bound?
                  (and (for-all (lambda (v) (memq v vars-b)) bound-in-a)
                       (for-all (lambda (v) (memq v vars-a)) bound-in-b))]
                [joinable? (and (pair? shared)
                                (data-pattern? ca)
                                (data-pattern? cb)
                                symmetric-bound?
                                ;; Don't hash-join when entity is bound — EAVT wins
                                (logic-var? (car ca))
                                (not (memq (car ca) bound))
                                (logic-var? (car cb))
                                (not (memq (car cb) bound)))])
           (if joinable?
               (let ([new-bound (append vars-a vars-b bound)])
                 (loop (cddr clauses) new-bound
                       (cons (list 'hash-join ca cb shared) result)))
               (let ([new-bound (append (clause-bound-vars ca bound) bound)])
                 (loop (cdr clauses) new-bound
                       (cons (list 'sequential ca) result)))))])))

) ;; end library
