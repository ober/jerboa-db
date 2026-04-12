#!chezscheme
;;; (jerboa-db query planner) — Query plan optimization
;;;
;;; Reorders clauses to bind variables early (most selective first).
;;; Chooses the correct index for each data pattern based on bound variables.

(library (jerboa-db query planner)
  (export
    reorder-clauses choose-index
    clause-bound-vars clause-used-vars
    score-clause)

  (import (chezscheme)
          (jerboa-db schema))

  ;; ---- Variable detection ----

  (define (logic-var? x)
    (and (symbol? x)
         (let ([s (symbol->string x)])
           (and (> (string-length s) 0)
                (char=? (string-ref s 0) #\?)))))

  ;; Variables that a data-pattern clause binds (introduces)
  (define (clause-bound-vars clause already-bound)
    (filter (lambda (v) (and (logic-var? v) (not (memq v already-bound))))
            (cond
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
  (define (clause-used-vars clause)
    (cond
      [(and (pair? clause) (not (pair? (car clause))))
       (filter logic-var? clause)]
      [(and (pair? clause) (pair? (car clause)))
       (filter logic-var? (append (car clause) (cdr clause)))]
      [else '()]))

  ;; ---- Selectivity scoring ----
  ;; Higher score = more selective = should come first.

  (define (score-clause clause bound-vars schema)
    (cond
      ;; Data pattern: (?e attr ?v ...)
      [(and (pair? clause) (not (pair? (car clause)))
            (>= (length clause) 3))
       (let* ([e-pos (car clause)]
              [a-pos (cadr clause)]
              [v-pos (caddr clause)]
              [e-bound? (or (not (logic-var? e-pos)) (memq e-pos bound-vars))]
              [a-bound? (not (logic-var? a-pos))]  ;; attrs are always concrete
              [v-bound? (or (not (logic-var? v-pos)) (memq v-pos bound-vars))])
         (let ([attr (and a-bound? (schema-lookup-by-ident schema a-pos))])
           (+
             (if e-bound? 100 0)        ;; entity bound: very selective
             (if (and attr (db-attribute-unique attr)) 90 0)  ;; unique attr
             (if v-bound? 50 0)         ;; value bound
             (if a-bound? 20 0)         ;; attribute bound (always true for valid queries)
             (if (and attr (indexed-attr? attr)) 10 0))))]  ;; indexed
      ;; Predicate/function clauses: score by how many vars are already bound
      [(and (pair? clause) (pair? (car clause)))
       (let ([used (clause-used-vars clause)])
         (* 5 (length (filter (lambda (v) (memq v bound-vars)) used))))]
      [else 0]))

  ;; ---- Clause reordering ----
  ;; Greedy algorithm: pick the highest-scoring clause, add its bindings,
  ;; repeat until all clauses are placed.

  (define (reorder-clauses clauses initial-bound-vars schema)
    (let loop ([remaining clauses]
               [bound-vars initial-bound-vars]
               [result '()])
      (if (null? remaining)
          (reverse result)
          (let* ([scored (map (lambda (c)
                                (cons (score-clause c bound-vars schema) c))
                              remaining)]
                 [sorted (sort (lambda (a b) (> (car a) (car b))) scored)]
                 [best (cdar sorted)]
                 [new-bound (clause-bound-vars best bound-vars)])
            (loop (remq best remaining)
                  (append new-bound bound-vars)
                  (cons best result))))))

  ;; ---- Index selection ----
  ;; Choose which index to scan for a data pattern.

  (define (choose-index pattern bound-vars schema)
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
        ;; Value known + indexed -> AVET (value lookup)
        [(and v-bound? attr (indexed-attr? attr)) 'avet]
        ;; Default: scan by attribute -> AEVT
        [else 'aevt])))

) ;; end library
