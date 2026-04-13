#!chezscheme
;;; (jerboa-db query rules) — Rule expansion and recursive evaluation
;;;
;;; Rules are reusable query fragments, enabling recursion (e.g., ancestor).
;;; Evaluated via semi-naive fixed-point iteration.

(library (jerboa-db query rules)
  (export
    parse-rules expand-rule-invocation
    rule-name? rule-lookup
    evaluate-rules-fixed-point)

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

  ;; ---- Rule parsing ----
  ;; Rule definition: ((rule-name ?arg ...) clause clause ...)
  ;; Multiple rules with the same name = disjunction (OR).

  (defstruct rule-def
    (name     ;; symbol
     params   ;; list of logic variables
     clauses)) ;; list of clause lists (each alternative)

  (def (parse-rules rule-forms)
    ;; Returns an eq-hashtable: name -> list of rule-def
    (let ([ht (make-eq-hashtable)])
      (for-each
        (lambda (form)
          (let* ([head (car form)]
                 [name (car head)]
                 [params (cdr head)]
                 [body (cdr form)]
                 [existing (hashtable-ref ht name '())])
            (hashtable-set! ht name
              (cons (make-rule-def name params body) existing))))
        rule-forms)
      ht))

  (def (rule-name? name rules-ht)
    (and rules-ht (hashtable-contains? rules-ht name)))

  (def (rule-lookup name rules-ht)
    (and rules-ht (hashtable-ref rules-ht name '())))

  ;; ---- Rule expansion ----
  ;; When a rule invocation (rule-name ?x ?y) appears in a query,
  ;; substitute the rule's body with fresh variable names.

  (def fresh-counter 0)

  (def (fresh-var base)
    (set! fresh-counter (+ fresh-counter 1))
    (string->symbol
      (string-append "?" (symbol->string base) "__"
                     (number->string fresh-counter))))

  (def (logic-var? x)
    (and (symbol? x)
         (> (string-length (symbol->string x)) 0)
         (char=? (string-ref (symbol->string x) 0) #\?)))

  ;; Rename variables in clauses to avoid capture
  (def (rename-vars clauses params args)
    ;; Build substitution: param -> arg, internal vars -> fresh
    (let ([subst (make-eq-hashtable)]
          [internal-vars '()])
      ;; Map params to args
      (for-each (lambda (p a) (hashtable-set! subst p a))
                params args)
      ;; Collect internal variables (in body but not in params)
      (for-each
        (lambda (clause)
          (for-each
            (lambda (x)
              (when (and (logic-var? x)
                         (not (hashtable-contains? subst x)))
                (hashtable-set! subst x (fresh-var x))))
            (flatten-clause clause)))
        clauses)
      ;; Apply substitution
      (map (lambda (clause) (subst-clause clause subst)) clauses)))

  (def (flatten-clause clause)
    (cond [(pair? clause) (append (flatten-clause (car clause))
                                  (flatten-clause (cdr clause)))]
          [(null? clause) '()]
          [else (list clause)]))

  (def (subst-clause clause subst)
    (cond
      [(and (symbol? clause) (logic-var? clause))
       (hashtable-ref subst clause clause)]
      [(pair? clause)
       (cons (subst-clause (car clause) subst)
             (subst-clause (cdr clause) subst))]
      [else clause]))

  (def (expand-rule-invocation name args rules-ht)
    ;; Returns a list of clause-lists (alternatives for this rule).
    ;; Each alternative is a list of clauses with variables renamed.
    (let ([defs (rule-lookup name rules-ht)])
      (map (lambda (rdef)
             (rename-vars (rule-def-clauses rdef)
                          (rule-def-params rdef)
                          args))
           defs)))

  ;; ---- Fixed-point evaluation ----
  ;; For recursive rules, iterate until no new bindings are produced.

  (def (evaluate-rules-fixed-point eval-fn initial-bindings max-iterations)
    ;; eval-fn: (bindings) -> new-bindings
    ;; Returns accumulated bindings when fixed point reached.
    (let loop ([bindings initial-bindings]
               [iteration 0])
      (if (>= iteration max-iterations)
          bindings  ;; Safety limit
          (let ([new-bindings (eval-fn bindings)])
            (if (= (length new-bindings) (length bindings))
                bindings  ;; Fixed point
                (loop new-bindings (+ iteration 1)))))))

) ;; end library
