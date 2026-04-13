#!chezscheme
;;; (jerboa-db query aggregates) — Aggregate functions for query results
;;;
;;; Aggregates reduce a collection of values to a single result.
;;; Used in :find clauses: (count ?x), (sum ?x), (avg ?x), etc.

(library (jerboa-db query aggregates)
  (export
    aggregate? aggregate-apply
    register-aggregate!)

  (import (except (chezscheme)
                  make-hash-table hash-table?
                  sort sort!
                  printf fprintf
                  path-extension path-absolute?
                  with-input-from-string with-output-to-string
                  iota 1+ 1-
                  partition
                  make-date make-time)
          (jerboa prelude))

  ;; ---- Aggregate registry ----

  (def aggregate-registry (make-eq-hashtable))

  (def (register-aggregate! name proc)
    ;; proc: (list-of-values) -> result
    (hashtable-set! aggregate-registry name proc))

  (def (aggregate? name)
    (hashtable-contains? aggregate-registry name))

  (def (aggregate-apply name values)
    (let ([proc (hashtable-ref aggregate-registry name #f)])
      (unless proc (error 'aggregate-apply "Unknown aggregate" name))
      (proc values)))

  ;; ---- Built-in aggregates ----

  ;; count: number of values
  (register-aggregate! 'count length)

  ;; count-distinct: number of unique values
  (register-aggregate! 'count-distinct
    (lambda (vs)
      (let ([ht (make-hashtable equal-hash equal?)])
        (for-each (lambda (v) (hashtable-set! ht v #t)) vs)
        (hashtable-size ht))))

  ;; sum: sum of numeric values
  (register-aggregate! 'sum
    (lambda (vs) (apply + (filter number? vs))))

  ;; avg: average of numeric values
  (register-aggregate! 'avg
    (lambda (vs)
      (let ([nums (filter number? vs)])
        (if (null? nums) 0
            (inexact (/ (apply + nums) (length nums)))))))

  ;; min: minimum value
  (register-aggregate! 'min
    (lambda (vs)
      (if (null? vs) #f
          (fold-left (lambda (best v)
                       (if (or (not best)
                               (and (number? v) (number? best) (< v best))
                               (and (string? v) (string? best) (string<? v best)))
                           v best))
                     #f vs))))

  ;; max: maximum value
  (register-aggregate! 'max
    (lambda (vs)
      (if (null? vs) #f
          (fold-left (lambda (best v)
                       (if (or (not best)
                               (and (number? v) (number? best) (> v best))
                               (and (string? v) (string? best) (string>? v best)))
                           v best))
                     #f vs))))

  ;; median: middle value (sorts numerically)
  (register-aggregate! 'median
    (lambda (vs)
      (let ([sorted (sort < (filter number? vs))])
        (if (null? sorted) #f
            (let ([n (length sorted)])
              (if (odd? n)
                  (list-ref sorted (quotient n 2))
                  (inexact (/ (+ (list-ref sorted (- (quotient n 2) 1))
                                 (list-ref sorted (quotient n 2)))
                              2))))))))

  ;; distinct: set of distinct values
  (register-aggregate! 'distinct
    (lambda (vs)
      (let ([ht (make-hashtable equal-hash equal?)])
        (for-each (lambda (v) (hashtable-set! ht v #t)) vs)
        (vector->list (hashtable-keys ht)))))

  ;; sample/rand: N random samples
  (register-aggregate! 'sample
    (lambda (vs)
      ;; When called from engine, first element is N
      vs))  ;; engine handles arg extraction

  (register-aggregate! 'rand
    (lambda (vs) vs))  ;; alias for sample

) ;; end library
