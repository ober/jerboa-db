#!chezscheme
;;; (jerboa-db query functions) — Built-in query predicates and functions
;;;
;;; Predicate clauses: [(> ?age 30)]  — filter, don't bind
;;; Function clauses:  [(str ?a " " ?b) ?full]  — compute and bind result

(library (jerboa-db query functions)
  (export
    builtin-predicate? builtin-function?
    apply-builtin-predicate apply-builtin-function
    resolve-builtin)

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

  ;; ---- Built-in predicates (return boolean, used for filtering) ----

  (def builtin-predicates
    `((>  . ,>) (<  . ,<) (>= . ,>=) (<= . ,<=)
      (=  . ,=) (not= . ,(lambda (a b) (not (equal? a b))))
      (zero? . ,zero?) (pos? . ,positive?) (neg? . ,negative?)
      (even? . ,even?) (odd? . ,odd?)
      (string-starts-with? . ,(lambda (s prefix) (and (string? s) (string? prefix)
                                (let ([pl (string-length prefix)])
                                  (and (<= pl (string-length s))
                                       (string=? (substring s 0 pl) prefix))))))
      (string-ends-with? . ,(lambda (s suffix) (and (string? s) (string? suffix)
                               (let ([sl (string-length s)] [xl (string-length suffix)])
                                 (and (<= xl sl)
                                      (string=? (substring s (- sl xl) sl) suffix))))))
      (string-contains? . ,(lambda (s sub)
                              (and (string? s) (string? sub)
                                   (let loop ([i 0])
                                     (cond [(> (+ i (string-length sub)) (string-length s)) #f]
                                           [(string=? (substring s i (+ i (string-length sub))) sub) #t]
                                           [else (loop (+ i 1))])))))))

  ;; ---- Built-in functions (return a value to bind) ----

  (def builtin-functions
    `((str . ,(lambda args
                (apply string-append
                       (map (lambda (x)
                              (cond [(string? x) x]
                                    [(number? x) (number->string x)]
                                    [(symbol? x) (symbol->string x)]
                                    [(boolean? x) (if x "true" "false")]
                                    [else (format "~a" x)]))
                            args))))
      (subs . ,(lambda (s start . rest)
                 (if (null? rest)
                     (substring s start (string-length s))
                     (substring s start (car rest)))))
      (upper-case . ,(lambda (s) (string-upcase s)))
      (lower-case . ,(lambda (s) (string-downcase s)))
      (count . ,(lambda (x)
                  (cond [(string? x) (string-length x)]
                        [(list? x) (length x)]
                        [(vector? x) (vector-length x)]
                        [else 0])))
      (+ . ,+) (- . ,-) (* . ,*) (/ . ,/)
      (mod . ,mod) (inc . ,(lambda (x) (+ x 1))) (dec . ,(lambda (x) (- x 1)))
      (abs . ,abs)
      (max . ,max) (min . ,min)
      (ground . ,(lambda (v) v))
      (tuple . ,list)
      (missing? . ,'SPECIAL)  ;; handled specially in engine
      (get-else . ,'SPECIAL)))  ;; handled specially in engine

  ;; ---- Lookup ----

  (def (builtin-predicate? name)
    (assq name builtin-predicates))

  (def (builtin-function? name)
    (assq name builtin-functions))

  (def (resolve-builtin name)
    (or (let ([p (assq name builtin-predicates)]) (and p (cdr p)))
        (let ([f (assq name builtin-functions)])  (and f (cdr f)))
        #f))

  (def (apply-builtin-predicate name args)
    (let ([proc (cdr (assq name builtin-predicates))])
      (apply proc args)))

  (def (apply-builtin-function name args)
    (let ([proc (cdr (assq name builtin-functions))])
      (apply proc args)))

) ;; end library
