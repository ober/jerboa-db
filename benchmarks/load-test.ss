#!/usr/bin/env scheme --libdirs lib:~/mine/jerboa/lib --script
;;; Jerboa-DB load test
;;;
;;; Measures write throughput, query throughput, and scaling characteristics.
;;;
;;; Usage:
;;;   scheme --libdirs "lib:~/mine/jerboa/lib" --script benchmarks/load-test.ss
;;;   scheme --libdirs "lib:~/mine/jerboa/lib" --script benchmarks/load-test.ss --quick

(import (jerboa prelude)
        (jerboa-db core))

;; ---- Config ----

(def args (cdr (command-line)))

(def quick?
  (and (pair? args) (string=? (car args) "--quick")))

;; Scale factor: quick mode runs 1/10th the iterations
(def (S n) (if quick? (max 1 (quotient n 10)) n))

;; ---- Timing ----

(def (now-ms)
  (let ([t (current-time 'time-monotonic)])
    (+ (* 1000.0 (time-second t))
       (/ (time-nanosecond t) 1e6))))

(def (elapsed-ms start)
  (- (now-ms) start))

(def (ops-per-sec n ms)
  (if (> ms 0.01)
      (inexact->exact (round (* n (/ 1000.0 ms))))
      0))

;; ---- Formatting ----

(def (rpad s width)
  (let ([len (string-length s)])
    (if (>= len width) s
        (string-append s (make-string (- width len) #\space)))))

(def (lpad s width)
  (let ([len (string-length s)])
    (if (>= len width) s
        (string-append (make-string (- width len) #\space) s))))

(def (fmt-int n)    (lpad (number->string (inexact->exact (round n))) 8))
(def (fmt-rate n)   (lpad (number->string (ops-per-sec-raw n)) 9))
(def (fmt-ms ms)    (lpad (number->string (inexact->exact (round ms))) 7))
(def (fmt-ms-f ms)
  ;; Show one decimal for small values
  (let ([rounded (/ (round (* ms 10)) 10.0)])
    (lpad (number->string rounded) 8)))

(def (ops-per-sec-raw n)
  ;; returns the integer rate, avoiding division by near-zero
  (+ 0 (ops-per-sec n (max 0.01 (elapsed-since-start)))))

(def elapsed-since-start 0) ;; placeholder — overridden per scenario

;; ---- Printing ----

(def (section title)
  (displayln "")
  (displayln "--- " title " ---"))

(def (result-row label n ms)
  (displayln
    "  " (rpad label 38)
    (fmt-int n) " ops"
    (fmt-ms ms) " ms"
    "  →  " (fmt-int (ops-per-sec n ms)) " ops/sec"))

(def (latency-row label ms)
  (displayln "  " (rpad label 30) (fmt-ms-f ms) " ms"))

;; ---- Schema and data generation ----

(def (make-conn)
  (let ([conn (connect ":memory:")])
    (transact! conn
      (list
        `((db/ident . user/name)
          (db/valueType . db.type/string)
          (db/cardinality . db.cardinality/one))
        `((db/ident . user/email)
          (db/valueType . db.type/string)
          (db/cardinality . db.cardinality/one)
          (db/unique . db.unique/identity))
        `((db/ident . user/age)
          (db/valueType . db.type/long)
          (db/cardinality . db.cardinality/one))
        `((db/ident . user/score)
          (db/valueType . db.type/double)
          (db/cardinality . db.cardinality/one))
        `((db/ident . user/active)
          (db/valueType . db.type/boolean)
          (db/cardinality . db.cardinality/one))))
    conn))

(def (user-op i)
  `((user/name   . ,(str "User-" i))
    (user/email  . ,(str "user" i "@example.com"))
    (user/age    . ,(+ 18 (modulo i 62)))
    (user/score  . ,(exact->inexact (/ (* i 13) 100.0)))
    (user/active . ,(even? i))))

;; Bulk-load n entities into conn in one transaction
(def (bulk-load! conn n)
  (transact! conn
    (for/collect ([i (in-range n)])
      (user-op i))))

;; ---- Scenario 1: Individual write throughput ----
;; Every entity is its own transaction — worst case latency.

(def (bench-individual-writes n)
  (let ([conn (make-conn)]
        [start (now-ms)])
    (dotimes (i n)
      (transact! conn (list (user-op i))))
    (let ([ms (elapsed-ms start)])
      (close conn)
      ms)))

;; ---- Scenario 2: Batch write throughput ----
;; All entities in one transaction — best case throughput.
;; Also test intermediate batch sizes.

(def (bench-batch-writes n batch-size)
  (let ([conn (make-conn)]
        [start (now-ms)])
    (let loop ([offset 0])
      (when (< offset n)
        (let ([end (min (+ offset batch-size) n)])
          (transact! conn
            (for/collect ([i (in-range offset end)])
              (user-op i)))
          (loop end))))
    (let ([ms (elapsed-ms start)])
      (close conn)
      ms)))

;; ---- Scenario 3: Query throughput ----
;; Database pre-populated; run queries in a tight loop.

(def (bench-queries conn n-queries)
  (let* ([d (db conn)]
         ;; exact match
         [t1 (now-ms)]
         [_ (dotimes (_ n-queries)
              (q '((find ?e) (where (?e user/age 30))) d))]
         [ms-exact (elapsed-ms t1)]
         ;; predicate filter
         [t2 (now-ms)]
         [_ (dotimes (_ n-queries)
              (q '((find ?e) (where (?e user/age ?a) [(> ?a 50)])) d))]
         [ms-pred (elapsed-ms t2)]
         ;; two-attribute join (capped at 1/10 of n-queries to keep bench fast)
         [nq-join (max 1 (quotient n-queries 10))]
         [t3 (now-ms)]
         [_ (dotimes (_ nq-join)
              (q '((find ?e ?name)
                   (where (?e user/name ?name)
                          (?e user/active #t)
                          (?e user/age ?a)
                          [(> ?a 40)])) d))]
         [ms-join (elapsed-ms t3)]
         ;; count aggregate
         [nq-agg (max 1 (quotient n-queries 10))]
         [t4 (now-ms)]
         [_ (dotimes (_ nq-agg)
              (q '((find (count ?e)) (where (?e user/age ?_a))) d))]
         [ms-agg (elapsed-ms t4)])
    (list
      (list 'exact-match  n-queries ms-exact)
      (list 'predicate    n-queries ms-pred)
      (list 'two-attr-join nq-join ms-join)
      (list 'count-agg    nq-agg    ms-agg))))

;; ---- Scenario 4: Pull throughput ----
;; Pull full entity map vs. targeted attribute selection.

(def (bench-pull conn n-queries)
  (let* ([d (db conn)]
         ;; grab one known eid by lookup
         [eid (let ([rows (q '((find ?e) (where (?e user/email "user42@example.com"))) d)])
                (and (pair? rows) (caar rows)))]
         [t1 (now-ms)]
         [_ (dotimes (_ n-queries)
              (when eid (pull d '[*] eid)))]
         [ms-wildcard (elapsed-ms t1)]
         [t2 (now-ms)]
         [_ (dotimes (_ n-queries)
              (when eid (pull d '[user/name user/age user/score] eid)))]
         [ms-targeted (elapsed-ms t2)])
    (list
      (list 'pull-wildcard  n-queries ms-wildcard)
      (list 'pull-targeted  n-queries ms-targeted))))

;; ---- Scenario 5: DB size scaling ----
;; How does a full-scan count query grow with entity count?

(def (bench-scaling sizes)
  (for/collect ([n sizes])
    (let ([conn (make-conn)])
      (bulk-load! conn n)
      (let* ([d (db conn)]
             [start (now-ms)]
             [_ (q '((find (count ?e)) (where (?e user/age ?_a))) d)]
             [ms (elapsed-ms start)])
        (close conn)
        (list n ms)))))

;; ---- Scenario 6: Mixed read/write ----
;; Simulate a realistic workload: 1 write followed by 4 reads.

(def (bench-mixed conn n-cycles)
  (let ([start (now-ms)]
        [d (db conn)])
    (let loop ([i n-cycles] [d d])
      (when (> i 0)
        ;; 1 write
        (let* ([report (transact! conn (list (user-op (+ 100000 (- n-cycles i)))))]
               [d2     (tx-report-db-after report)])
          ;; 4 reads against the updated db
          (q '((find ?e) (where (?e user/active #t) (?e user/age ?a) [(> ?a 40)])) d2)
          (q '((find (count ?e)) (where (?e user/age ?_a))) d2)
          (q '((find ?e ?name) (where (?e user/name ?name) (?e user/active #t))) d2)
          (q '((find ?e) (where (?e user/age 30))) d2)
          (loop (- i 1) d2))))
    (let ([ms (elapsed-ms start)])
      (let ([total-ops (* n-cycles 5)])   ;; 1 write + 4 reads per cycle
        (list total-ops ms)))))

;; ---- Scenario 7: Transaction log stress ----
;; Rapid-fire tiny transactions; measures transaction overhead.

(def (bench-tx-overhead n)
  (let ([conn (make-conn)]
        [start (now-ms)])
    ;; Single-datom updates to the same entity (age increments)
    (let* ([report (transact! conn (list (user-op 0)))]
           [eid    (let ([rows (q '((find ?e) (where (?e user/email "user0@example.com")))
                                  (tx-report-db-after report))])
                     (and (pair? rows) (caar rows)))])
      (dotimes (i n)
        (when eid
          (transact! conn
            (list `((db/id . ,eid)
                    (user/age . ,(+ 18 (modulo i 62)))))))))
    (let ([ms (elapsed-ms start)])
      (close conn)
      ms)))

;; ================================================================
;; Main
;; ================================================================

(displayln "")
(displayln "=== Jerboa-DB Load Test ===")
(displayln "Mode: " (if quick? "quick (1/10 scale)" "full"))
(displayln "")

;; ---- 1. Individual write throughput ----

(section "1. Write throughput — individual transactions (1 entity each)")

(let* ([n1 (S 1000)]
       [n2 (S 5000)]
       [ms1 (bench-individual-writes n1)]
       [ms2 (bench-individual-writes n2)])
  (result-row (str (number->string n1) " transactions") n1 ms1)
  (result-row (str (number->string n2) " transactions") n2 ms2))

;; ---- 2. Batch write throughput ----

(section "2. Write throughput — batch sizes (5,000 total entities each run)")

(let ([n (S 5000)])
  (for-each
    (lambda (bs)
      (let ([ms (bench-batch-writes n bs)])
        (result-row (str "batch=" (number->string bs)) n ms)))
    '(1 10 50 100 500)))

;; ---- 3. Query throughput ----

(section "3. Query throughput — DB pre-loaded with 5,000 entities")

(let* ([conn (make-conn)]
       [_    (bulk-load! conn (S 5000))]
       [nq   (S 1000)]
       [results (bench-queries conn nq)])
  (for-each
    (lambda (r)
      (result-row (symbol->string (car r)) (cadr r) (caddr r)))
    results)
  (close conn))

;; ---- 4. Pull throughput ----

(section "4. Pull throughput — single-entity retrieval (5,000 entity DB)")

(let* ([conn (make-conn)]
       [_    (bulk-load! conn (S 5000))]
       [nq   (S 2000)]
       [results (bench-pull conn nq)])
  (for-each
    (lambda (r)
      (result-row (symbol->string (car r)) (cadr r) (caddr r)))
    results)
  (close conn))

;; ---- 5. Query latency vs. DB size ----

(section "5. Scaling — full-scan count query vs. entity count")
(displayln "  " (rpad "DB size" 14) (rpad "count query latency" 24))
(displayln "  " (make-string 36 #\-))

(let ([results (bench-scaling (if quick? '(100 500 1000 2000) '(100 500 1000 5000 10000)))])
  (for-each
    (lambda (row)
      (let ([n (car row)] [ms (cadr row)])
        (latency-row (str (lpad (number->string n) 7) " entities") ms)))
    results))

;; ---- 6. Mixed read/write ----

(section "6. Mixed workload — 1 write + 4 reads per cycle")

(let* ([conn  (make-conn)]
       [_     (bulk-load! conn (S 2000))]
       [ncyc  (S 500)]
       [res   (bench-mixed conn ncyc)]
       [total (car res)]
       [ms    (cadr res)])
  (result-row (str (number->string ncyc) " cycles (5 ops each)") total ms)
  (close conn))

;; ---- 7. Transaction overhead ----

(section "7. Transaction overhead — single-datom updates to one entity")

(let* ([n   (S 2000)]
       [ms  (bench-tx-overhead n)])
  (result-row (str (number->string n) " single-attr updates") n ms))

;; ---- Summary ----

(displayln "")
(displayln "=== Done ===")
(displayln "")
