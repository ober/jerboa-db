(import (jerboa prelude)
        (rename (only (chezscheme) make-time) (make-time chez-make-time))
        (jerboa-db core)
        (jerboa-db replication)
        (jerboa-db cluster)
        (jerboa-db transport))

;; ---- Test harness (same as test-cluster.ss) ----

(def test-count 0)
(def pass-count 0)
(def fail-count 0)

(defrule (test name body ...)
  (begin
    (set! test-count (+ test-count 1))
    (guard (exn [#t (set! fail-count (+ fail-count 1))
                    (displayln "FAIL: " name)
                    (displayln "  Error: "
                               (if (message-condition? exn)
                                   (condition-message exn)
                                   exn))])
      body ...
      (set! pass-count (+ pass-count 1))
      (displayln "PASS: " name))))

(defrule (assert-equal actual expected)
  (let ([a actual] [e expected])
    (unless (equal? a e)
      (error 'assert-equal (format "Expected ~s but got ~s" e a)))))

(defrule (assert-true expr)
  (unless expr (error 'assert-true "Expected true")))

(defrule (assert-false expr)
  (when expr (error 'assert-false "Expected false")))

;; Wait up to timeout-ms for (pred) to return truthy, polling every 30 ms.
(def (wait-until pred timeout-ms)
  (let loop ([elapsed 0])
    (cond
      [(pred) #t]
      [(>= elapsed timeout-ms) #f]
      [else
       (sleep (chez-make-time 'time-duration 30000000 0))
       (loop (+ elapsed 30))])))

;; ============================================================
(displayln "")
(displayln "=== Jerboa-DB Transport Tests (TCP) ===")
;; ============================================================

;; Global state for the multi-test cluster.
;; Nodes are started once and shared across tests; stopped at the end.
(def tnode-a #f) (def rconn-a #f) (def port-a #f)
(def tnode-b #f) (def rconn-b #f) (def port-b #f)

;; ---- 1. Node startup ----

(test "start node A (no initial peers, OS-assigned port)"
  (let-values ([(t r) (start-transport-db-node! 'node-a '() ":memory:" 0)])
    (set! tnode-a t)
    (set! rconn-a r)
    (set! port-a (transport-node-listen-port t)))
  (assert-true (transport-node? tnode-a))
  (assert-true (> port-a 0)))

(test "start node B (peer = A), then add B as peer to A"
  (let-values ([(t r) (start-transport-db-node!
                         'node-b
                         `((node-a "127.0.0.1" ,port-a))
                         ":memory:" 0)])
    (set! tnode-b t)
    (set! rconn-b r)
    (set! port-b (transport-node-listen-port t)))
  ;; Now that we know B's port, tell A about B
  (transport-node-add-peer! tnode-a 'node-b "127.0.0.1" port-b)
  (assert-true (transport-node? tnode-b))
  (assert-true (> port-b 0)))

;; ---- 2. Leader election ----

(test "a leader is elected within 1500ms"
  ;; Give time for TCP connections to establish + Raft election (~150–300 ms)
  (let ([got-leader
         (wait-until
           (lambda () (or (cluster-leader? rconn-a) (cluster-leader? rconn-b)))
           1500)])
    (assert-true got-leader)))

;; ---- 3. Identify leader/follower ----

(def leader   #f)
(def follower #f)

(test "identify leader and follower"
  (set! leader   (if (cluster-leader? rconn-a) rconn-a rconn-b))
  (set! follower (if (cluster-leader? rconn-a) rconn-b rconn-a))
  (assert-true  (cluster-leader? leader))
  (assert-false (cluster-leader? follower)))

;; ---- 4. Transact through leader ----

(test "schema transact through leader"
  (cluster-transact! leader
    (list
      '((db/ident . person/name)
        (db/valueType . db.type/string)
        (db/cardinality . db.cardinality/one))
      '((db/ident . person/age)
        (db/valueType . db.type/long)
        (db/cardinality . db.cardinality/one))))
  (assert-true #t))  ;; no exception = pass

(test "data transact through leader"
  (cluster-transact! leader
    (list
      '((person/name . "Alice") (person/age . 30))
      '((person/name . "Bob")   (person/age . 25))))
  (assert-true #t))

;; ---- 5. Query from leader ----

(test "query from leader returns data"
  (let ([results
         (q '[(find ?n ?a)
              (where (?e person/name ?n)
                     (?e person/age  ?a))]
            (cluster-db leader))])
    (assert-equal (length results) 2)
    (assert-true (member '("Alice" 30) results))
    (assert-true (member '("Bob"   25) results))))

;; ---- 6. Replication to follower ----

(test "follower applies transactions within 500ms"
  ;; The apply fiber polls every 50 ms; committed entries should appear quickly.
  (let ([replicated
         (wait-until
           (lambda ()
             (let ([db (cluster-db follower)])
               (not (null? (q '[(find ?n) (where (?e person/name ?n))]
                               db)))))
           500)])
    (assert-true replicated)))

(test "query from follower returns same data as leader"
  (let ([results
         (q '[(find ?n ?a)
              (where (?e person/name ?n)
                     (?e person/age  ?a))]
            (cluster-db follower))])
    (assert-equal (length results) 2)
    (assert-true (member '("Alice" 30) results))
    (assert-true (member '("Bob"   25) results))))

;; ---- 7. Additional transact verifies continued replication ----

(test "second transact replicates to follower"
  (cluster-transact! leader
    (list '((person/name . "Carol") (person/age . 35))))
  (let ([replicated
         (wait-until
           (lambda ()
             (not (null? (q '[(find ?n) (where (?e person/name "Carol")
                                               (?e person/name ?n))]
                             (cluster-db follower)))))
           500)])
    (assert-true replicated)))

;; ---- 8. Cluster status ----

(test "cluster-status includes Raft fields and basis-tx"
  (let ([status (cluster-status leader)])
    (assert-true (assoc 'role         status))
    (assert-true (assoc 'term         status))
    (assert-true (assoc 'basis-tx     status))
    (assert-true (assoc 'last-applied status))))

;; ---- 9. Stop both nodes ----

(test "stop both nodes cleanly"
  (stop-transport-node! tnode-a)
  (stop-transport-node! tnode-b)
  (assert-true #t))

;; ---- Summary ----

(displayln "")
(displayln (str "Results: " pass-count "/" test-count " passed, "
                fail-count " failed"))
(when (> fail-count 0) (exit 1))
