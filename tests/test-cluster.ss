(import (jerboa prelude)
        ;; Capture Chez's SRFI-19 make-time before prelude shadows it with datetime's version.
        ;; (sleep) requires a time-duration record created by Chez's native make-time.
        (rename (only (chezscheme) make-time) (make-time chez-make-time))
        (jerboa-db core)
        (jerboa-db replication)
        (jerboa-db cluster))

;; ---- Test harness ----

(def test-count 0)
(def pass-count 0)
(def fail-count 0)

(defrule (test name body ...)
  (begin
    (set! test-count (+ test-count 1))
    (guard (exn [#t (set! fail-count (+ fail-count 1))
                    (displayln "FAIL: " name)
                    (displayln "  Error: " (if (message-condition? exn)
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

;; ---- Helpers ----

;; Wait up to timeout-ms for (pred) to return truthy.
(def (wait-until pred timeout-ms)
  (let loop ([elapsed 0])
    (cond
      [(pred) #t]
      [(>= elapsed timeout-ms) #f]
      [else
       (sleep (chez-make-time 'time-duration 20000000 0)) ;; 20ms
       (loop (+ elapsed 20))])))

;; ============================================================
(displayln "")
(displayln "=== Jerboa-DB Cluster Tests ===")
;; ============================================================

(test "start-local-db-cluster creates N nodes"
  (def cfg (new-replication-config 'test-node #f ":memory:"))
  (def nodes (start-local-db-cluster 3 cfg))
  (assert-equal (length nodes) 3)
  (for-each cluster-stop! nodes))

(test "leader elected within 1000ms in 3-node cluster"
  (def cfg (new-replication-config 'test-node #f ":memory:"))
  (def nodes (start-local-db-cluster 3 cfg))
  ;; Raft election timeout: 150–300ms per attempt.  Allow 1000ms so
  ;; background threads from the previous test don't cause a flake.
  (def leader-found?
    (wait-until (lambda () (any cluster-leader? nodes)) 1000))
  (assert-true leader-found?)
  (for-each cluster-stop! nodes))

(test "cluster-status returns raft fields"
  (def cfg (new-replication-config 'test-node #f ":memory:"))
  (def nodes (start-local-db-cluster 1 cfg))  ;; single-node, always leader
  ;; Wait for single-node leader election (always wins immediately).
  ;; Use let after wait-until (expression) so no def follows an expression.
  (wait-until (lambda () (cluster-leader? (car nodes))) 1000)
  (let ([status (cluster-status (car nodes))])
    (assert-true (assq 'role status))
    (assert-true (assq 'term status))
    (assert-true (assq 'basis-tx status)))
  (for-each cluster-stop! nodes))

(test "cluster-transact! on single-node cluster applies transaction"
  ;; Single-node Raft cluster: elects itself leader immediately.
  (def cfg (new-replication-config 'solo #f ":memory:"))
  (def nodes (start-local-db-cluster 1 cfg))
  (def node (car nodes))
  ;; Wait for self-election (~150–300 ms)
  (def elected? (wait-until (lambda () (cluster-leader? node)) 1000))
  (assert-true elected?)
  ;; Define schema via cluster-transact!
  (cluster-transact! node
    (list
      `((db/ident . person/name)
        (db/valueType . db.type/string)
        (db/cardinality . db.cardinality/one))
      `((db/ident . person/age)
        (db/valueType . db.type/long)
        (db/cardinality . db.cardinality/one))))
  ;; Insert data
  (cluster-transact! node
    (list `((person/name . "Alice") (person/age . 30))))
  ;; Query via cluster-db (use let — def after expressions is invalid)
  (let ([result
         (q '((find ?name ?age)
              (where (?e person/name ?name)
                     (?e person/age  ?age)))
            (cluster-db node))])
    (assert-equal result '(("Alice" 30))))
  (cluster-stop! node))

(test "follower converges to leader state in 3-node cluster"
  (def cfg (new-replication-config 'node-x #f ":memory:"))
  (def nodes (start-local-db-cluster 3 cfg))
  ;; Wait for a leader.  Hoist both defs before the first expression so
  ;; no definition appears after an expression (Chez restriction).
  (def leader-found? (wait-until (lambda () (any cluster-leader? nodes)) 1000))
  (def leader (find cluster-leader? nodes))
  (assert-true leader-found?)
  ;; Transact schema + one entity on the leader
  (cluster-transact! leader
    (list
      `((db/ident . thing/val)
        (db/valueType . db.type/long)
        (db/cardinality . db.cardinality/one))))
  (cluster-transact! leader
    (list `((thing/val . 42))))
  ;; Give apply fibers time to replicate to followers (~150 ms)
  (sleep (chez-make-time 'time-duration 200000000 0))
  ;; Every node should now see thing/val = 42 (use let — def after sleep expression)
  (let ([followers (filter (lambda (n) (not (cluster-leader? n))) nodes)])
    (for-each
      (lambda (follower)
        (let ([res (q '((find ?v)
                        (where (?e thing/val ?v)))
                      (cluster-db follower))])
          (assert-equal (length res) 1)
          (assert-equal (caar res) 42)))
      followers))
  (for-each cluster-stop! nodes))

(test "replicated-connect wraps a connection"
  (def cfg (new-replication-config 'solo #f ":memory:"))
  (def rconn (replicated-connect ":memory:" cfg))
  (assert-true (replicated-conn? rconn))
  (cluster-stop! rconn))

;; ============================================================
(displayln "")
(displayln "=== Results ===")
(displayln "Total: " test-count " | Passed: " pass-count " | Failed: " fail-count)
(when (> fail-count 0)
  (exit 1))
