#!chezscheme
;;; Tests for (jerboa-db peer) — Round 9 Phases 46/47/49.
;;;
;;; Strategy: each test group spins up its own server on a unique port and
;;; tears it down at the end.  This isolates state between tests and
;;; minimises the impact of any transient fiber-httpd flakiness on Termux.

(import (jerboa prelude)
        (rename (only (chezscheme) make-time) (make-time chez-make-time))
        (jerboa-db core)
        (jerboa-db server)
        (jerboa-db peer))

(def test-count 0)
(def pass-count 0)
(def fail-count 0)

(defrule (test name body ...)
  (begin
    (set! test-count (+ test-count 1))
    (display "RUN:  ") (displayln name)
    (flush-output-port (current-output-port))
    (guard (exn [#t (set! fail-count (+ fail-count 1))
                    (displayln "FAIL: " name)
                    (display "  Error: ") (display-condition exn) (newline)
                    (flush-output-port (current-output-port))])
      body ...
      (set! pass-count (+ pass-count 1))
      (displayln "PASS: " name)
      (flush-output-port (current-output-port)))))

(defrule (assert-true expr)
  (unless expr (error 'assert-true "Expected true")))

(defrule (assert-equal actual expected)
  (let ([a actual] [e expected])
    (unless (equal? a e)
      (error 'assert-equal (format "Expected ~s but got ~s" e a)))))

(displayln "")
(displayln "=== Jerboa-DB Peer Client Tests (Round 9) ===")
(displayln "")

;; ---- Helpers ----

(def (make-people-db)
  (let ([conn (connect ":memory:")])
    (transact! conn
      (list `((db/ident . person/name)
              (db/valueType . db.type/string)
              (db/cardinality . db.cardinality/one))
            `((db/ident . person/age)
              (db/valueType . db.type/long)
              (db/cardinality . db.cardinality/one))))
    (let ([t1 (tempid)] [t2 (tempid)])
      (transact! conn
        (list `((db/id . ,t1) (person/name . "Alice") (person/age . 30))
              `((db/id . ,t2) (person/name . "Bob")   (person/age . 25)))))
    conn))

(def (make-orders-db)
  (let ([conn (connect ":memory:")])
    (transact! conn
      (list `((db/ident . order/sku)
              (db/valueType . db.type/string)
              (db/cardinality . db.cardinality/one))))
    (let ([t (tempid)])
      (transact! conn (list `((db/id . ,t) (order/sku . "BOOK-1")))))
    conn))

(def (warmup)
  (sleep (chez-make-time 'time-duration 200000000 0)))

;; ---- Group 1: core read/pull/entity ----

;; Single shared server across all groups — fiber-httpd on Termux is too flaky
;; to start/stop cleanly between groups.  Use a fresh default DB and named
;; "orders" DB registered up front; tests are independent.

(def default-conn (make-people-db))
(def orders-conn  (make-orders-db))
(def shared-port  38770)
(def shared-srv   (start-server (new-server-config default-conn shared-port)))
(register-db! "orders" orders-conn)
(warmup)

(def peer   (connect-remote (str "http://127.0.0.1:" shared-port)))
(def orders (connect-remote (str "http://127.0.0.1:" shared-port) "orders"))

;; ---- Group 1: core read/pull/entity ----

(test "remote-connection?"
  (assert-true (remote-connection? peer)))

(test "remote-db returns alist with basis-tx"
  (let ([s (remote-db peer)])
    (assert-true (pair? s))
    (assert-true (number? (cdr (assq 'basis-tx s))))))

(test "remote-q returns 2 names"
  (let ([rows (remote-q peer '((find ?n) (where (?e person/name ?n))))])
    (assert-equal (length rows) 2)))

(test "remote-pull on Alice"
  (let* ([rows (remote-q peer '((find ?e) (where (?e person/name "Alice"))))]
         [eid (car (car rows))]
         [r   (remote-pull peer '[*] eid)])
    (assert-equal (cdr (assq 'person/name r)) "Alice")))

(test "remote-entity on Bob (Phase 46)"
  (let* ([rows (remote-q peer '((find ?e) (where (?e person/name "Bob"))))]
         [eid  (car (car rows))]
         [ent  (remote-entity peer eid)])
    (assert-equal (cdr (assq 'person/name ent)) "Bob")))

;; ---- Group 2: cache (Phase 49) ----

(test "cache miss then hit on identical remote-q"
  (remote-cache-clear! peer)
  (let ([rows1 (remote-q peer '((find ?n) (where (?e person/name ?n))))])
    (assert-equal (length rows1) 2)
    (let ([rows2 (remote-q peer '((find ?n) (where (?e person/name ?n))))])
      (assert-equal rows1 rows2))
    (assert-equal (cdr (assq 'size (remote-cache-stats peer))) 1)))

(test "cache stats reflect distinct queries"
  (remote-cache-clear! peer)
  (remote-q peer '((find ?n) (where (?e person/name ?n))))
  (remote-q peer '((find ?a) (where (?e person/age ?a))))
  (assert-equal (cdr (assq 'size (remote-cache-stats peer))) 2))

(test "cache capacity is tunable"
  (remote-cache-set-capacity! peer 1)
  (remote-cache-clear! peer)
  (remote-q peer '((find ?n) (where (?e person/name ?n))))
  (remote-q peer '((find ?a) (where (?e person/age ?a))))
  (let ([sz (cdr (assq 'size (remote-cache-stats peer)))])
    (assert-true (<= sz 1))))

(test "remote-transact! advances last-tx"
  (remote-cache-set-capacity! peer 256)
  (let ([tx-before (cdr (assq 'last-tx (remote-cache-stats peer)))])
    (let ([t (tempid)])
      (remote-transact! peer
        (list `((db/id . ,t) (person/name . "Carol") (person/age . 40)))))
    (assert-true
      (> (cdr (assq 'last-tx (remote-cache-stats peer))) tx-before))))

;; ---- Group 3: named-DB routing (Phase 47) ----

(test "named-DB connect retains db-name"
  (assert-equal (remote-connection-db-name orders) "orders"))

(test "named-DB remote-q sees only its data"
  (let ([rows (remote-q orders '((find ?s) (where (?o order/sku ?s))))])
    (assert-equal (length rows) 1)
    (assert-equal (caar rows) "BOOK-1")))

(test "named-DB remote-transact!"
  (let ([tx-before (cdr (assq 'last-tx (remote-cache-stats orders)))]
        [t (tempid)])
    (remote-transact! orders
      (list `((db/id . ,t) (order/sku . "BOOK-2"))))
    (assert-true
      (> (cdr (assq 'last-tx (remote-cache-stats orders))) tx-before))))

(test "named DB and default DB are isolated"
  (let ([orders-rows (remote-q orders '((find ?s) (where (?o order/sku ?s))))]
        [default-rows (remote-q peer '((find ?n) (where (?e person/name ?n))))])
    (assert-equal (length orders-rows) 2)    ;; BOOK-1 + BOOK-2
    (assert-equal (length default-rows) 3))) ;; Alice + Bob + Carol (added above)

(stop-server shared-srv)

;; ---- Summary ----

(displayln "")
(displayln (str "Results: " pass-count "/" test-count " passed, "
                fail-count " failed"))
;; Force exit so the script terminates promptly even if a fiber-httpd
;; accept loop is still parked on a closed socket.
(exit (if (> fail-count 0) 1 0))
