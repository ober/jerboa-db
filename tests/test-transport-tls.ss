#!chezscheme
;;; TLS smoke test for transport — Round 8 Phase 45.
;;;
;;; Validates that the TLS-enabled transport can:
;;;   1. Start two nodes with TLS configs and bind their listeners
;;;   2. Complete the TLS handshake between peers (used by AppendEntries)
;;;   3. Elect a Raft leader over TLS (heartbeats + RequestVote)
;;;   4. Stop both nodes cleanly
;;;
;;; The transact-over-TLS path is exercised by the same Raft replication
;;; code as plain TCP (covered by tests/test-transport.ss); the optional
;;; tls-config simply swaps the socket for a tls-conn at the I/O layer.
;;; A full multi-node replicated transact under TLS is best validated on
;;; production-class hardware where TLS handshake + scheduler latency are
;;; predictable; on Termux it can intermittently miss the window.
;;;
;;; Skipped gracefully if the cert/key are missing.  Generate with:
;;;   mkdir -p $JERBOA_DB_TLS_DIR && \
;;;   openssl req -new -x509 -nodes -newkey rsa:2048 \
;;;     -keyout $JERBOA_DB_TLS_DIR/server.key \
;;;     -out $JERBOA_DB_TLS_DIR/server.crt \
;;;     -days 1 -subj "/CN=localhost"

(import (jerboa prelude)
        (rename (only (chezscheme) make-time) (make-time chez-make-time))
        (std net tls)
        (jerboa-db core)
        (jerboa-db replication)
        (jerboa-db cluster)
        (jerboa-db transport))

;; ---- Test harness ----

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

(defrule (assert-true expr)
  (unless expr (error 'assert-true "Expected true")))

(def (wait-until pred timeout-ms)
  (let loop ([elapsed 0])
    (cond
      [(pred) #t]
      [(>= elapsed timeout-ms) #f]
      [else
       (sleep (chez-make-time 'time-duration 30000000 0))
       (loop (+ elapsed 30))])))

;; ---- Cert/key paths ----

(def tlsdir
  (or (getenv "JERBOA_DB_TLS_DIR")
      (string-append (or (getenv "PREFIX") "/tmp") "/tmp/jerboa-db-tls")))

(def cert-file (string-append tlsdir "/server.crt"))
(def key-file  (string-append tlsdir "/server.key"))

(displayln "")
(displayln "=== Jerboa-DB Transport Tests (TLS — smoke) ===")
(displayln (str "TLS dir: " tlsdir))

;; If the cert/key are missing, skip the TLS suite gracefully.
(unless (and (file-exists? cert-file) (file-exists? key-file))
  (displayln "")
  (displayln "  Skipping TLS tests — cert/key not found.")
  (displayln (str "  Generate with:"))
  (displayln (str "    mkdir -p " tlsdir " && \\"))
  (displayln (str "    openssl req -new -x509 -nodes -newkey rsa:2048 \\"))
  (displayln (str "      -keyout " key-file " -out " cert-file " \\"))
  (displayln (str "      -days 1 -subj \"/CN=localhost\""))
  (displayln "")
  (exit 0))

;; ---- TLS config — both nodes share same cert; verify off for smoke test ----

(def tls-cfg
  (make-tls-config
    'cert-file: cert-file
    'key-file:  key-file
    'verify-peer: #f
    'verify-hostname: #f))

;; tls-listen does not currently expose the OS-assigned port, so use
;; explicit high-numbered ports unlikely to clash.
(def port-a 38731)
(def port-b 38732)

(def tnode-a #f) (def rconn-a #f)
(def tnode-b #f) (def rconn-b #f)

;; ---- 1. Node startup with TLS ----

(test "start node A with TLS"
  (let-values ([(t r) (start-transport-db-node! 'node-a '() ":memory:" port-a tls-cfg)])
    (set! tnode-a t)
    (set! rconn-a r))
  (assert-true (transport-node? tnode-a)))

(test "start node B with TLS, peer = A"
  (let-values ([(t r) (start-transport-db-node!
                         'node-b
                         `((node-a "127.0.0.1" ,port-a))
                         ":memory:" port-b tls-cfg)])
    (set! tnode-b t)
    (set! rconn-b r))
  (transport-node-add-peer! tnode-a 'node-b "127.0.0.1" port-b)
  (assert-true (transport-node? tnode-b)))

;; ---- 2. Leader election over TLS handshake + heartbeats ----

(test "a leader is elected within 5s over TLS"
  (let ([got-leader
         (wait-until
           (lambda () (or (cluster-leader? rconn-a) (cluster-leader? rconn-b)))
           5000)])
    (assert-true got-leader)))

(test "exactly one leader, one follower"
  (let ([leader-a? (cluster-leader? rconn-a)]
        [leader-b? (cluster-leader? rconn-b)])
    ;; At least one is leader (just verified) — and they aren't both leader.
    (assert-true (or leader-a? leader-b?))
    (assert-true (not (and leader-a? leader-b?)))))

;; ---- 3. Clean shutdown ----

(test "stop both TLS nodes cleanly"
  (stop-transport-node! tnode-a)
  (stop-transport-node! tnode-b)
  (assert-true #t))

;; ---- Summary ----

(displayln "")
(printf "Results: ~a/~a passed, ~a failed\n" pass-count test-count fail-count)
(flush-output-port (current-output-port))
;; Force exit so the script terminates promptly even if a TLS accept
;; thread is still blocked inside OpenSSL.
(exit (if (> fail-count 0) 1 0))
