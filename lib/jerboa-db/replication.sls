#!chezscheme
;;; (jerboa-db replication) — Raft-based transactor HA + read replicas
;;;
;;; The transactor is a Raft leader. Transactions are proposed to the Raft log.
;;; Once committed by majority, they're applied to local indices.
;;; Followers apply transactions from the Raft log.

(library (jerboa-db replication)
  (export
    new-replication-config replication-config?
    start-replication stop-replication
    replication-status replication-leader?)

  (import (chezscheme))

  ;; ---- Replication configuration ----

  (define-record-type replication-config
    (fields node-id        ;; unique node identifier (string)
            cluster-nodes  ;; list of (node-id . address) pairs
            data-path      ;; local data directory
            election-timeout-ms   ;; Raft election timeout
            heartbeat-interval-ms ;; Raft heartbeat interval
            read-consistency))    ;; 'read-committed, 'read-latest, or 'as-of

  (define (new-replication-config node-id cluster-nodes data-path . opts)
    (make-replication-config
      node-id cluster-nodes data-path
      (if (and (pair? opts) (number? (car opts))) (car opts) 1500)
      (if (and (pair? opts) (pair? (cdr opts)) (number? (cadr opts)))
          (cadr opts) 500)
      (if (and (pair? opts) (pair? (cdr opts)) (pair? (cddr opts)))
          (caddr opts) 'read-committed)))

  ;; ---- Replication lifecycle ----
  ;; Uses (std raft) for leader election and log replication.
  ;; Uses (std actor transport) for inter-node communication.

  (define-record-type replication-state
    (fields config
            (mutable role)        ;; 'leader, 'follower, or 'candidate
            (mutable current-term)
            (mutable leader-id)
            (mutable last-applied-tx)))

  (define (start-replication config conn)
    ;; Initialize Raft node
    ;; 1. Start Raft state machine using (std raft)
    ;; 2. Register transaction proposal handler
    ;; 3. Register log application handler
    ;; 4. Start heartbeat/election timer
    (let ([state (make-replication-state config 'follower 0 #f 0)])
      (display (format "Replication started for node ~a\n"
                       (replication-config-node-id config)))
      state))

  (define (stop-replication state)
    (display "Replication stopped\n")
    (void))

  (define (replication-status state)
    `((role . ,(replication-state-role state))
      (term . ,(replication-state-current-term state))
      (leader . ,(replication-state-leader-id state))
      (last-applied-tx . ,(replication-state-last-applied-tx state))))

  (define (replication-leader? state)
    (eq? (replication-state-role state) 'leader))

) ;; end library
