#!chezscheme
;;; (jerboa-db cluster) — Replicated connection: Raft + core.ss integration
;;;
;;; This module bridges (jerboa-db core) and (jerboa-db replication) without
;;; introducing a circular import.  It can be imported by application code
;;; that wants a multi-node Jerboa-DB cluster.
;;;
;;; Architecture:
;;;   - Each cluster node wraps one `connection` (from core.ss) and one
;;;     `replication-state` (from replication.ss).
;;;   - A background apply fiber polls committed Raft entries every 50 ms and
;;;     applies them via `transact!` (full semantics: schema, fulltext, stats).
;;;   - Writes must go through `cluster-transact!`, which proposes to Raft and
;;;     blocks until the entry is applied locally (with a configurable timeout).
;;;   - Reads use `cluster-db` and may optionally enforce a consistency level.
;;;
;;; Network transport:
;;;   (std raft) uses in-process channels.  A local cluster (all nodes in one
;;;   OS process) works out of the box via `start-local-cluster`.  To span
;;;   processes or machines a transport adapter must bridge each node's inbox
;;;   channel to a TCP socket — that is Phase 6.3 (not yet implemented).

(library (jerboa-db cluster)
  (export
    ;; Replicated-connection type
    make-replicated-conn  replicated-conn?
    replicated-conn-conn           ;; underlying connection
    replicated-conn-state          ;; underlying replication-state

    ;; Lifecycle
    replicated-connect             ;; create + start a cluster node
    cluster-stop!                  ;; stop Raft + apply fiber

    ;; Write / read
    cluster-transact!              ;; propose → wait → return tx-report
    cluster-db                     ;; current db-value (read-committed)

    ;; Introspection
    cluster-status                 ;; alist of Raft status fields
    cluster-leader?

    ;; Convenience: start a fully-wired local (in-process) cluster
    start-local-db-cluster)

  (import (except (chezscheme)
                  make-hash-table hash-table?
                  sort sort!
                  printf fprintf
                  path-extension path-absolute?
                  with-input-from-string with-output-to-string
                  iota 1+ 1-
                  partition
                  make-date make-time
                  log               ;; avoid conflict with (jerboa-db core)'s log
                atom? meta)
          ;; Capture Chez's native make-time as chez-make-time before the prelude
          ;; shadows it with (std datetime)'s make-time.  We need the SRFI-19
          ;; time-duration record for (sleep ...) calls.
          (rename (only (chezscheme) make-time) (make-time chez-make-time))
          (jerboa prelude)
          (jerboa-db history)
          (jerboa-db core)
          (jerboa-db replication)
          (std raft))

  ;; =========================================================================
  ;; Replicated connection record
  ;; =========================================================================

  (defstruct replicated-conn
    (conn           ;; (jerboa-db core) connection
     state          ;; replication-state from (jerboa-db replication)
     apply-fiber))  ;; background thread applying committed entries

  ;; =========================================================================
  ;; Apply fiber
  ;; =========================================================================

  ;; cluster-effective-commit-index : raft-cluster -> integer
  ;;
  ;; Returns the index up to which log entries are safe to apply.
  ;; For N=1, (std raft) never calls try-advance-commit-index! because
  ;; that function is only triggered by AppendEntriesResponse — and a
  ;; single-node cluster has no peers to send responses.  We work around
  ;; this by treating the last proposed entry as committed: the single node
  ;; IS the quorum, so any proposed entry is trivially a majority.
  (def (cluster-effective-commit-index cluster)
    (let* ([leader (raft-cluster-leader cluster)]
           [n      (length (raft-cluster-nodes cluster))])
      (if leader
          (if (= n 1)
              ;; Single-node: highest log index = effective commit index.
              (let ([log (raft-log leader)])
                (if (null? log)
                    0
                    (or (log-entry-field (car (reverse log)) 0) 0)))
              (raft-commit-index leader))
          0)))

  ;; apply-committed-from-cluster! : conn replication-state raft-cluster -> void
  ;;
  ;; Applies newly committed Raft log entries to `conn` by reading the
  ;; CLUSTER LEADER's complete log.  All nodes share this apply path —
  ;; leader and followers alike — to avoid a bug in (std raft) where
  ;; each AppendEntries RPC can overwrite earlier entries from the
  ;; follower's local log view, causing followers to miss earlier commits.
  (def (apply-committed-from-cluster! conn state cluster)
    (let ([leader (raft-cluster-leader cluster)])
      (when leader
        (let ([commit-idx   (cluster-effective-commit-index cluster)]
              [last-applied (replication-last-applied-index state)])
          (for-each
            (lambda (entry)
              (let ([entry-index   (log-entry-field entry 0)]
                    [entry-command (log-entry-field entry 2)])
                (when (and (integer? entry-index)
                           (> entry-index last-applied)
                           (<= entry-index commit-idx))
                  ;; Advance watermark before transact! — at-most-once semantics.
                  (replication-set-last-applied-index! state entry-index)
                  (when (and (pair? entry-command)
                             (eq? (car entry-command) 'tx))
                    (guard (exn [#t
                                 (display (format "cluster apply: skip failed tx at index ~a: ~a\n"
                                                  entry-index
                                                  (if (message-condition? exn)
                                                      (condition-message exn)
                                                      "unknown error")))])
                      (transact! conn (cdr entry-command)))))))
            (raft-log leader))))))

  ;; start-cluster-apply-fiber! : conn replication-state raft-cluster -> thread
  ;;
  ;; Background fiber for cluster-mode nodes.  Reads the leader's log so
  ;; every node — leader and followers — sees a consistent, complete entry list.
  (def (start-cluster-apply-fiber! conn state cluster)
    (fork-thread
      (lambda ()
        (let loop ()
          ;; Sleep first so initial leader election can complete.
          (sleep (chez-make-time 'time-duration 50000000 0)) ;; 50 ms
          (when (replication-running? state)
            (apply-committed-from-cluster! conn state cluster)
            (loop))))))

  ;; start-apply-fiber! : conn replication-state -> thread
  ;;
  ;; Standalone (non-cluster) apply fiber used by replicated-connect.
  ;; Falls back to the per-node log via replication-for-each-committed!.
  (def (start-apply-fiber! conn state)
    (fork-thread
      (lambda ()
        (let loop ()
          (sleep (chez-make-time 'time-duration 50000000 0)) ;; 50 ms
          (when (replication-running? state)
            (replication-for-each-committed! state
              (lambda (entry-index tx-ops)
                (guard (exn [#t
                             (display (format "cluster apply: skip failed tx at index ~a: ~a\n"
                                              entry-index
                                              (if (message-condition? exn)
                                                  (condition-message exn)
                                                  "unknown error")))])
                  (transact! conn tx-ops))))
            (loop))))))

  ;; =========================================================================
  ;; Wait for a Raft log entry to be applied locally.
  ;; Used by cluster-transact! to give the caller a synchronous guarantee.
  ;; =========================================================================

  (def (wait-for-applied! state target-index timeout-ms)
    (let loop ([elapsed 0])
      (cond
        [(>= (replication-last-applied-index state) target-index)
         'ok]
        [(>= elapsed timeout-ms)
         (error 'cluster-transact!
                "timeout waiting for Raft consensus"
                `(target-index ,target-index
                  last-applied  ,(replication-last-applied-index state)
                  timeout-ms    ,timeout-ms))]
        [else
         (sleep (chez-make-time 'time-duration 10000000 0)) ;; 10 ms poll
         (loop (+ elapsed 10))])))

  ;; =========================================================================
  ;; Lifecycle
  ;; =========================================================================

  ;; replicated-connect : string replication-config -> replicated-conn
  ;;
  ;; Creates a regular connection at `path`, starts a Raft node for
  ;; `repl-config`, and launches the apply fiber.  Returns a
  ;; replicated-conn that callers use instead of the raw connection.
  (def (replicated-connect path repl-config)
    (let* ([conn  (connect path)]
           [state (start-replication repl-config)]
           [fiber (start-apply-fiber! conn state)])
      (make-replicated-conn conn state fiber)))

  ;; cluster-stop! : replicated-conn -> void
  ;;
  ;; Stops the Raft node and closes the underlying connection.
  ;; The apply fiber exits on its next 50 ms poll.
  (def (cluster-stop! rconn)
    (stop-replication (replicated-conn-state rconn))
    (close (replicated-conn-conn rconn)))

  ;; =========================================================================
  ;; Write path
  ;; =========================================================================

  ;; cluster-transact! : replicated-conn list [integer] -> tx-report
  ;;
  ;; Proposes tx-ops to the Raft log, waits up to `timeout-ms` (default 5000)
  ;; for the entry to be applied to the local connection, then returns the
  ;; tx-report from the local connection's tx-log.
  ;;
  ;; Raises an error if this node is not the Raft leader.  Callers must
  ;; redirect writes to the leader; use (cluster-leader? rconn) to check.
  (def (cluster-transact! rconn tx-ops . opts)
    (let ([timeout-ms (if (pair? opts) (car opts) 5000)]
          [state (replicated-conn-state rconn)])
      (unless (cluster-leader? rconn)
        (error 'cluster-transact!
               "write rejected: this node is not the Raft leader"))
      ;; Propose to Raft (synchronous proposal, async apply).
      (let-values ([(status log-index) (replicated-transact! state tx-ops)])
        ;; Wait for the apply fiber to apply this exact entry.
        (wait-for-applied! state log-index timeout-ms)
        ;; The most-recently applied tx-report is at the head of the tx-log.
        (car (connection-tx-log (replicated-conn-conn rconn))))))

  ;; =========================================================================
  ;; Read path
  ;; =========================================================================

  ;; cluster-db : replicated-conn -> db-value
  ;;
  ;; Returns the current db-value from the local connection (read-committed).
  ;; May lag the leader by at most one heartbeat interval (~50 ms).
  ;; Use (cluster-db/consistent rconn) for leader-only reads.
  (def (cluster-db rconn)
    (db (replicated-conn-conn rconn)))

  ;; cluster-db/consistent : replicated-conn -> db-value
  ;;
  ;; Returns the local db-value but only if this node is the leader.
  ;; Guarantees the db reflects all committed transactions.
  (def (cluster-db/consistent rconn)
    (unless (cluster-leader? rconn)
      (error 'cluster-db/consistent
             "consistent reads require the Raft leader"))
    (db (replicated-conn-conn rconn)))

  ;; =========================================================================
  ;; Introspection
  ;; =========================================================================

  ;; cluster-status : replicated-conn -> alist
  ;;
  ;; Returns a combined status alist: Raft fields + db basis-tx + applied index.
  (def (cluster-status rconn)
    (let* ([state   (replicated-conn-state rconn)]
           [conn    (replicated-conn-conn rconn)]
           [current (db conn)]
           [raft    (replication-status state)])
      (append raft
              `((basis-tx      . ,(db-value-basis-tx current))
                (last-applied  . ,(replication-last-applied-index state))))))

  ;; cluster-leader? : replicated-conn -> boolean
  (def (cluster-leader? rconn)
    (replication-leader? (replicated-conn-state rconn)))

  ;; =========================================================================
  ;; Local cluster convenience
  ;; =========================================================================

  ;; start-local-db-cluster : integer replication-config -> (list-of replicated-conn)
  ;;
  ;; Creates a fully-wired in-process Raft cluster of `node-count` nodes,
  ;; each backed by an in-memory connection.  Returns a list of replicated-conn
  ;; records.  Useful for testing and local development.
  ;;
  ;; Each node gets its own `:memory:` connection.  When a transaction is
  ;; proposed on the leader, the apply fiber replicates it to all followers
  ;; so every node converges to the same state.
  ;;
  ;; Usage:
  ;;   (def cfg (new-replication-config 'node-0 #f ":memory:"))
  ;;   (def nodes (start-local-db-cluster 3 cfg))
  ;;   ;; Wait for leader election (~150–300 ms)
  ;;   (sleep (make-time 'time-duration 500000000 0))
  ;;   (def leader (find cluster-leader? nodes))
  ;;   (cluster-transact! leader my-schema-tx)
  (def (start-local-db-cluster node-count base-config)
    ;; start-local-cluster from replication.ss creates a wired Raft cluster
    ;; and returns (values list-of-replication-states raft-cluster).
    (let-values ([(states cluster) (start-local-cluster node-count base-config)])
      ;; Pair each replication-state with its own fresh connection and a
      ;; cluster-aware apply fiber that reads the leader's complete log.
      (map (lambda (state)
             (let* ([conn  (connect ":memory:")]
                    [fiber (start-cluster-apply-fiber! conn state cluster)])
               (make-replicated-conn conn state fiber)))
           states)))

) ;; end library
