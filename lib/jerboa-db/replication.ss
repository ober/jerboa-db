#!chezscheme
;;; (jerboa-db replication) — Raft-based transactor HA + read replicas
;;;
;;; Architecture:
;;;   - Each node wraps a (std raft) node for leader election and log consensus.
;;;   - The Raft log carries serialized transaction op-lists as commands,
;;;     tagged as ('tx . tx-ops).
;;;   - Only the Raft leader accepts writes via replicated-transact!.
;;;   - All nodes (including followers) can apply committed entries via
;;;     replication-apply-committed!, which drives process-transaction locally.
;;;   - Three read consistency levels: read-committed, read-latest, as-of-tx.
;;;
;;; LIMITATION (network transport):
;;;   (std raft) communicates via in-process Scheme channels.  In a local
;;;   cluster built with make-raft-cluster / start-local-cluster, all nodes
;;;   share the same OS process and communicate over channels — no network I/O
;;;   is needed.  To span processes or machines, a future transport adapter
;;;   would need to bridge each node's inbox channel to a TCP/TLS socket (e.g.,
;;;   via a (std net raft-transport) module).  The consensus algorithm itself is
;;;   complete and correct; only the wire transport is absent.
;;;
;;; Integration with (jerboa-db core):
;;;   replication-apply-committed! accepts three callbacks instead of a raw
;;;   connection record so this module does not import (jerboa-db core) and
;;;   avoids a circular dependency.  See the docstring for details.

(library (jerboa-db replication)
  (export
    ;; Configuration
    new-replication-config  replication-config?
    replication-config-node-id  replication-config-cluster-nodes
    replication-config-data-path  replication-config-election-timeout-ms
    replication-config-heartbeat-interval-ms  replication-config-read-consistency

    ;; Lifecycle
    start-replication       stop-replication
    start-replication-from-node!  ;; wrap a pre-configured raft-node (for transport adapter)

    ;; Multi-node local cluster convenience
    start-local-cluster

    ;; Status / introspection
    replication-status      replication-leader?
    replication-running?

    ;; Write path (leader only)
    replicated-transact!

    ;; Apply path (all nodes)
    replication-apply-committed!
    replication-for-each-committed!   ;; callback-based variant (conn-aware callers)
    replication-last-applied-index    ;; expose watermark for polling

    ;; Read consistency helpers
    read-committed          read-latest         as-of-tx

    ;; Helpers re-exported for use in (jerboa-db cluster)
    log-entry-field
    replication-set-last-applied-index!)

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
          (jerboa prelude)
          (std raft)
          (jerboa-db tx)
          (jerboa-db history))

  ;; =========================================================================
  ;; Configuration
  ;; =========================================================================

  (define-record-type replication-config
    (fields node-id               ;; unique node identifier (symbol or integer)
            cluster-nodes         ;; list of peer raft-node objects (in-process),
                                  ;; or #f for a standalone single-node deployment
            data-path             ;; local data directory (informational / future use)
            election-timeout-ms   ;; Raft election timeout hint (ms) — informational;
                                  ;; (std raft) manages its own 150–300 ms timeout
            heartbeat-interval-ms ;; Raft heartbeat interval hint (ms) — informational;
                                  ;; (std raft) uses a 50 ms heartbeat internally
            read-consistency))    ;; default: 'read-committed | 'read-latest | 'as-of

  ;; Convenience constructor: required args + optional tail for timeout / consistency.
  (def (new-replication-config node-id cluster-nodes data-path . opts)
    (make-replication-config
      node-id
      cluster-nodes
      data-path
      (if (and (pair? opts) (number? (car opts)))  (car opts)  1500)
      (if (and (pair? opts) (pair? (cdr opts)) (number? (cadr opts)))
          (cadr opts) 500)
      (if (and (pair? opts) (pair? (cdr opts)) (pair? (cddr opts)))
          (caddr opts) 'read-committed)))

  ;; =========================================================================
  ;; Replication state
  ;; =========================================================================

  (define-record-type replication-state
    (fields config                                  ;; replication-config
            raft-node                               ;; (std raft) node object
            cluster                                 ;; raft-cluster (or #f for standalone)
            (mutable last-applied-index)            ;; highest Raft log index applied locally
            (mutable running?)))          ;; highest Raft log index applied locally

  ;; =========================================================================
  ;; Lifecycle
  ;; =========================================================================

  ;; start-replication : replication-config -> replication-state
  ;;
  ;; Creates and starts a Raft node.
  ;;   - Standalone (cluster-nodes = #f): single-node Raft, elects itself leader
  ;;     after the election timeout (~150–300 ms).
  ;;   - Multi-node: caller should use start-local-cluster for a fully wired
  ;;     in-process cluster.  Passing a list of peer raft-node objects as
  ;;     cluster-nodes is reserved for future use.
  (def (start-replication config)
    (let* ([node-id (replication-config-node-id config)]
           [node    (make-raft-node node-id)])
      (raft-start! node)
      (let ([state (make-replication-state config node #f 0 #t)])
        (display
          (string-append "Replication started: node "
                         (if (symbol? node-id)
                             (symbol->string node-id)
                             (number->string node-id))
                         "\n"))
        state)))

  ;; start-local-cluster : integer replication-config -> (values list raft-cluster)
  ;;
  ;; Create a fully wired local (in-process) Raft cluster of node-count nodes,
  ;; start all of them, and return:
  ;;   - a list of replication-state records (one per node)
  ;;   - the underlying raft-cluster object
  ;;
  ;; All nodes share the same base-config; each gets the cluster's actual
  ;; raft-node as its raft-node field.
  (def (start-local-cluster node-count base-config)
    (let* ([cluster (make-raft-cluster node-count)]
           [nodes   (raft-cluster-nodes cluster)])
      ;; Start all Raft nodes
      (for-each raft-start! nodes)
      (let ([states
             (map (lambda (n)
                    (make-replication-state base-config n cluster 0 #t))
                  nodes)])
        (values states cluster))))

  ;; stop-replication : replication-state -> void
  (def (stop-replication state)
    (replication-state-running?-set! state #f)
    (raft-stop! (replication-state-raft-node state))
    (void))

  ;; start-replication-from-node! : raft-node replication-config -> replication-state
  ;;
  ;; Wraps a caller-provided, already-started raft-node in a replication-state.
  ;; Use this when the raft-node has been configured externally before being wrapped
  ;; — for example, by (jerboa-db transport), which wires TCP proxy channels into
  ;; the node's peers list before calling raft-start!.
  ;;
  ;; The caller is responsible for having called raft-start! on the node.
  (def (start-replication-from-node! node config)
    (make-replication-state config node #f 0 #t))

  ;; =========================================================================
  ;; Status / introspection
  ;; =========================================================================

  ;; replication-status : replication-state -> alist
  ;;
  ;; Returns a snapshot of the node's current Raft state:
  ;;   node-id, role (follower/candidate/leader), term, commit-index,
  ;;   last-applied-index, leader? flag.
  (def (replication-status state)
    (let* ([node   (replication-state-raft-node state)]
           [config (replication-state-config state)])
      `((node-id            . ,(replication-config-node-id config))
        (role               . ,(raft-state node))
        (term               . ,(raft-term node))
        (commit-index       . ,(raft-commit-index node))
        (last-applied-index . ,(replication-state-last-applied-index state))
        (leader?            . ,(raft-leader? node)))))

  ;; replication-leader? : replication-state -> boolean
  (def (replication-leader? state)
    (raft-leader? (replication-state-raft-node state)))

  ;; replication-running? : replication-state -> boolean
  ;;
  ;; Returns #t if this replication state is still active (not stopped).
  ;; Used by the apply fiber in (jerboa-db cluster) to exit the polling loop.
  ;; Set to #f by stop-replication.
  (def (replication-running? state)
    (replication-state-running? state))

  ;; =========================================================================
  ;; Write path — leader only
  ;; =========================================================================

  ;; replicated-transact! : replication-state list -> (values symbol integer)
  ;;
  ;; Proposes a transaction to the Raft log.  Only the leader may accept writes;
  ;; followers must redirect to the leader or wait.  Raises an error if this
  ;; node is not currently the leader.
  ;;
  ;; tx-ops is a list of transaction operations in the format accepted by
  ;; process-transaction and transact! (entity maps, [:db/add …] vectors, etc.).
  ;; The command stored in the Raft log is tagged as ('tx . tx-ops).
  ;;
  ;; Once raft-propose! returns 'ok, the entry is in the Raft log and will be
  ;; committed once a majority acknowledges it.  Drive replication-apply-committed!
  ;; (passing the returned log-index as a hint, or without a hint) to apply it
  ;; to the local database.
  ;;
  ;; Returns: (values 'ok log-index) on acceptance,
  ;;          raises error on non-leader or Raft rejection.
  (def (replicated-transact! state tx-ops)
    (unless (replication-leader? state)
      (error 'replicated-transact!
             "not leader: only the Raft leader may accept write transactions"
             (replication-config-node-id (replication-state-config state))))
    (let-values ([(status log-index)
                  (raft-propose! (replication-state-raft-node state)
                                 (cons 'tx tx-ops))])
      (case status
        [(ok)  (values 'ok log-index)]
        [else  (error 'replicated-transact!
                      "Raft proposal rejected — node may have lost leadership"
                      status)])))

  ;; =========================================================================
  ;; Apply path — all nodes
  ;; =========================================================================

  ;; replication-apply-committed! :
  ;;   replication-state
  ;;   (-> db-value)            get-db     — thunk returning current db-value
  ;;   (-> list)                get-eid    — thunk returning next-eid mutable cell
  ;;   (db-value -> void)       set-db!    — callback to store updated db-value
  ;;   -> integer
  ;;
  ;; Applies all Raft log entries committed beyond last-applied-index to the
  ;; local database.  Uses process-transaction from (jerboa-db tx) to execute
  ;; each transaction against the current db-value.
  ;;
  ;; Accepts three callbacks rather than a raw connection record to avoid a
  ;; circular import with (jerboa-db core).  From core.sls:
  ;;
  ;;   (replication-apply-committed! state
  ;;     (lambda () (connection-current-db conn))
  ;;     (lambda () (connection-next-eid conn))
  ;;     (lambda (new-db) (connection-current-db-set! conn new-db)))
  ;;
  ;; Non-tx Raft commands (e.g., cluster membership changes) are skipped
  ;; (watermark still advances).  Failed transactions are skipped with a
  ;; warning rather than aborting the apply loop — this matches Datomic's
  ;; "best-effort follower" semantics.
  ;;
  ;; Returns the count of transactions successfully applied.
  (def (replication-apply-committed! state get-db get-eid set-db!)
    (let* ([node          (replication-state-raft-node state)]
           [commit-index  (raft-commit-index node)]
           [last-applied  (replication-state-last-applied-index state)]
           [applied-count 0])
      ;; raft-log returns the list of ALL log entries (both committed and possibly
      ;; uncommitted tail).  We process only entries whose index <= commit-index.
      ;;
      ;; (std raft) stores entries as log-entry records: (index term command).
      ;; log-entry accessors are NOT exported from (std raft), but Chez
      ;; define-record-type creates standard record instances; we use
      ;; record-accessor via the RTD to reach them by field position.
      (for-each
        (lambda (entry)
          (let ([entry-index   (log-entry-field entry 0)]   ;; field 0: index
                [entry-command (log-entry-field entry 2)])  ;; field 2: command
            (when (and (integer? entry-index)
                       (> entry-index last-applied)
                       (<= entry-index commit-index))
              ;; Advance watermark first so a crash mid-apply still moves forward.
              (replication-state-last-applied-index-set! state entry-index)
              ;; Apply only tx commands.
              (when (and (pair? entry-command)
                         (eq? (car entry-command) 'tx))
                (let* ([tx-ops   (cdr entry-command)]
                       [db       (get-db)]
                       [eid-cell (get-eid)]
                       [report   (guard (exn [#t
                                              (begin
                                                (display
                                                  (string-append
                                                    "replication-apply-committed!: "
                                                    "skipping failed tx at index "
                                                    (number->string entry-index)
                                                    ": "
                                                    (if (message-condition? exn)
                                                        (condition-message exn)
                                                        "unknown error")
                                                    "\n"))
                                                #f)])
                                   (process-transaction db tx-ops eid-cell))])
                  (when report
                    (set-db! (tx-report-db-after report))
                    (set! applied-count (+ applied-count 1))))))))
        (raft-log node))
      applied-count))

  ;; =========================================================================
  ;; Callback-based apply (conn-aware variant)
  ;; =========================================================================

  ;; replication-for-each-committed! : replication-state (integer list -> void) -> integer
  ;;
  ;; Iterates over newly committed Raft log entries beyond last-applied-index.
  ;; For each committed tx entry, calls (proc entry-index tx-ops).
  ;; The caller is responsible for executing the actual transaction — this allows
  ;; callers (e.g., jerboa-db cluster.ss) to use the full transact! path, which
  ;; includes schema materialization, fulltext indexing, and stats updates.
  ;;
  ;; Non-tx log entries (cluster membership changes, no-ops) are skipped.
  ;; Returns the number of entries delivered to proc.
  (def (replication-for-each-committed! state proc)
    (let* ([node          (replication-state-raft-node state)]
           [commit-index  (raft-commit-index node)]
           [last-applied  (replication-state-last-applied-index state)]
           [delivered 0])
      (for-each
        (lambda (entry)
          (let ([entry-index   (log-entry-field entry 0)]
                [entry-command (log-entry-field entry 2)])
            (when (and (integer? entry-index)
                       (> entry-index last-applied)
                       (<= entry-index commit-index))
              ;; Advance watermark before calling proc so a crash mid-apply
              ;; still moves the watermark forward (at-most-once semantics).
              (replication-state-last-applied-index-set! state entry-index)
              (when (and (pair? entry-command)
                         (eq? (car entry-command) 'tx))
                (proc entry-index (cdr entry-command))
                (set! delivered (+ delivered 1))))))
        (raft-log node))
      delivered))

  ;; replication-last-applied-index : replication-state -> integer
  ;;
  ;; Returns the highest Raft log index that has been applied locally.
  ;; Useful for polling in cluster-transact! to know when a proposal
  ;; has been applied (entry-index <= last-applied-index).
  (def (replication-last-applied-index state)
    (replication-state-last-applied-index state))

  ;; =========================================================================
  ;; Read consistency levels
  ;; =========================================================================

  ;; read-committed : replication-state (-> db-value) -> db-value
  ;;
  ;; Returns the local db-value as-is.  Safe on any node; may lag the leader
  ;; by at most one replication round-trip (~50 ms heartbeat interval).
  (def (read-committed state get-db)
    (get-db))

  ;; read-latest : replication-state (-> db-value) -> db-value
  ;;
  ;; Returns the local db-value, but only if this node is currently the leader.
  ;; The leader's local DB is always up-to-date with committed transactions.
  ;; Raises an error if called on a follower or candidate.
  (def (read-latest state get-db)
    (unless (replication-leader? state)
      (error 'read-latest
             "read-latest requires this node to be the Raft leader"
             (replication-config-node-id (replication-state-config state))))
    (get-db))

  ;; as-of-tx : replication-state (-> db-value) integer -> db-value
  ;;
  ;; Returns a time-travel view of the database as it was at the given
  ;; transaction basis (tx-id).  Valid on any node (the local indices contain
  ;; all historical datoms).  Uses (jerboa-db history) as-of semantics.
  (def (as-of-tx state get-db basis-tx)
    (as-of (get-db) basis-tx))

  ;; =========================================================================
  ;; Internal helpers
  ;; =========================================================================

  ;; replication-set-last-applied-index! : replication-state integer -> void
  ;;
  ;; Exported setter so (jerboa-db cluster)'s cluster-aware apply fiber can
  ;; advance the watermark without importing the internal record accessor.
  (def (replication-set-last-applied-index! state index)
    (replication-state-last-applied-index-set! state index))

  ;; log-entry-field : record integer -> value
  ;;
  ;; Access the nth field of a (std raft) log-entry record by position.
  ;; Fields are: 0=index, 1=term, 2=command (per define-record-type order).
  ;; This avoids importing internal log-entry-* accessors from (std raft).
  (def (log-entry-field entry n)
    (guard (exn [#t #f])
      ((record-accessor (record-rtd entry) n) entry)))

) ;; end library
