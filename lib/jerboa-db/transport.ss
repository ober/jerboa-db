#!chezscheme
;;; (jerboa-db transport) — TCP transport adapter for (std raft)
;;;
;;; Bridges each raft-node's in-process channels to TCP sockets so nodes
;;; can run in separate OS processes or on separate machines.
;;;
;;; Architecture:
;;;   Each Raft node has:
;;;     - An inbox channel: the Raft message loop reads incoming messages here.
;;;     - A peers list: ((id . channel) pairs) the node writes outgoing messages here.
;;;
;;;   For each peer, the transport installs a "proxy channel" in the peers list.
;;;   - Outbound fiber: reads proxy-ch → serializes → writes length-framed FASL to TCP
;;;   - Inbound fiber:  reads length-framed FASL from TCP → puts to node inbox
;;;
;;;   Connections are uni-directional per pair:
;;;     Node A dials B for A→B traffic (A writes via out-port of A's outbound connection)
;;;     Node B dials A for B→A traffic (B writes via out-port of B's outbound connection)
;;;   Each node also runs an accept loop; accepted connections feed the inbox.
;;;
;;; Wire protocol:
;;;   [4 bytes big-endian uint32: body length][body: FASL-encoded Raft message]
;;;
;;; client-propose messages (#(client-propose cmd reply-ch)) are local-only and
;;;   are dropped by the outbound fiber (they carry live reply channels).
;;;
;;; Apply semantics:
;;;   Unlike start-local-db-cluster (which reads the cluster leader's full log),
;;;   start-transport-db-node! uses each node's own committed log via
;;;   replication-for-each-committed!.  This is correct for multi-process clusters.

(library (jerboa-db transport)
  (export
    ;; Core transport lifecycle
    start-transport-node!
    stop-transport-node!
    ;; Add a peer after startup (needed for staged test setup)
    transport-node-add-peer!
    ;; Accessors
    transport-node-replication-state
    transport-node-listen-port
    transport-node?
    ;; Convenience: transport + DB connection in one call
    start-transport-db-node!)

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
          (rename (only (chezscheme) make-time) (make-time chez-make-time))
          (jerboa prelude)
          (std net tcp)
          (std net tls)
          (std fasl)
          (std misc channel)
          (std raft)
          (jerboa-db replication)
          (jerboa-db core)
          (jerboa-db cluster))

  ;; =========================================================================
  ;; Transport node record
  ;; =========================================================================

  ;; defstruct generates: make-transport-node transport-node? transport-node-<field>
  ;; and transport-node-<field>-set! for each field.
  (defstruct transport-node
    (raft-node            ;; (std raft) raft-node — holds inbox + peers
     replication-state    ;; replication-state wrapping the raft-node
     listen-port          ;; actual TCP listen port (important when 0 was requested)
     server               ;; tcp-server (plain) or tls-conn (TLS) listen handle
     tls-config           ;; tls-config record or #f (plain TCP)
     running?))           ;; mutable: set to #f by stop-transport-node!

  ;; =========================================================================
  ;; Stop sentinel
  ;; =========================================================================

  ;; A unique value placed on proxy channels to signal the outbound fiber to exit.
  ;; Cannot appear as a real Raft message (which are vectors).
  (define %stop% (list 'transport-stop))

  ;; =========================================================================
  ;; TLS port adapters
  ;; =========================================================================
  ;;
  ;; TLS gives us a single bidirectional `tls-conn` (SSL*) — but the rest of
  ;; the transport works with Chez binary input/output ports.  These adapters
  ;; wrap a tls-conn into the port abstractions so the framing layer below
  ;; is agnostic to TCP vs. TLS.
  ;;
  ;; A single tls-conn is shared between the in-port and out-port.  Closing
  ;; either port calls `tls-close`, which is idempotent.

  (define (tls-conn->in-port conn)
    (make-custom-binary-input-port "tls-in"
      (lambda (bv start n)
        (let ([buf (make-bytevector n)])
          (let ([got (guard (exn [#t 0]) (tls-read conn buf n))])
            (when (> got 0)
              (bytevector-copy! buf 0 bv start got))
            got)))
      #f #f
      (lambda () (guard (exn [#t (void)]) (tls-close conn)))))

  (define (tls-conn->out-port conn)
    (make-custom-binary-output-port "tls-out"
      (lambda (bv start n)
        (let ([slice (if (and (= start 0) (= n (bytevector-length bv)))
                         bv
                         (let ([s (make-bytevector n)])
                           (bytevector-copy! bv start s 0 n)
                           s))])
          (guard (exn [#t (error 'tls-write-port "TLS write failed" exn)])
            (tls-write conn slice))
          n))
      #f #f
      (lambda () (guard (exn [#t (void)]) (tls-close conn)))))

  ;; =========================================================================
  ;; Wire protocol
  ;; =========================================================================

  ;; write-frame : binary-output-port bytevector → void
  ;; Writes a 4-byte big-endian uint32 length header followed by body bytes.
  (define (write-frame out-port body)
    (let* ([len    (bytevector-length body)]
           [header (make-bytevector 4)])
      (bytevector-u8-set! header 0 (bitwise-and (ash len -24) #xff))
      (bytevector-u8-set! header 1 (bitwise-and (ash len -16) #xff))
      (bytevector-u8-set! header 2 (bitwise-and (ash len  -8) #xff))
      (bytevector-u8-set! header 3 (bitwise-and  len          #xff))
      (put-bytevector out-port header)
      (put-bytevector out-port body)
      (flush-output-port out-port)))

  ;; read-frame : binary-input-port → bytevector or #f
  ;; Reads a length-prefixed frame.  Returns #f on EOF or truncated read.
  (define (read-frame in-port)
    (let ([header (get-bytevector-n in-port 4)])
      (if (and (bytevector? header) (= (bytevector-length header) 4))
        (let ([len (+ (ash (bytevector-u8-ref header 0) 24)
                      (ash (bytevector-u8-ref header 1) 16)
                      (ash (bytevector-u8-ref header 2)  8)
                           (bytevector-u8-ref header 3))])
          (let ([body (get-bytevector-n in-port len)])
            (and (bytevector? body) (= (bytevector-length body) len) body)))
        #f)))

  ;; =========================================================================
  ;; Inbound listener
  ;; =========================================================================

  ;; start-inbound-listener! : binary-input-port channel → thread
  ;;
  ;; Reads FASL-framed Raft messages from in-port and delivers them to
  ;; node-inbox.  One fiber per accepted inbound TCP connection.
  ;; Exits silently on EOF or deserialization error.
  (define (start-inbound-listener! in-port node-inbox)
    (fork-thread
      (lambda ()
        (let loop ()
          (let ([frame (guard (exn [#t #f]) (read-frame in-port))])
            (when frame
              (let ([msg (guard (exn [#t #f]) (bytevector->fasl frame))])
                (when msg
                  (guard (exn [#t (void)])
                    (channel-put node-inbox msg))))
              (loop)))))))

  ;; =========================================================================
  ;; Accept loop
  ;; =========================================================================

  ;; start-accept-loop! : server channel tls-config → thread
  ;;
  ;; Accepts inbound connections and starts an inbound listener for each.
  ;; In TCP mode, the out-port is closed immediately — inbound connections
  ;; are receive-only (peers dial us for their outbound traffic).
  ;; In TLS mode, accept yields a single bidirectional tls-conn; we wrap
  ;; only its read side in a port and discard the write side (the underlying
  ;; SSL is closed when the in-port is closed).
  (define (start-accept-loop! server node-inbox tls-cfg)
    (fork-thread
      (lambda ()
        (let loop ()
          (cond
            [tls-cfg
             (let ([tconn (guard (exn [#t #f]) (tls-accept server))])
               (when tconn
                 (start-inbound-listener!
                   (tls-conn->in-port tconn) node-inbox)))]
            [else
             (let ([conn (guard (exn [#t #f])
                           (let-values ([(in out) (tcp-accept-binary server)])
                             (cons in out)))])
               (when conn
                 (guard (exn [#t (void)]) (close-port (cdr conn)))
                 (start-inbound-listener! (car conn) node-inbox)))])
          (loop)))))

  ;; =========================================================================
  ;; Outbound peer connector
  ;; =========================================================================

  ;; start-peer-connector! : channel string integer tls-config → thread
  ;;
  ;; Maintains a persistent outbound connection to one peer.
  ;; On connection failure, reconnects with exponential backoff (100ms → 5s).
  ;; Stops when the %stop% sentinel is received on proxy-ch.
  ;;
  ;; If tls-cfg is non-#f, dials TLS via tls-connect and wraps the resulting
  ;; tls-conn in a binary output port; otherwise uses plain TCP.
  ;;
  ;; client-propose messages (#(client-propose cmd reply-ch)) are dropped
  ;; — they carry live reply channels and must never cross process boundaries.
  (define (start-peer-connector! proxy-ch peer-host peer-port tls-cfg)
    (fork-thread
      (lambda ()
        (let retry ([backoff-ms 100])
          (let ([out-port
                 (guard (exn [#t #f])
                   (cond
                     [tls-cfg
                      (let ([tconn (tls-connect peer-host peer-port tls-cfg)])
                        (tls-conn->out-port tconn))]
                     [else
                      (let-values ([(in out) (tcp-connect-binary peer-host peer-port)])
                        out)]))])
            (if out-port
              (begin
                ;; in-port intentionally ignored in TCP mode: both ports share
                ;; the same fd/closed? flag in fd->binary-ports, so closing in-port
                ;; would destroy out-port too.  The fd is GC'd when the conn drops.
                ;; Run the proxy loop; returns #t if disconnected, #f if stopped
                (let ([disconnected?
                       (let loop ()
                         (let ([msg (channel-get proxy-ch)])
                           (cond
                             ;; Stop sentinel — exit cleanly
                             [(eq? msg %stop%) #f]
                             ;; client-propose carries a live channel — skip
                             [(and (vector? msg)
                                   (> (vector-length msg) 0)
                                   (eq? (vector-ref msg 0) 'client-propose))
                              (loop)]
                             [else
                              (let ([ok? (guard (exn [#t #f])
                                           (write-frame out-port (fasl->bytevector msg))
                                           #t)])
                                (if ok?
                                  (loop)
                                  #t))])))])  ;; #t = disconnected, retry
                  (guard (exn [#t (void)]) (close-port out-port))
                  (when disconnected?
                    (sleep (chez-make-time 'time-duration (* backoff-ms 1000000) 0))
                    (retry (min (* backoff-ms 2) 5000)))))
              ;; Connection failed — sleep and retry
              (begin
                (sleep (chez-make-time 'time-duration (* backoff-ms 1000000) 0))
                (retry (min (* backoff-ms 2) 5000)))))))))

  ;; =========================================================================
  ;; Internal helpers
  ;; =========================================================================

  ;; Proxy channel capacity.  Large enough that a 5-second reconnect window
  ;; (worst-case backoff) never fills the channel at 50 ms heartbeat rate.
  (define proxy-ch-capacity 1024)

  ;; wire-peer! : (id host port) tls-config → (id . proxy-channel)
  ;;
  ;; Creates a proxy channel for one peer and starts the outbound connector fiber.
  (define (wire-peer! spec tls-cfg)
    (let ([peer-id   (car   spec)]
          [peer-host (cadr  spec)]
          [peer-port (caddr spec)])
      (let ([proxy-ch (make-channel proxy-ch-capacity)])
        (start-peer-connector! proxy-ch peer-host peer-port tls-cfg)
        (cons peer-id proxy-ch))))

  ;; =========================================================================
  ;; Lifecycle
  ;; =========================================================================

  ;; start-transport-node! :
  ;;   node-id     symbol or integer
  ;;   peer-specs  list of (id host port)
  ;;   data-path   string (":memory:" or file path — informational, passed to config)
  ;;   listen-port integer (0 = OS-assigned; use transport-node-listen-port to read back)
  ;;   tls-config  (optional) tls-config record from (std net tls) or #f
  ;;   → transport-node
  ;;
  ;; Creates a raft-node, wires proxy channels for each peer, starts the
  ;; listener, and starts the Raft consensus engine.  When tls-config is
  ;; supplied, uses TLS over TCP via libssl (verified peer certs by default
  ;; — set verify-peer:/verify-hostname: in the config to relax for tests).
  ;;
  ;; To get a full DB-backed node with the cluster API, use start-transport-db-node!.
  (def (start-transport-node! node-id peer-specs data-path listen-port (tls-config #f))
    (let* ([node       (make-raft-node node-id)]
           [node-inbox (raft-node-inbox node)]
           [peer-chans (map (lambda (s) (wire-peer! s tls-config)) peer-specs)])
      ;; Install proxy channels as the node's peers list
      (raft-node-peers-set! node peer-chans)
      ;; Bind listen socket (plain TCP or TLS)
      (let-values ([(server actual-port)
                    (cond
                      [tls-config
                       (let* ([sconn (tls-listen "0.0.0.0" listen-port tls-config)]
                              [fd    (%tls-server-port-from-conn sconn listen-port)])
                         (values sconn fd))]
                      [else
                       (let ([s (tcp-listen "0.0.0.0" listen-port 16)])
                         (values s (tcp-server-port s)))])])
        ;; Accept loop feeds the node's inbox from inbound connections
        (start-accept-loop! server node-inbox tls-config)
        ;; Start Raft consensus engine
        (raft-start! node)
        ;; Wrap in replication-state for cluster API compatibility
        (let* ([config (new-replication-config node-id #f data-path)]
               [state  (start-replication-from-node! node config)])
          (display (str "transport: node " node-id
                        " listening on port " actual-port
                        (if tls-config " (TLS)" "")
                        "\n"))
          (make-transport-node node state actual-port server tls-config #t)))))

  ;; %tls-server-port-from-conn : tls-conn requested-port → integer
  ;;
  ;; tls-listen does not currently expose the bound port, so we fall back
  ;; to the requested port.  When `requested-port` is 0 (OS-assigned), the
  ;; caller cannot easily discover the assigned port — recommend passing
  ;; an explicit non-zero port for TLS deployments.
  (define (%tls-server-port-from-conn _conn requested-port) requested-port)

  ;; stop-transport-node! : transport-node → void
  ;;
  ;; Stops the Raft node, signals outbound proxy fibers to exit, and closes
  ;; the listen socket (TCP or TLS).
  (def (stop-transport-node! tnode)
    (transport-node-running?-set! tnode #f)
    ;; Stop Raft (sends stop-signal to inbox)
    (stop-replication (transport-node-replication-state tnode))
    ;; Signal each outbound proxy to exit
    (for-each (lambda (p)
                (guard (exn [#t (void)])
                  (channel-put (cdr p) %stop%)))
              (raft-node-peers (transport-node-raft-node tnode)))
    ;; Close listen socket
    (guard (exn [#t (void)])
      (cond
        [(transport-node-tls-config tnode)
         (tls-close (transport-node-server tnode))]
        [else
         (tcp-close (transport-node-server tnode))])))

  ;; transport-node-add-peer! :
  ;;   transport-node peer-id string integer → void
  ;;
  ;; Adds a new peer to an already-running transport node and starts an
  ;; outbound connector to it.  This is the key primitive for staged test
  ;; setup, where port numbers are only known after start:
  ;;
  ;;   (def a (start-transport-node! 0 '() ":memory:" 0))
  ;;   (def b (start-transport-node! 1 `((0 "127.0.0.1" ,(transport-node-listen-port a)))
  ;;                                 ":memory:" 0))
  ;;   (transport-node-add-peer! a 1 "127.0.0.1" (transport-node-listen-port b))
  (def (transport-node-add-peer! tnode peer-id peer-host peer-port)
    (let* ([node      (transport-node-raft-node tnode)]
           [tls-cfg   (transport-node-tls-config tnode)]
           [new-entry (wire-peer! (list peer-id peer-host peer-port) tls-cfg)])
      ;; raft-node-add-peer! is thread-safe and also initialises next-index /
      ;; match-index if the node is already a leader, preventing send-heartbeats!
      ;; from crashing with a (cdr #f) on the new peer's missing entry.
      (raft-node-add-peer! node peer-id (cdr new-entry))))

  ;; =========================================================================
  ;; Convenience: transport + DB + replicated-conn
  ;; =========================================================================

  ;; start-transport-db-node! :
  ;;   node-id    symbol or integer
  ;;   peer-specs list of (id host port)
  ;;   data-path  string (":memory:" or file path for persistent storage)
  ;;   listen-port integer (0 = OS-assigned)
  ;;   tls-config (optional) tls-config record from (std net tls) or #f
  ;;   → (values transport-node replicated-conn)
  ;;
  ;; Creates the transport node, opens a DB connection at data-path, and
  ;; starts an apply fiber that replicates committed Raft entries into the
  ;; local DB.  Returns a replicated-conn compatible with the full cluster API:
  ;;   cluster-transact!, cluster-db, cluster-status, cluster-leader?
  (def (start-transport-db-node! node-id peer-specs data-path listen-port (tls-config #f))
    (let* ([tnode  (start-transport-node! node-id peer-specs data-path listen-port tls-config)]
           [state  (transport-node-replication-state tnode)]
           [conn   (connect data-path)]
           [fiber  (fork-thread
                     (lambda ()
                       (let loop ()
                         (sleep (chez-make-time 'time-duration 50000000 0)) ;; 50 ms
                         (when (replication-running? state)
                           (replication-for-each-committed! state
                             (lambda (entry-index tx-ops)
                               (guard (exn [#t
                                            (display
                                             (str "transport apply: skip failed tx at index "
                                                  entry-index "\n"))])
                                 (transact! conn tx-ops))))
                           (loop)))))]
           [rconn  (make-replicated-conn conn state fiber)])
      (values tnode rconn)))

) ;; end library
