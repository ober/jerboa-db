#!chezscheme
;;; (jerboa-db peer) — Remote peer client library
;;;
;;; Connects to a Jerboa-DB server over HTTP/WebSocket.
;;; Same API as embedded mode (connect, db, transact!, q, pull).
;;;
;;; Round 9 (Phase 46–49) features:
;;;   - remote-entity                    (Phase 46)
;;;   - named-database routing           (Phase 47)
;;;   - basis-tx-keyed query-result cache (Phase 49)
;;;
;;; Automatic failover: connect-remote* accepts a list of URLs. If a request
;;; to the primary URL fails, the client retries the next URL with exponential
;;; backoff. Useful for Raft-based HA deployments where leadership can change.

(library (jerboa-db peer)
  (export
    ;; Core lifecycle
    connect-remote connect-remote* remote-connection?
    ;; Database operations
    remote-db remote-transact! remote-q remote-pull remote-entity
    remote-tx-stream
    ;; Cache controls (Phase 49)
    remote-cache-stats remote-cache-clear! remote-cache-set-capacity!
    ;; Accessors useful for diagnostics + named-DB introspection
    remote-connection-db-name
    remote-connection-last-tx
    remote-connection-urls)

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
          (rename (only (chezscheme) make-time) (make-time chez-make-time))
          (jerboa prelude)
          (std net request)
          (std net fiber-ws)
          (std text edn)
          (jerboa-db datom)
          (jerboa-db schema)
          (jerboa-db history))

  ;; ---- Remote connection ----
  ;;
  ;; Field order is positional for the defstruct-generated constructor;
  ;; do not reorder without updating the make-* call sites below.

  (defstruct remote-connection
    (urls               ;; list of URLs, first is current primary
     last-tx            ;; last known transaction ID (used as cache basis)
     retry-count        ;; consecutive failures on current primary
     db-name            ;; #f for default DB, otherwise a non-empty string
     cache              ;; hash-table: cache-key (list) → cached EDN result
     cache-keys         ;; list of cache-keys, MRU first (LRU tail evicted)
     cache-capacity     ;; integer, max # entries in `cache`
     cache-mutex))      ;; mutex protecting the three cache fields above

  (def %default-cache-capacity% 256)

  (def (make-fresh-remote urls db-name)
    (make-remote-connection
      urls               ;; urls
      0                  ;; last-tx
      0                  ;; retry-count
      (and db-name
           (or (and (string? db-name) (positive? (string-length db-name)) db-name)
               (error 'connect-remote "db-name must be a non-empty string or #f"
                      db-name)))
      (make-hash-table)  ;; cache
      '()                ;; cache-keys
      %default-cache-capacity% ;; cache-capacity
      (make-mutex)))     ;; cache-mutex

  ;; ---- URL management ----

  (def (current-url conn)
    (car (remote-connection-urls conn)))

  (def (build-url conn path)
    (string-append (current-url conn) path))

  ;; Rotate: move failed primary to end, try next URL
  (def (failover! conn)
    (let ([urls (remote-connection-urls conn)])
      (when (> (length urls) 1)
        (remote-connection-urls-set! conn (append (cdr urls) (list (car urls))))
        (remote-connection-retry-count-set! conn 0))))

  ;; ---- Path routing (Phase 47: named DB support) ----
  ;;
  ;; The default DB uses a mix of /api/* and /api/db/* paths (legacy).
  ;; A named DB consistently uses /api/db/<name>/*.

  (def (api-path conn verb)
    (let ([name (remote-connection-db-name conn)])
      (cond
        [name
         (case verb
           [(stats)         (str "/api/db/" name "/stats")]
           [(schema)        (str "/api/db/" name "/schema")]
           [(transact)      (str "/api/db/" name "/transact")]
           [(query)         (str "/api/db/" name "/query")]
           [(pull)          (str "/api/db/" name "/pull")]
           [(entity-prefix) (str "/api/db/" name "/entity/")]
           [else (error 'api-path "unknown verb" verb)])]
        [else
         (case verb
           [(stats)         "/api/db/stats"]
           [(schema)        "/api/db/schema"]
           [(transact)      "/api/transact"]
           [(query)         "/api/query"]
           [(pull)          "/api/pull"]
           [(entity-prefix) "/api/entity/"]
           [else (error 'api-path "unknown verb" verb)])])))

  ;; ---- EDN response shape repair ----
  ;;
  ;; The EDN encoder cannot represent dotted pairs and emits each (k . v)
  ;; alist entry as the proper 2-element list (k v).  After parsing we walk
  ;; the structure and convert any list-of-2-element-lists with symbol heads
  ;; back into dotted-pair alists, so callers can use (cdr (assq ...)).

  (def (looks-like-alist? x)
    (and (pair? x)
         (let loop ([l x])
           (cond
             [(null? l) #t]
             [(and (pair? l)
                   (pair? (car l))
                   (symbol? (caar l))
                   (pair? (cdar l))
                   (null? (cddar l)))
              (loop (cdr l))]
             [else #f]))))

  (def (edn->alist x)
    (cond
      [(null? x) x]
      [(looks-like-alist? x)
       (map (lambda (entry) (cons (car entry) (edn->alist (cadr entry)))) x)]
      [(pair? x)
       (cons (edn->alist (car x)) (edn->alist (cdr x)))]
      [else x]))

  ;; ---- HTTP helpers ----

  (def (check-response! who resp url)
    (let ([status (request-status resp)])
      (unless (and (>= status 200) (< status 300))
        (error who
          (string-append "HTTP error " (number->string status) " from " url)
          (request-text resp)))))

  ;; Execute a thunk with retry+failover on transient errors.
  ;; Retries up to max-retries times with exponential backoff (100ms, 200ms, 400ms).

  (def (with-retry conn thunk max-retries)
    (let loop ([attempt 0])
      (guard (exn
              [#t
               (remote-connection-retry-count-set! conn
                 (+ (remote-connection-retry-count conn) 1))
               (if (< attempt max-retries)
                   (begin
                     (failover! conn)
                     ;; Exponential backoff: sleep 100ms * 2^attempt
                     (let ([delay-s (/ (* 100 (expt 2 attempt)) 1000)])
                       (sleep (chez-make-time 'time-duration
                                (inexact->exact (floor (* delay-s 1000000000)))
                                0)))
                     (loop (+ attempt 1)))
                   (raise exn))])
        (thunk))))

  ;; POST with EDN body; returns parsed EDN from response body.
  (def (post-edn conn path body-obj)
    (with-retry conn
      (lambda ()
        (let* ([url  (build-url conn path)]
               [body (edn->string body-obj)]
               [resp (http-post url
                       '(("Content-Type" . "application/edn")
                         ("Accept"       . "application/edn"))
                       body)])
          (check-response! 'post-edn resp url)
          (string->edn (request-text resp))))
      3))

  ;; GET; returns parsed EDN from response body.
  (def (get-edn conn path)
    (with-retry conn
      (lambda ()
        (let* ([url  (build-url conn path)]
               [resp (http-get url '(("Accept" . "application/edn")) #f)])
          (check-response! 'get-edn resp url)
          (string->edn (request-text resp))))
      3))

  ;; ---- Cache (Phase 49) ----
  ;;
  ;; LRU keyed on (basis-tx, op-shape).  Cache lookup uses the connection's
  ;; current `last-tx` as the basis; transact! bumps last-tx, so old keys
  ;; become unreachable and naturally age out via LRU eviction.
  ;;
  ;; The cache is intentionally optimistic: if another client transacts
  ;; against the same DB, our last-tx may be stale until our next transact
  ;; or remote-db call.  Datomic clients have similar semantics.

  (def (cache-key conn op . args)
    ;; e.g. (1234 q ((find ?e) (where ...)))
    ;;      (1234 pull [* :user/name] 17)
    ;;      (1234 entity 17)
    (cons (remote-connection-last-tx conn) (cons op args)))

  (def (cache-lookup conn key)
    ;; Returns (cons #t value) on hit, (cons #f #f) on miss.
    (with-mutex (remote-connection-cache-mutex conn)
      (let ([cache (remote-connection-cache conn)])
        (cond
          [(hash-key? cache key)
           (let ([val (hash-ref cache key)])
             ;; Move key to MRU front
             (remote-connection-cache-keys-set! conn
               (cons key
                     (filter (lambda (k) (not (equal? k key)))
                             (remote-connection-cache-keys conn))))
             (cons #t val))]
          [else (cons #f #f)]))))

  (def (cache-store! conn key val)
    (with-mutex (remote-connection-cache-mutex conn)
      (let* ([cache    (remote-connection-cache conn)]
             [old-keys (filter (lambda (k) (not (equal? k key)))
                               (remote-connection-cache-keys conn))]
             [cap      (remote-connection-cache-capacity conn)]
             [new-keys (cons key old-keys)])
        (hash-put! cache key val)
        (cond
          [(> (length new-keys) cap)
           (let ([keep (take new-keys cap)]
                 [drop (drop new-keys cap)])
             (for-each (lambda (k) (hash-remove! cache k)) drop)
             (remote-connection-cache-keys-set! conn keep))]
          [else
           (remote-connection-cache-keys-set! conn new-keys)]))))

  (def (remote-cache-clear! conn)
    (with-mutex (remote-connection-cache-mutex conn)
      (remote-connection-cache-set! conn (make-hash-table))
      (remote-connection-cache-keys-set! conn '())))

  (def (remote-cache-set-capacity! conn cap)
    (unless (and (integer? cap) (positive? cap))
      (error 'remote-cache-set-capacity! "capacity must be a positive integer" cap))
    (with-mutex (remote-connection-cache-mutex conn)
      (remote-connection-cache-capacity-set! conn cap)
      ;; Trim if currently over capacity
      (let* ([keys (remote-connection-cache-keys conn)]
             [over (- (length keys) cap)])
        (when (positive? over)
          (let ([keep (take keys cap)]
                [drop (drop keys cap)])
            (let ([cache (remote-connection-cache conn)])
              (for-each (lambda (k) (hash-remove! cache k)) drop))
            (remote-connection-cache-keys-set! conn keep))))))

  (def (remote-cache-stats conn)
    (with-mutex (remote-connection-cache-mutex conn)
      (list
        (cons 'size     (length (remote-connection-cache-keys conn)))
        (cons 'capacity (remote-connection-cache-capacity conn))
        (cons 'last-tx  (remote-connection-last-tx conn)))))

  ;; ---- connect-remote ----
  ;; Single URL.  Verifies connectivity via GET /health.
  ;; Optional db-name (string) routes operations to /api/db/<name>/*.

  (def (connect-remote url . opts)
    (let ([conn (make-fresh-remote (list url)
                  (and (pair? opts) (car opts)))])
      (verify-connectivity! conn)
      conn))

  ;; ---- connect-remote* ----
  ;; Multiple URLs for automatic failover.  Optional db-name as second arg.

  (def (connect-remote* urls . opts)
    (unless (pair? urls)
      (error 'connect-remote* "At least one URL required"))
    (let ([conn (make-fresh-remote urls
                  (and (pair? opts) (car opts)))])
      ;; Try to connect to any available URL
      (let loop ([remaining urls])
        (if (null? remaining)
            (error 'connect-remote* "Could not reach any server" urls)
            (guard (exn [#t (loop (cdr remaining))])
              (remote-connection-urls-set! conn
                (append (list (car remaining))
                        (filter (lambda (u) (not (string=? u (car remaining)))) urls)))
              (verify-connectivity! conn))))
      conn))

  (def (verify-connectivity! conn)
    (let* ([health-url (string-append (current-url conn) "/health")]
           [resp (guard (exn [#t #f])
                   (http-get health-url))])
      (unless resp
        (error 'connect-remote "Cannot reach server" (current-url conn)))
      (let ([status (request-status resp)])
        (unless (= status 200)
          (error 'connect-remote "Server health check failed"
                 (current-url conn) status)))))

  ;; ---- remote-db ----
  ;;
  ;; Returns the server's stats alist for the connection's database, and
  ;; refreshes our `last-tx` from the response so subsequent cache lookups
  ;; key on the freshest basis we know.

  (def (remote-db conn)
    (let ([stats (edn->alist (get-edn conn (api-path conn 'stats)))])
      (when (pair? stats)
        (let ([e (assq 'basis-tx stats)])
          (when e (remote-connection-last-tx-set! conn (cdr e)))))
      stats))

  ;; ---- remote-transact! ----
  ;;
  ;; Bumps last-tx on success.  Old cache entries (keyed by previous
  ;; last-tx) become unreachable; LRU evicts them as new keys arrive.

  (def (remote-transact! conn tx-ops)
    (let ([result (edn->alist (post-edn conn (api-path conn 'transact) tx-ops))])
      (when (pair? result)
        (let ([e (assq 'tx-id result)])
          (when e (remote-connection-last-tx-set! conn (cdr e)))))
      result))

  ;; ---- remote-q ----

  (def (remote-q conn query-form)
    (let* ([key (cache-key conn 'q query-form)]
           [hit (cache-lookup conn key)])
      (cond
        [(car hit) (cdr hit)]
        [else
         (let ([result (post-edn conn (api-path conn 'query) query-form)])
           (cache-store! conn key result)
           result)])))

  ;; ---- remote-pull ----

  (def (remote-pull conn pattern eid)
    (let* ([key (cache-key conn 'pull pattern eid)]
           [hit (cache-lookup conn key)])
      (cond
        [(car hit) (cdr hit)]
        [else
         (let ([result (edn->alist
                         (post-edn conn (api-path conn 'pull) (list pattern eid)))])
           (cache-store! conn key result)
           result)])))

  ;; ---- remote-entity (Phase 46) ----
  ;;
  ;; Server returns the equivalent of (pull '[*] eid) for the given entity.

  (def (remote-entity conn eid)
    (unless (number? eid)
      (error 'remote-entity "eid must be a number" eid))
    (let* ([key (cache-key conn 'entity eid)]
           [hit (cache-lookup conn key)])
      (cond
        [(car hit) (cdr hit)]
        [else
         (let ([result (edn->alist
                         (get-edn conn
                           (string-append (api-path conn 'entity-prefix)
                                          (number->string eid))))])
           (cache-store! conn key result)
           result)])))

  ;; ---- remote-tx-stream ----
  ;;
  ;; The server publishes tx events on /api/tx-stream as a WebSocket.
  ;; A client-side WebSocket connect helper is not yet available in
  ;; (std net fiber-ws) — only the server-side `fiber-ws-upgrade` is.
  ;; Closing the gap requires an upstream `fiber-ws-connect` in jerboa
  ;; (handshake + Sec-WebSocket-Key + frame codec from std net websocket).
  ;;
  ;; Until then, applications can simulate streaming via periodic
  ;; remote-db calls — the basis-tx field advances with every server-side
  ;; transact and remote-db updates the cache basis automatically.

  (def (remote-tx-stream conn handler)
    (error 'remote-tx-stream
      "WebSocket tx-stream needs (std net fiber-ws) client-side connect; \
       use periodic remote-db polling for now"))

) ;; end library
