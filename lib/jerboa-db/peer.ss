#!chezscheme
;;; (jerboa-db peer) — Remote peer client library
;;;
;;; Connects to a Jerboa-DB server over HTTP/WebSocket.
;;; Same API as embedded mode (connect, db, transact!, q, pull).
;;;
;;; Automatic failover: connect-remote* accepts a list of URLs. If a request
;;; to the primary URL fails, the client retries the next URL with exponential
;;; backoff. Useful for Raft-based HA deployments where leadership can change.

(library (jerboa-db peer)
  (export
    connect-remote connect-remote* remote-connection?
    remote-db remote-transact! remote-q remote-pull
    remote-tx-stream)

  (import (except (chezscheme)
                  make-hash-table hash-table?
                  sort sort!
                  printf fprintf
                  path-extension path-absolute?
                  with-input-from-string with-output-to-string
                  iota 1+ 1-
                  partition
                  make-date make-time)
          (jerboa prelude)
          (std net request)
          (std net fiber-ws)
          (std text edn)
          (jerboa-db datom)
          (jerboa-db schema)
          (jerboa-db history))

  ;; ---- Remote connection ----

  (defstruct remote-connection
    (urls          ;; list of URLs, first is current primary
     last-tx       ;; last known transaction ID
     retry-count)) ;; consecutive failures on current primary

  (def (make-single-remote-connection url)
    (make-remote-connection (list url) 0 0))

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
                       (sleep (make-time 'time-duration
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
               [resp (http-get url '(("Accept" . "application/edn")))])
          (check-response! 'get-edn resp url)
          (string->edn (request-text resp))))
      3))

  ;; ---- connect-remote ----
  ;; Single URL. Verifies connectivity via GET /health.

  (def (connect-remote url)
    (let ([conn (make-single-remote-connection url)])
      (verify-connectivity! conn)
      conn))

  ;; ---- connect-remote* ----
  ;; Multiple URLs for automatic failover.

  (def (connect-remote* urls)
    (unless (pair? urls)
      (error 'connect-remote* "At least one URL required"))
    (let ([conn (make-remote-connection urls 0 0)])
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

  (def (remote-db conn)
    (let ([stats (get-edn conn "/api/db/stats")])
      (when (pair? stats)
        (let ([e (assq 'basis-tx stats)])
          (when e (remote-connection-last-tx-set! conn (cdr e)))))
      stats))

  ;; ---- remote-transact! ----

  (def (remote-transact! conn tx-ops)
    (let ([result (post-edn conn "/api/transact" tx-ops)])
      (when (pair? result)
        (let ([e (assq 'tx-id result)])
          (when e (remote-connection-last-tx-set! conn (cdr e)))))
      result))

  ;; ---- remote-q ----

  (def (remote-q conn query-form)
    (post-edn conn "/api/query" query-form))

  ;; ---- remote-pull ----

  (def (remote-pull conn pattern eid)
    (post-edn conn "/api/pull" (list pattern eid)))

  ;; ---- remote-tx-stream ----

  (def (remote-tx-stream conn handler)
    (error 'remote-tx-stream
      "WebSocket tx-stream requires fiber context; use (std fiber) to spawn"))

) ;; end library
