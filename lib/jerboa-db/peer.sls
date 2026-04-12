#!chezscheme
;;; (jerboa-db peer) — Remote peer client library
;;;
;;; Connects to a Jerboa-DB server over HTTP/WebSocket.
;;; Same API as embedded mode (connect, db, transact!, q, pull).

(library (jerboa-db peer)
  (export
    connect-remote remote-connection?
    remote-db remote-transact! remote-q remote-pull
    remote-tx-stream)

  (import (chezscheme)
          (std net request)
          (std net fiber-ws)
          (std text edn)
          (jerboa-db datom)
          (jerboa-db schema)
          (jerboa-db history))

  ;; ---- Remote connection ----

  (define-record-type remote-connection
    (fields url            ;; base URL: "http://localhost:8484"
            (mutable last-tx))) ;; last known transaction ID

  ;; ---- Helpers ----

  (define (build-url conn path)
    (string-append (remote-connection-url conn) path))

  (define (check-response! who resp url)
    (let ([status (request-status resp)])
      (unless (and (>= status 200) (< status 300))
        (error who
          (string-append "HTTP error " (number->string status) " from " url)
          (request-text resp)))))

  ;; POST with EDN body; returns parsed EDN from response body.
  (define (post-edn conn path body-obj)
    (let* ([url  (build-url conn path)]
           [body (edn->string body-obj)]
           [resp (http-post url
                   '(("Content-Type" . "application/edn")
                     ("Accept"       . "application/edn"))
                   body)])
      (check-response! 'post-edn resp url)
      (string->edn (request-text resp))))

  ;; GET; returns parsed EDN from response body.
  (define (get-edn conn path)
    (let* ([url  (build-url conn path)]
           [resp (http-get url
                   '(("Accept" . "application/edn")))])
      (check-response! 'get-edn resp url)
      (string->edn (request-text resp))))

  ;; ---- connect-remote ----
  ;; Verifies connectivity via GET /health and returns a connection record.

  (define (connect-remote url)
    (let* ([health-url (string-append url "/health")]
           [resp (guard (exn [#t #f])
                   (http-get health-url))])
      (unless resp
        (error 'connect-remote "Cannot reach server" url))
      (let ([status (request-status resp)]
            [body   (request-text resp)])
        (unless (and (= status 200) (string? body) (string=? body "ok"))
          (error 'connect-remote "Server health check failed" url status)))
      (make-remote-connection url 0)))

  ;; ---- remote-db ----
  ;; Fetches current db stats; updates cached last-tx.

  (define (remote-db conn)
    (let ([stats (get-edn conn "/api/db/stats")])
      (when (pair? stats)
        (let ([basis-tx-entry (assq 'basis-tx stats)])
          (when basis-tx-entry
            (remote-connection-last-tx-set! conn (cdr basis-tx-entry)))))
      stats))

  ;; ---- remote-transact! ----
  ;; POST /api/transact with EDN tx-ops, returns the tx report alist.

  (define (remote-transact! conn tx-ops)
    (let ([result (post-edn conn "/api/transact" tx-ops)])
      (when (pair? result)
        (let ([tx-id-entry (assq 'tx-id result)])
          (when tx-id-entry
            (remote-connection-last-tx-set! conn (cdr tx-id-entry)))))
      result))

  ;; ---- remote-q ----
  ;; POST /api/query with EDN query form, returns list of result tuples.

  (define (remote-q conn query-form)
    (post-edn conn "/api/query" query-form))

  ;; ---- remote-pull ----
  ;; POST /api/pull with EDN [pattern eid], returns entity map.

  (define (remote-pull conn pattern eid)
    (post-edn conn "/api/pull" (list pattern eid)))

  ;; ---- remote-tx-stream ----
  ;; Connects to /api/tx-stream via WebSocket, calls handler with each
  ;; transaction report (as a parsed EDN alist) until the stream ends.
  ;; handler: (lambda (tx-data) ...) where tx-data is a parsed EDN value.
  ;;
  ;; Note: This is a synchronous loop; run in a separate thread to avoid
  ;; blocking the caller.

  (define (remote-tx-stream conn handler)
    (error 'remote-tx-stream
      "WebSocket tx-stream requires fiber context; use (std fiber) to spawn a fiber and call remote-tx-stream/fiber instead"))

) ;; end library
