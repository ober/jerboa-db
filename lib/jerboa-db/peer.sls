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
          (jerboa-db datom)
          (jerboa-db schema)
          (jerboa-db history))

  ;; ---- Remote connection ----

  (define-record-type remote-connection
    (fields url            ;; base URL: "http://localhost:8484"
            (mutable cache)   ;; local segment cache
            (mutable last-tx))) ;; last known transaction

  (define (connect-remote url)
    (let ([conn (make-remote-connection url (make-eq-hashtable) 0)])
      ;; Verify connectivity
      (let ([health (http-get (string-append url "/health"))])
        (unless (and health (string=? health "ok"))
          (error 'connect-remote "Cannot reach server" url)))
      conn))

  ;; ---- Remote operations ----

  (define (remote-db conn)
    ;; Get current database snapshot info from server
    (let ([stats (http-get (string-append (remote-connection-url conn)
                                          "/api/db/stats"))])
      (remote-connection-last-tx-set! conn
        (cdr (assq 'basis-tx (read-from-string stats))))
      stats))

  (define (remote-transact! conn tx-ops)
    (http-post
      (string-append (remote-connection-url conn) "/api/transact")
      (format "~s" tx-ops)))

  (define (remote-q conn query-form)
    (let ([response (http-post
                      (string-append (remote-connection-url conn) "/api/query")
                      (format "~s" query-form))])
      (read-from-string response)))

  (define (remote-pull conn pattern eid)
    (let ([response (http-post
                      (string-append (remote-connection-url conn) "/api/pull")
                      (format "~s" (list pattern eid)))])
      (read-from-string response)))

  ;; ---- Transaction stream (WebSocket) ----

  (define (remote-tx-stream conn handler)
    ;; handler: (lambda (tx-report-data) ...)
    ;; Will use (std net fiber-ws) for WebSocket connection
    (error 'remote-tx-stream
      "WebSocket tx-stream not yet implemented — requires (std net fiber-ws)"))

  ;; ---- HTTP stubs ----
  ;; These will be replaced with (std net request) calls.

  (define (http-get url)
    ;; Stub: will use (std net request) GET
    "ok")

  (define (http-post url body)
    ;; Stub: will use (std net request) POST
    "")

  (define (read-from-string s)
    (guard (exn [else #f])
      (let ([port (open-input-string s)])
        (read port))))

) ;; end library
