#!chezscheme
;;; (jerboa-db server) — HTTP/WebSocket API server
;;;
;;; Exposes Jerboa-DB over HTTP for multi-client access.
;;; Built on (std net fiber-httpd) + (std net router).

(library (jerboa-db server)
  (export
    start-server stop-server
    new-server-config server-config?)

  (import (chezscheme)
          (jerboa-db datom)
          (jerboa-db schema)
          (jerboa-db core))

  ;; ---- Server configuration ----

  (define-record-type server-config
    (fields port       ;; integer (default 8484)
            host       ;; string (default "0.0.0.0")
            conn       ;; jerboa-db connection
            format))   ;; 'edn or 'json (default 'edn)

  (define (new-server-config conn . opts)
    (let ([port (if (and (pair? opts) (number? (car opts))) (car opts) 8484)]
          [host (if (and (pair? opts) (pair? (cdr opts))) (cadr opts) "0.0.0.0")]
          [fmt  (if (and (pair? opts) (pair? (cdr opts)) (pair? (cddr opts)))
                    (caddr opts) 'edn)])
      (make-server-config port host conn fmt)))

  ;; ---- Route definitions ----
  ;; POST /api/transact        — submit transaction
  ;; POST /api/query           — run Datalog query
  ;; POST /api/pull            — pull entity data
  ;; GET  /api/entity/:eid     — get entity by ID
  ;; GET  /api/db/stats        — database statistics
  ;; GET  /api/db/schema       — current schema
  ;; GET  /health              — health check
  ;; WS   /api/tx-stream       — real-time transaction stream

  (define (build-routes config)
    ;; Returns a handler function: (request) -> response
    ;; This will be wired to (std net fiber-httpd) route system.
    (let ([conn (server-config-conn config)])
      (lambda (method path body)
        (cond
          ;; Health check
          [(and (eq? method 'GET) (string=? path "/health"))
           (make-response 200 "ok")]

          ;; Database stats
          [(and (eq? method 'GET) (string=? path "/api/db/stats"))
           (make-response 200 (format "~s" (db-stats conn)))]

          ;; Schema
          [(and (eq? method 'GET) (string=? path "/api/db/schema"))
           (make-response 200
             (format "~s" (map (lambda (attr)
                                (list (db-attribute-ident attr)
                                      (db-attribute-value-type attr)
                                      (db-attribute-cardinality attr)))
                              (schema-for conn))))]

          ;; Transact
          [(and (eq? method 'POST) (string=? path "/api/transact"))
           (guard (exn [else (make-response 400 (format "Error: ~a" exn))])
             (let* ([tx-ops (read-body body)]
                    [report (transact! conn tx-ops)])
               (make-response 200
                 (format "~s" `((tx-id . ,(db-value-basis-tx
                                            (tx-report-db-after report)))
                                (datom-count . ,(length (tx-report-tx-data report)))
                                (tempids . ,(tx-report-tempids report)))))))]

          ;; Query
          [(and (eq? method 'POST) (string=? path "/api/query"))
           (guard (exn [else (make-response 400 (format "Error: ~a" exn))])
             (let* ([form (read-body body)]
                    [results (q form (db conn))])
               (make-response 200 (format "~s" results))))]

          ;; Pull
          [(and (eq? method 'POST) (string=? path "/api/pull"))
           (guard (exn [else (make-response 400 (format "Error: ~a" exn))])
             (let* ([params (read-body body)]
                    [pattern (car params)]
                    [eid (cadr params)]
                    [result (pull (db conn) pattern eid)])
               (make-response 200 (format "~s" result))))]

          ;; 404
          [else (make-response 404 "Not found")]))))

  ;; ---- Response helper ----

  (define (make-response status body)
    (list status body))

  (define (read-body body)
    (if (string? body)
        (let ([port (open-input-string body)])
          (read port))
        body))

  ;; ---- Server lifecycle ----
  ;; Will use (std net fiber-httpd) for actual HTTP serving.

  (define (start-server config)
    ;; Placeholder: actual implementation uses fiber-httpd
    (let ([handler (build-routes config)])
      (display (format "Jerboa-DB server starting on ~a:~a\n"
                       (server-config-host config)
                       (server-config-port config)))
      ;; Return a server handle for stop-server
      (list 'server config handler)))

  (define (stop-server handle)
    (display "Jerboa-DB server stopped\n")
    (void))

) ;; end library
