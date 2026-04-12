#!chezscheme
;;; (jerboa-db server) — HTTP/WebSocket API server
;;;
;;; Exposes Jerboa-DB over HTTP for multi-client access.
;;; Built on (std net fiber-httpd) + (std net fiber-ws).

(library (jerboa-db server)
  (export
    start-server stop-server
    new-server-config server-config?)

  (import (chezscheme)
          (std net fiber-httpd)
          (std net fiber-ws)
          (std text edn)
          (jerboa-db datom)
          (jerboa-db schema)
          (jerboa-db history)
          (jerboa-db core))

  ;; ---- Server configuration ----

  (define-record-type server-config
    (fields port       ;; integer (default 8484)
            host       ;; string (default "0.0.0.0")
            conn))     ;; jerboa-db connection

  (define (new-server-config conn . opts)
    (let ([port (if (and (pair? opts) (number? (car opts))) (car opts) 8484)]
          [host (if (and (pair? opts) (pair? (cdr opts)) (string? (cadr opts)))
                    (cadr opts)
                    "0.0.0.0")])
      (make-server-config port host conn)))

  ;; ---- WebSocket client registry ----
  ;; A list of connected fiber-ws clients for the tx-stream endpoint.
  ;; Protected by a mutex so multiple fibers can mutate it safely.

  (define ws-clients '())
  (define ws-mutex (make-mutex))

  (define (ws-add-client! ws)
    (with-mutex ws-mutex
      (set! ws-clients (cons ws ws-clients))))

  (define (ws-remove-client! ws)
    (with-mutex ws-mutex
      (set! ws-clients (filter (lambda (c) (not (eq? c ws))) ws-clients))))

  (define (ws-broadcast! msg)
    (let ([snapshot
           (with-mutex ws-mutex (list-copy ws-clients))])
      (for-each
        (lambda (ws)
          (guard (exn [#t (ws-remove-client! ws)])
            (when (fiber-ws-open? ws)
              (fiber-ws-send ws msg))))
        snapshot)))

  ;; ---- EDN helpers ----

  (define (respond-edn status obj)
    (respond status
      '(("Content-Type" . "application/edn"))
      (edn->string obj)))

  (define (parse-edn-body req)
    (let ([body (request-body req)])
      (if (and body (string? body) (> (string-length body) 0))
          (string->edn body)
          #f)))

  (define (error->edn-string exn)
    (edn->string
      `(error ,(if (message-condition? exn)
                   (condition-message exn)
                   (format "~a" exn)))))

  ;; ---- Route handlers ----

  (define (handle-health req)
    (respond-text 200 "ok"))

  (define (handle-db-stats conn req)
    (respond-edn 200 (db-stats conn)))

  (define (handle-db-schema conn req)
    (let ([attrs (schema-for conn)])
      (respond-edn 200
        (map (lambda (attr)
               (list (db-attribute-ident attr)
                     (db-attribute-value-type attr)
                     (db-attribute-cardinality attr)))
             attrs))))

  (define (handle-transact conn req)
    (guard (exn [#t (respond-edn 400 (list 'error
                                      (if (message-condition? exn)
                                          (condition-message exn)
                                          (format "~a" exn))))])
      (let ([tx-ops (parse-edn-body req)])
        (unless tx-ops
          (error 'transact "empty or invalid EDN body"))
        (let ([report (transact! conn tx-ops)])
          ;; Notify tx-stream clients
          (let ([tx-data
                 (list
                   (cons 'tx-id (db-value-basis-tx (tx-report-db-after report)))
                   (cons 'datom-count (length (tx-report-tx-data report)))
                   (cons 'tempids (tx-report-tempids report)))])
            (ws-broadcast! (edn->string tx-data))
            (respond-edn 200 tx-data))))))

  (define (handle-query conn req)
    (guard (exn [#t (respond-edn 400 (list 'error
                                      (if (message-condition? exn)
                                          (condition-message exn)
                                          (format "~a" exn))))])
      (let ([form (parse-edn-body req)])
        (unless form
          (error 'query "empty or invalid EDN body"))
        (let ([results (q form (db conn))])
          (respond-edn 200 results)))))

  (define (handle-pull conn req)
    (guard (exn [#t (respond-edn 400 (list 'error
                                      (if (message-condition? exn)
                                          (condition-message exn)
                                          (format "~a" exn))))])
      (let ([params (parse-edn-body req)])
        (unless (and params (pair? params) (pair? (cdr params)))
          (error 'pull "body must be EDN list of [pattern eid]"))
        (let* ([pattern (car params)]
               [eid (cadr params)]
               [result (pull (db conn) pattern eid)])
          (respond-edn 200 result)))))

  (define (handle-entity conn eid-str req)
    (guard (exn [#t (respond-edn 400 (list 'error
                                      (if (message-condition? exn)
                                          (condition-message exn)
                                          (format "~a" exn))))])
      (let ([eid (string->number eid-str)])
        (unless eid
          (error 'entity "eid must be a number" eid-str))
        (let ([result (pull (db conn) '[*] eid)])
          (respond-edn 200 result)))))

  ;; WebSocket tx-stream handler.
  ;; Registered as the handler for make-websocket-response.
  ;; The fiber loops reading from the client (to detect disconnect),
  ;; while ws-broadcast! pushes transaction events to all clients.
  (define (handle-tx-stream-ws fd poller req)
    (let ([ws (fiber-ws-upgrade (request-headers req) fd poller)])
      (when ws
        (ws-add-client! ws)
        ;; Send a "connected" message
        (guard (exn [#t (void)])
          (fiber-ws-send ws (edn->string '(connected))))
        ;; Loop: park reading until client disconnects or sends something
        (let loop ()
          (guard (exn [#t (void)])  ;; connection error = drop client
            (let ([msg (fiber-ws-recv ws)])
              (when msg (loop)))))
        ;; Client gone
        (ws-remove-client! ws)
        (guard (exn [#t (void)])
          (fiber-ws-close ws)))))

  ;; ---- Router construction ----

  (define (build-router conn)
    (let ([r (make-router)])
      (route-get  r "/health"
        (lambda (req) (handle-health req)))
      (route-get  r "/api/db/stats"
        (lambda (req) (handle-db-stats conn req)))
      (route-get  r "/api/db/schema"
        (lambda (req) (handle-db-schema conn req)))
      (route-post r "/api/transact"
        (lambda (req) (handle-transact conn req)))
      (route-post r "/api/query"
        (lambda (req) (handle-query conn req)))
      (route-post r "/api/pull"
        (lambda (req) (handle-pull conn req)))
      (route-get  r "/api/entity/:eid"
        (lambda (req)
          (let ([eid-str (route-param req "eid")])
            (handle-entity conn eid-str req))))
      ;; WebSocket tx-stream: returns a websocket-response so fiber-httpd
      ;; performs the handshake and hands off (fd poller req) to the handler.
      (route-get  r "/api/tx-stream"
        (lambda (req)
          (make-websocket-response handle-tx-stream-ws)))
      r))

  ;; ---- Server lifecycle ----

  (define (start-server config)
    (let* ([conn   (server-config-conn config)]
           [port   (server-config-port config)]
           [router (build-router conn)]
           [handler (lambda (req) (router-dispatch router req))]
           [srv   (fiber-httpd-start port handler)])
      (list 'server config srv)))

  (define (stop-server handle)
    (let ([srv (caddr handle)])
      (fiber-httpd-stop! srv)))

) ;; end library
