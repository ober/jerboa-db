#!chezscheme
;;; (jerboa-db server) — HTTP/WebSocket API server
;;;
;;; Exposes Jerboa-DB over HTTP for multi-client access.
;;; Built on (std net fiber-httpd) + (std net fiber-ws).
;;;
;;; Routes:
;;;   GET  /health                       — liveness probe
;;;   GET  /api/dbs                      — list registered database names
;;;   GET  /api/db/stats                 — stats for the default connection
;;;   GET  /api/db/schema                — schema for the default connection
;;;   POST /api/transact                 — transact against the default connection
;;;   POST /api/query                    — query against the default connection
;;;   POST /api/pull                     — pull against the default connection
;;;   GET  /api/entity/:eid              — entity lookup on the default connection
;;;   GET  /api/tx-stream                — WebSocket tx-stream (all databases)
;;;   GET  /api/db/:name/stats           — stats for named database
;;;   GET  /api/db/:name/schema          — schema for named database
;;;   POST /api/db/:name/transact        — transact against named database
;;;   POST /api/db/:name/query           — query against named database
;;;   POST /api/db/:name/pull            — pull against named database
;;;   GET  /api/db/:name/entity/:eid     — entity lookup on named database

(library (jerboa-db server)
  (export
    start-server stop-server
    new-server-config server-config?
    register-db! unregister-db! lookup-db)

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
          (jerboa prelude)
          (std net fiber-httpd)
          (std net fiber-ws)
          (std text edn)
          (jerboa-db datom)
          (jerboa-db schema)
          (jerboa-db history)
          (jerboa-db core))

  ;; ---- Server configuration ----

  (defstruct server-config
    (port   ;; integer (default 8484)
     host   ;; string (default "0.0.0.0")
     conn)) ;; jerboa-db connection (primary / "default" database)

  (def (new-server-config conn . opts)
    (let ([port (if (and (pair? opts) (number? (car opts))) (car opts) 8484)]
          [host (if (and (pair? opts) (pair? (cdr opts)) (string? (cadr opts)))
                    (cadr opts)
                    "0.0.0.0")])
      (make-server-config port host conn)))

  ;; ---- Database registry ----
  ;;
  ;; Maps db-name (string) → connection.  Thread-safe via *registry-mutex*.
  ;; The primary connection passed to start-server is registered as "default".

  (def *db-registry* (make-hash-table))
  (def *registry-mutex* (make-mutex))

  (def (register-db! name conn)
    (with-mutex *registry-mutex*
      (hash-put! *db-registry* name conn)))

  (def (unregister-db! name)
    (with-mutex *registry-mutex*
      (hash-remove! *db-registry* name)))

  (def (lookup-db name)
    (with-mutex *registry-mutex*
      (hash-get *db-registry* name)))

  (def (list-db-names)
    (with-mutex *registry-mutex*
      (hash-keys *db-registry*)))

  ;; ---- WebSocket client registry ----
  ;; A list of connected fiber-ws clients for the tx-stream endpoint.
  ;; Protected by a mutex so multiple fibers can mutate it safely.

  (def ws-clients '())
  (def ws-mutex (make-mutex))

  (def (ws-add-client! ws)
    (with-mutex ws-mutex
      (set! ws-clients (cons ws ws-clients))))

  (def (ws-remove-client! ws)
    (with-mutex ws-mutex
      (set! ws-clients (filter (lambda (c) (not (eq? c ws))) ws-clients))))

  (def (ws-broadcast! msg)
    (let ([snapshot
           (with-mutex ws-mutex (list-copy ws-clients))])
      (for-each
        (lambda (ws)
          (guard (exn [#t (ws-remove-client! ws)])
            (when (fiber-ws-open? ws)
              (fiber-ws-send ws msg))))
        snapshot)))

  ;; ---- EDN helpers ----

  (def (respond-edn status obj)
    (respond status
      '(("Content-Type" . "application/edn"))
      (edn->string obj)))

  (def (parse-edn-body req)
    (let ([body (request-body req)])
      (if (and body (string? body) (> (string-length body) 0))
          (string->edn body)
          #f)))

  ;; ---- Route handlers ----

  (def (handle-health req)
    (respond-text 200 "ok"))

  (def (handle-list-dbs req)
    (let ([names (list-db-names)])
      (respond-edn 200 names)))

  (def (handle-db-stats conn req)
    (respond-edn 200 (db-stats conn)))

  (def (handle-db-schema conn req)
    (let ([attrs (schema-for conn)])
      (respond-edn 200
        (map (lambda (attr)
               (list (db-attribute-ident attr)
                     (db-attribute-value-type attr)
                     (db-attribute-cardinality attr)))
             attrs))))

  (def (handle-transact conn req)
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

  (def (handle-query conn req)
    (guard (exn [#t (respond-edn 400 (list 'error
                                      (if (message-condition? exn)
                                          (condition-message exn)
                                          (format "~a" exn))))])
      (let ([form (parse-edn-body req)])
        (unless form
          (error 'query "empty or invalid EDN body"))
        (let ([results (q form (db conn))])
          (respond-edn 200 results)))))

  (def (handle-pull conn req)
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

  (def (handle-entity conn eid-str req)
    (guard (exn [#t (respond-edn 400 (list 'error
                                      (if (message-condition? exn)
                                          (condition-message exn)
                                          (format "~a" exn))))])
      (let ([eid (string->number eid-str)])
        (unless eid
          (error 'entity "eid must be a number" eid-str))
        (let ([result (pull (db conn) '[*] eid)])
          (respond-edn 200 result)))))

  ;; ---- Named-database handler helpers ----
  ;;
  ;; These routes look up the named database from the registry and
  ;; delegate to the same handlers as the default routes.

  (def (handle-named-db-stats name req)
    (let ([conn (lookup-db name)])
      (if conn
          (handle-db-stats conn req)
          (respond-edn 404 (list 'error (str "unknown database: " name))))))

  (def (handle-named-db-schema name req)
    (let ([conn (lookup-db name)])
      (if conn
          (handle-db-schema conn req)
          (respond-edn 404 (list 'error (str "unknown database: " name))))))

  (def (handle-named-transact name req)
    (let ([conn (lookup-db name)])
      (if conn
          (handle-transact conn req)
          (respond-edn 404 (list 'error (str "unknown database: " name))))))

  (def (handle-named-query name req)
    (let ([conn (lookup-db name)])
      (if conn
          (handle-query conn req)
          (respond-edn 404 (list 'error (str "unknown database: " name))))))

  (def (handle-named-pull name req)
    (let ([conn (lookup-db name)])
      (if conn
          (handle-pull conn req)
          (respond-edn 404 (list 'error (str "unknown database: " name))))))

  (def (handle-named-entity name eid-str req)
    (let ([conn (lookup-db name)])
      (if conn
          (handle-entity conn eid-str req)
          (respond-edn 404 (list 'error (str "unknown database: " name))))))

  ;; WebSocket tx-stream handler.
  ;; Returned as the handler for make-websocket-response.
  ;; fiber-httpd performs the handshake and calls this with (fd poller req).
  ;; The fiber loops reading from the client (to detect disconnect),
  ;; while ws-broadcast! pushes transaction events to all clients.
  (def (handle-tx-stream-ws fd poller req)
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

  (def (build-router conn)
    (let ([r (make-router)])
      ;; Health + metadata
      (route-get  r "/health"
        (lambda (req) (handle-health req)))
      (route-get  r "/api/dbs"
        (lambda (req) (handle-list-dbs req)))

      ;; Default-database routes
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

      ;; Named-database routes: /api/db/:name/<verb>
      (route-get  r "/api/db/:name/stats"
        (lambda (req)
          (handle-named-db-stats (route-param req "name") req)))
      (route-get  r "/api/db/:name/schema"
        (lambda (req)
          (handle-named-db-schema (route-param req "name") req)))
      (route-post r "/api/db/:name/transact"
        (lambda (req)
          (handle-named-transact (route-param req "name") req)))
      (route-post r "/api/db/:name/query"
        (lambda (req)
          (handle-named-query (route-param req "name") req)))
      (route-post r "/api/db/:name/pull"
        (lambda (req)
          (handle-named-pull (route-param req "name") req)))
      (route-get  r "/api/db/:name/entity/:eid"
        (lambda (req)
          (handle-named-entity (route-param req "name")
                               (route-param req "eid")
                               req)))

      ;; WebSocket tx-stream: returns a websocket-response so fiber-httpd
      ;; performs the handshake and hands off (fd poller req) to the handler.
      (route-get  r "/api/tx-stream"
        (lambda (req)
          (make-websocket-response handle-tx-stream-ws)))
      r))

  ;; ---- Server lifecycle ----

  (def (start-server config)
    (let* ([conn    (server-config-conn config)]
           [port    (server-config-port config)]
           [_       (register-db! "default" conn)]
           [router  (build-router conn)]
           [handler (lambda (req) (router-dispatch router req))]
           [srv     (fiber-httpd-start port handler)])
      (list 'server config srv)))

  (def (stop-server handle)
    (let ([srv (caddr handle)])
      (fiber-httpd-stop! srv)))

) ;; end library
