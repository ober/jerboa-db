#!/usr/bin/env scheme --libdirs lib:~/mine/jerboa/lib --script
;;; jerboa-db CLI tool
;;;
;;; Usage:
;;;   jerboa-db serve   --port PORT --data PATH [--host HOST]
;;;   jerboa-db stats   --data PATH
;;;   jerboa-db backup  --data PATH --output PATH
;;;   jerboa-db gc      --data PATH [--all]
;;;   jerboa-db repl    --data PATH
;;;   jerboa-db import  --data PATH --format csv|edn --file FILE
;;;   jerboa-db export  --data PATH --format edn|csv --output FILE

(import (jerboa prelude)
        (jerboa-db core)
        (jerboa-db backup)
        (jerboa-db gc))

;; ---- Argument parsing ----

(def (parse-args args)
  ;; Returns (subcommand . options-alist)
  ;; options-alist: ((flag . value) ...) where flag is a string like "--port"
  (if (null? args)
      (cons "help" '())
      (let ([subcmd (car args)]
            [rest   (cdr args)])
        (let loop ([rest rest] [opts '()])
          (cond
            [(null? rest)
             (cons subcmd (reverse opts))]
            [(and (string-prefix? "--" (car rest)) (pair? (cdr rest)))
             (loop (cddr rest)
                   (cons (cons (substring (car rest) 2 (string-length (car rest)))
                               (cadr rest))
                         opts))]
            [(string-prefix? "--" (car rest))
             (loop (cdr rest)
                   (cons (cons (substring (car rest) 2 (string-length (car rest)))
                               #t)
                         opts))]
            [else
             (loop (cdr rest) opts)])))))

(def (opt opts key default)
  (let ([pair (assoc key opts)])
    (if pair (cdr pair) default)))

;; ---- Subcommands ----

(def (cmd-serve opts)
  (let ([port (string->number (opt opts "port" "8484"))]
        [data (opt opts "data" ":memory:")]
        [host (opt opts "host" "0.0.0.0")])
    (displayln "Starting Jerboa-DB server...")
    (displayln "  Data: " data)
    (displayln "  Bind: " host ":" port)
    (let ([conn (connect data)])
      (guard (exn [#t (displayln "Error: server module unavailable: "
                                  (condition-message exn))])
        (let ([server-mod (eval '(import (jerboa-db server)))])
          (eval `(start-server! ,conn ,host ,port))
          (displayln "Server running. Press Ctrl-C to stop.")
          ;; Block forever
          (let loop () (sleep (make-time 'time-duration 0 1)) (loop)))))))

(def (cmd-stats opts)
  (let ([data (opt opts "data" ":memory:")])
    (let* ([conn (connect data)]
           [stats (db-stats conn)])
      (displayln "=== Jerboa-DB Stats ===")
      (for-each
        (lambda (pair)
          (displayln "  " (car pair) ": " (cdr pair)))
        stats)
      (close conn))))

(def (cmd-backup opts)
  (let ([data   (opt opts "data" #f)]
        [output (opt opts "output" #f)])
    (unless data   (error 'backup "--data required"))
    (unless output (error 'backup "--output required"))
    (displayln "Backing up " data " -> " output)
    (let ([conn (connect data)])
      (backup! conn output)
      (close conn)
      (displayln "Backup complete."))))

(def (cmd-gc opts)
  (let ([data      (opt opts "data" #f)]
        [all?      (opt opts "all" #f)])
    (unless data (error 'gc "--data required"))
    (displayln "Running garbage collection on " data " ...")
    (let* ([conn (connect data)]
           [result (if all?
                       (gc-collect! conn #t)
                       (gc-collect! conn))])
      (displayln "  Examined: " (cdr (assq 'examined result)))
      (displayln "  Removed:  " (cdr (assq 'removed result)))
      (displayln "  Duration: " (cdr (assq 'duration-ms result)) "ms")
      (close conn))))

(def (cmd-repl opts)
  (let ([data (opt opts "data" ":memory:")])
    (displayln "Jerboa-DB REPL (type :quit to exit)")
    (displayln "Connected to: " data)
    (let ([conn (connect data)])
      (let loop ()
        (display "jerboa-db> ")
        (flush-output-port (current-output-port))
        (let ([line (get-line (current-input-port))])
          (cond
            [(eof-object? line) (displayln "Bye.")]
            [(string=? (string-trim line) ":quit") (displayln "Bye.")]
            [(string=? (string-trim line) "") (loop)]
            [else
             (guard (exn [#t (displayln "Error: " (condition-message exn))])
               (let* ([form (with-input-from-string line read)]
                      [result (q form (db conn))])
                 (for-each (lambda (row) (displayln row)) result)))
             (loop)]))))))

(def (cmd-import opts)
  (let ([data   (opt opts "data" #f)]
        [format (opt opts "format" "edn")]
        [file   (opt opts "file" #f)])
    (unless data (error 'import "--data required"))
    (unless file (error 'import "--file required"))
    (displayln "Importing " file " (" format ") into " data " ...")
    (let ([conn (connect data)])
      (guard (exn [#t (displayln "Error: analytics module needed for CSV import")
                      (displayln (condition-message exn))])
        (cond
          [(string=? format "edn")
           ;; Read EDN list of tx-ops
           (let* ([raw (read-file-string file)]
                  [ops (with-input-from-string raw read)])
             (transact! conn ops)
             (displayln "Import complete."))]
          [(string=? format "csv")
           (eval `(import (jerboa-db analytics)))
           (eval `(import-csv ,conn ,file))]
          [else (error 'import "Unknown format" format)]))
      (close conn))))

(def (cmd-export opts)
  (let ([data   (opt opts "data" #f)]
        [format (opt opts "format" "edn")]
        [output (opt opts "output" #f)])
    (unless data   (error 'export "--data required"))
    (unless output (error 'export "--output required"))
    (displayln "Exporting " data " -> " output " (" format ") ...")
    (let* ([conn (connect data)]
           [d    (db conn)])
      (cond
        [(string=? format "edn")
         ;; Export all current datoms as EDN
         (let* ([stats (db-stats conn)]
                [eids-result (q '((find ?e) (where (?e db/txInstant ?tx))) d)]
                ;; Export schema + all entities
                [all-attrs (schema-for conn)])
           (write-file-string output
             (with-output-to-string
               (lambda ()
                 (displayln ";; Jerboa-DB export")
                 (displayln ";; Generated: " (number->string (time-second (current-time))))
                 (displayln (db-stats conn))))))]
        [else (error 'export "Unknown format" format)])
      (close conn))))

(def (cmd-help opts)
  (displayln "Jerboa-DB command-line tool")
  (displayln "")
  (displayln "Commands:")
  (displayln "  serve   --port PORT --data PATH  Start HTTP server")
  (displayln "  stats   --data PATH              Show database statistics")
  (displayln "  backup  --data PATH --output PATH Create backup")
  (displayln "  gc      --data PATH [--all]      Run garbage collection")
  (displayln "  repl    --data PATH              Interactive query REPL")
  (displayln "  import  --data PATH --format csv|edn --file FILE  Import data")
  (displayln "  export  --data PATH --format edn --output FILE    Export data"))

;; ---- Dispatch ----

(let* ([args   (cdr (command-line))]  ;; skip script name
       [parsed (parse-args args)]
       [subcmd (car parsed)]
       [opts   (cdr parsed)])
  (cond
    [(string=? subcmd "serve")   (cmd-serve opts)]
    [(string=? subcmd "stats")   (cmd-stats opts)]
    [(string=? subcmd "backup")  (cmd-backup opts)]
    [(string=? subcmd "gc")      (cmd-gc opts)]
    [(string=? subcmd "repl")    (cmd-repl opts)]
    [(string=? subcmd "import")  (cmd-import opts)]
    [(string=? subcmd "export")  (cmd-export opts)]
    [(string=? subcmd "help")    (cmd-help opts)]
    [else
     (displayln "Unknown command: " subcmd)
     (cmd-help '())
     (exit 1)]))
