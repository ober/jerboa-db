#!chezscheme
;;; (jerboa-db log) — Transaction log navigation API
;;;
;;; Provides log-handle, a navigable wrapper over a connection's tx-log list,
;;; matching Datomic's (d/log conn) API.
;;;
;;; Note: (log conn) is defined in (jerboa-db core) which wraps this library.
;;; This library operates on a tx-report list; core.ss extracts it from the conn.

(library (jerboa-db log)
  (export make-log-handle log-handle? log-handle-tx-log
          log? log-tx-range log-tx-list)

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
          (jerboa-db tx)
          (jerboa-db history))

  (defstruct log-handle (tx-log))

  (def (log? x) (log-handle? x))

  ;; Return all tx-reports as a list (most recent first)
  (def (log-tx-list lh)
    (log-handle-tx-log lh))

  ;; (log-tx-range lh start end) — returns tx-reports whose tx-id is in [start, end]
  ;; start/end: tx integer IDs, or #f for open-ended
  (def (log-tx-range lh start end)
    (filter
      (lambda (report)
        (let ([tx-id (db-value-basis-tx (tx-report-db-after report))])
          (and (or (not start) (>= tx-id start))
               (or (not end)   (<= tx-id end)))))
      (log-tx-list lh)))

) ;; end library
