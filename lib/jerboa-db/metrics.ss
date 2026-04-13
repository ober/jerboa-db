#!chezscheme
;;; (jerboa-db metrics) — Prometheus-style Metrics
;;;
;;; Tracks operational counters and timings for Jerboa-DB.
;;; Provides both a structured snapshot (alist) and Prometheus exposition format.

(library (jerboa-db metrics)
  (export
    ;; Metric recording
    record-tx-metric!
    record-query-metric!

    ;; Snapshot and export
    metrics-snapshot
    metrics-prometheus

    ;; Reset
    metrics-reset!)

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
          (jerboa-db cache)
          (jerboa-db index protocol)
          (jerboa-db history)
          (jerboa-db core))

  ;; ---- Internal state ----
  ;; All metrics are mutable cells.

  ;; Transaction metrics
  (def tx-total           (list 0))   ;; (mutable integer)
  (def tx-duration-min-ns (list #f))  ;; #f until first record
  (def tx-duration-max-ns (list 0))
  (def tx-duration-sum-ns (list 0))

  ;; Query metrics
  (def query-total           (list 0))
  (def query-duration-min-ns (list #f))
  (def query-duration-max-ns (list 0))
  (def query-duration-sum-ns (list 0))

  ;; Helpers for mutable cell access
  (def (cell-get c) (car c))
  (def (cell-set! c v) (set-car! c v))

  ;; ---- record-tx-metric! ----
  ;; duration-ns: transaction duration in nanoseconds (positive integer)

  (def (record-tx-metric! duration-ns)
    (cell-set! tx-total (+ (cell-get tx-total) 1))
    (cell-set! tx-duration-sum-ns (+ (cell-get tx-duration-sum-ns) duration-ns))
    (let ([cur-min (cell-get tx-duration-min-ns)])
      (when (or (not cur-min) (< duration-ns cur-min))
        (cell-set! tx-duration-min-ns duration-ns)))
    (when (> duration-ns (cell-get tx-duration-max-ns))
      (cell-set! tx-duration-max-ns duration-ns)))

  ;; ---- record-query-metric! ----
  ;; duration-ns: query duration in nanoseconds (positive integer)

  (def (record-query-metric! duration-ns)
    (cell-set! query-total (+ (cell-get query-total) 1))
    (cell-set! query-duration-sum-ns (+ (cell-get query-duration-sum-ns) duration-ns))
    (let ([cur-min (cell-get query-duration-min-ns)])
      (when (or (not cur-min) (< duration-ns cur-min))
        (cell-set! query-duration-min-ns duration-ns)))
    (when (> duration-ns (cell-get query-duration-max-ns))
      (cell-set! query-duration-max-ns duration-ns)))

  ;; ---- metrics-snapshot ----
  ;; Returns an alist of all current metric values.
  ;; conn is optional: if provided, live index counts and cache ratio are included.

  (def (metrics-snapshot . args)
    (let* ([conn (and (pair? args) (car args))]
           [tx-count  (cell-get tx-total)]
           [q-count   (cell-get query-total)]
           [tx-avg    (if (> tx-count 0)
                          (/ (cell-get tx-duration-sum-ns) tx-count)
                          0)]
           [q-avg     (if (> q-count 0)
                          (/ (cell-get query-duration-sum-ns) q-count)
                          0)]
           [base
            `((transactions-total             . ,tx-count)
              (tx-duration-min-ns             . ,(or (cell-get tx-duration-min-ns) 0))
              (tx-duration-max-ns             . ,(cell-get tx-duration-max-ns))
              (tx-duration-avg-ns             . ,tx-avg)
              (queries-total                  . ,q-count)
              (query-duration-min-ns          . ,(or (cell-get query-duration-min-ns) 0))
              (query-duration-max-ns          . ,(cell-get query-duration-max-ns))
              (query-duration-avg-ns          . ,q-avg))])
      ;; Augment with live DB stats if connection provided
      (if conn
          (let* ([stats    (db-stats conn)]
                 [eavt     (cdr (assq 'eavt-count stats))]
                 [cache    (cdr (assq 'cache stats))]
                 [hit-rate (cdr (assq 'hit-rate cache))])
            (append base
                    `((datoms-total   . ,eavt)
                      (cache-hit-ratio . ,hit-rate))))
          base)))

  ;; ---- metrics-prometheus ----
  ;; Returns a string in Prometheus exposition format.
  ;; conn is optional (same as metrics-snapshot).

  (def (metrics-prometheus . args)
    (let* ([snap (apply metrics-snapshot args)]
           [lines '()])
      (def (emit! name help type value)
        (set! lines
              (cons (string-append
                      "# HELP jerboa_db_" name " " help "\n"
                      "# TYPE jerboa_db_" name " " type "\n"
                      "jerboa_db_" name " " (number->string
                                               (if (rational? value)
                                                   (inexact value)
                                                   value))
                      "\n")
                    lines)))
      (def (get key) (let ([p (assq key snap)]) (if p (cdr p) 0)))
      (emit! "transactions_total"
             "Total number of transactions processed"
             "counter"
             (get 'transactions-total))
      (emit! "transaction_duration_seconds_min"
             "Minimum transaction duration in seconds"
             "gauge"
             (/ (get 'tx-duration-min-ns) 1e9))
      (emit! "transaction_duration_seconds_max"
             "Maximum transaction duration in seconds"
             "gauge"
             (/ (get 'tx-duration-max-ns) 1e9))
      (emit! "transaction_duration_seconds_avg"
             "Average transaction duration in seconds"
             "gauge"
             (/ (get 'tx-duration-avg-ns) 1e9))
      (emit! "queries_total"
             "Total number of queries processed"
             "counter"
             (get 'queries-total))
      (emit! "query_duration_seconds_min"
             "Minimum query duration in seconds"
             "gauge"
             (/ (get 'query-duration-min-ns) 1e9))
      (emit! "query_duration_seconds_max"
             "Maximum query duration in seconds"
             "gauge"
             (/ (get 'query-duration-max-ns) 1e9))
      (emit! "query_duration_seconds_avg"
             "Average query duration in seconds"
             "gauge"
             (/ (get 'query-duration-avg-ns) 1e9))
      ;; Optional: datom/cache metrics when conn was provided
      (when (assq 'datoms-total snap)
        (emit! "datoms_total"
               "Total number of datoms in EAVT index"
               "gauge"
               (get 'datoms-total)))
      (when (assq 'cache-hit-ratio snap)
        (emit! "cache_hit_ratio"
               "Entity cache hit ratio (0.0-1.0)"
               "gauge"
               (get 'cache-hit-ratio)))
      (apply string-append (reverse lines))))

  ;; ---- metrics-reset! ----
  ;; Reset all counters (useful between test runs).

  (def (metrics-reset!)
    (cell-set! tx-total 0)
    (cell-set! tx-duration-min-ns #f)
    (cell-set! tx-duration-max-ns 0)
    (cell-set! tx-duration-sum-ns 0)
    (cell-set! query-total 0)
    (cell-set! query-duration-min-ns #f)
    (cell-set! query-duration-max-ns 0)
    (cell-set! query-duration-sum-ns 0))

) ;; end library
