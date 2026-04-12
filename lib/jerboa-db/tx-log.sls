#!chezscheme
;;; (jerboa-db tx-log) — Append-only transaction log
;;;
;;; The transaction log is the source of truth. Indices can be rebuilt from it.
;;; Stored as segment files using FASL encoding, with optional gzip compression.

(library (jerboa-db tx-log)
  (export
    new-tx-log tx-log?
    tx-log-append! tx-log-replay tx-log-range
    tx-log-entry? tx-log-entry-tx-id tx-log-entry-instant tx-log-entry-datoms
    tx-log-count tx-log-latest-tx)

  (import (chezscheme)
          (jerboa-db datom))

  ;; ---- Transaction log entry ----

  (define-record-type tx-log-entry
    (fields tx-id      ;; monotonic transaction ID
            instant    ;; epoch seconds (integer)
            datoms))   ;; list of datom field vectors: #(e a v tx added?)

  ;; ---- Transaction log ----
  ;; In-memory: a simple reversed list of entries.
  ;; Persistent: segment files on disk.

  (define-record-type tx-log
    (fields (mutable entries)       ;; list of tx-log-entry (most recent first)
            (mutable segment-dir)   ;; directory for segment files (or #f for in-memory)
            (mutable segment-size)  ;; max entries per segment
            (mutable current-segment-count))) ;; entries in current segment

  (define (new-tx-log . opts)
    ;; Optional: (new-tx-log dir segment-size)
    (let ([dir (if (and (pair? opts) (string? (car opts))) (car opts) #f)]
          [seg-size (if (and (pair? opts) (pair? (cdr opts))) (cadr opts) 10000)])
      (make-tx-log '() dir seg-size 0)))

  ;; ---- Append ----

  (define (tx-log-append! log tx-id instant datoms)
    (let ([entry (make-tx-log-entry tx-id instant
                   (map datom->serializable datoms))])
      ;; Prepend to in-memory list
      (tx-log-entries-set! log (cons entry (tx-log-entries log)))
      (tx-log-current-segment-count-set! log
        (+ (tx-log-current-segment-count log) 1))
      ;; Write to disk if persistent
      (when (tx-log-segment-dir log)
        (write-entry-to-segment! log entry)
        ;; Rotate segment if needed
        (when (>= (tx-log-current-segment-count log)
                   (tx-log-segment-size log))
          (rotate-segment! log)))
      entry))

  (define (datom->serializable d)
    (vector (datom-e d) (datom-a d) (datom-v d) (datom-tx d) (datom-added? d)))

  (define (serializable->datom v)
    (make-datom (vector-ref v 0) (vector-ref v 1) (vector-ref v 2)
                (vector-ref v 3) (vector-ref v 4)))

  ;; ---- Replay ----

  (define (tx-log-replay log from-tx index-fn)
    ;; Replay all entries from from-tx forward, calling index-fn for each datom.
    (let ([entries (reverse (tx-log-entries log))])
      (for-each
        (lambda (entry)
          (when (>= (tx-log-entry-tx-id entry) from-tx)
            (for-each
              (lambda (dv)
                (index-fn (serializable->datom dv)))
              (tx-log-entry-datoms entry))))
        entries)))

  ;; ---- Range query ----

  (define (tx-log-range log start-tx end-tx)
    ;; Return entries in [start-tx, end-tx)
    (filter (lambda (e)
              (let ([tx (tx-log-entry-tx-id e)])
                (and (>= tx start-tx) (< tx end-tx))))
            (reverse (tx-log-entries log))))

  ;; ---- Stats ----

  (define (tx-log-count log)
    (length (tx-log-entries log)))

  (define (tx-log-latest-tx log)
    (if (null? (tx-log-entries log))
        0
        (tx-log-entry-tx-id (car (tx-log-entries log)))))

  ;; ---- Segment file I/O ----

  (define (write-entry-to-segment! log entry)
    (let* ([dir (tx-log-segment-dir log)]
           [segment-num (quotient (tx-log-count log)
                                   (tx-log-segment-size log))]
           [path (string-append dir "/segment-"
                                (number->string segment-num) ".fasl")])
      ;; Ensure directory exists
      (unless (file-exists? dir)
        (mkdir dir))
      ;; Append entry as FASL to segment file
      (let ([port (open-file-output-port path
                    (file-options no-fail no-truncate)
                    (buffer-mode block) #f)])
        (set-port-position! port (port-length port))
        (fasl-write
          (vector (tx-log-entry-tx-id entry)
                  (tx-log-entry-instant entry)
                  (tx-log-entry-datoms entry))
          port)
        (close-port port))))

  (define (rotate-segment! log)
    (tx-log-current-segment-count-set! log 0))

  ;; ---- Load from disk ----

  (define (tx-log-load-segments dir)
    ;; Scan directory for segment files, read and merge
    (let ([log (new-tx-log dir 10000)])
      (when (file-exists? dir)
        (let ([files (sort string<?
                       (filter (lambda (f) (string-suffix? f ".fasl"))
                               (directory-list dir)))])
          (for-each
            (lambda (file)
              (let ([path (string-append dir "/" file)])
                (load-segment-file! log path)))
            files)))
      log))

  (define (load-segment-file! log path)
    (let ([port (open-file-input-port path (file-options) (buffer-mode block) #f)])
      (let loop ()
        (guard (exn [else (void)])
          (let ([v (fasl-read port)])
            (when v
              (let ([entry (make-tx-log-entry
                             (vector-ref v 0)
                             (vector-ref v 1)
                             (vector-ref v 2))])
                (tx-log-entries-set! log
                  (cons entry (tx-log-entries log)))
                (loop))))))
      (close-port port)))

  (define (string-suffix? s suffix)
    (let ([sl (string-length s)] [xl (string-length suffix)])
      (and (>= sl xl)
           (string=? (substring s (- sl xl) sl) suffix))))

) ;; end library
