#!chezscheme
;;; (jerboa-db stats) — Per-attribute datom count statistics
;;;
;;; Maintains a live count of current datoms per attribute, updated
;;; incrementally on every transaction.  Consumed by the query planner
;;; to order clauses by actual selectivity.

(library (jerboa-db stats)
  (export
    make-db-stats db-stats?
    db-stats-update!
    db-stats-attr-count
    db-stats-total-datoms
    db-stats-selectivity-score)

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
          (jerboa-db datom))

  ;; ---- Stats record ----
  ;; attr-counts: eq-hashtable mapping attribute-id (integer) -> current datom count
  ;; total: running total of all current datoms across all attributes

  (define-record-type db-stats-rec
    (fields (mutable attr-counts)
            (mutable total))
    (protocol (lambda (new) (lambda () (new (make-eq-hashtable) 0)))))

  ;; Constructor alias
  (def (make-db-stats) (make-db-stats-rec))
  (def (db-stats? x) (db-stats-rec? x))

  ;; ---- Update from a transaction's datom list ----
  ;; Called after each transact! with the list of new datoms (tx-data).
  ;; Adjusts counts: added?=#t -> increment, added?=#f -> decrement.

  (def (db-stats-update! stats tx-datoms)
    (for-each
      (lambda (d)
        (let* ([aid   (datom-a d)]
               [delta (if (datom-added? d) 1 -1)]
               [old   (hashtable-ref (db-stats-rec-attr-counts stats) aid 0)]
               [new   (max 0 (+ old delta))])
          (hashtable-set! (db-stats-rec-attr-counts stats) aid new)
          (db-stats-rec-total-set! stats
            (max 0 (+ (db-stats-rec-total stats) delta)))))
      tx-datoms))

  ;; ---- Query helpers ----

  (def (db-stats-attr-count stats attr-id)
    (hashtable-ref (db-stats-rec-attr-counts stats) attr-id 0))

  (def (db-stats-total-datoms stats)
    (db-stats-rec-total stats))

  ;; Returns a selectivity score 0-900 for an attribute:
  ;; lower count -> higher score (more selective -> should come first in plan).
  ;; Returns 500 for unknown attributes (neutral heuristic).
  (def (db-stats-selectivity-score stats attr-id)
    (let ([n     (db-stats-attr-count stats attr-id)]
          [total (db-stats-total-datoms stats)])
      (cond
        [(= total 0) 500]   ;; no data yet
        [(= n 0)     900]   ;; attribute has no datoms -- very selective (or missing)
        [else
         ;; Score inversely proportional to fraction of total datoms
         ;; n/total = 1.0 -> score 0 (not selective at all)
         ;; n/total -> 0  -> score 900 (very selective)
         (let ([frac (/ n total)])
           (inexact->exact (round (* 900 (- 1.0 (min 1.0 (inexact frac)))))))])))

) ;; end library
