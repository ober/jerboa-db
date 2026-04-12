#!chezscheme
;;; (jerboa-db value-store) — Content-addressed value store
;;;
;;; Stores large values (strings, bytes) by their FNV-1a hash for
;;; deduplication. When many entities share the same large value, it is
;;; stored once. Index keys hold only the 8-byte hash.
;;;
;;; Currently in-memory; can be upgraded to LevelDB persistence.

(library (jerboa-db value-store)
  (export
    make-value-store value-store?
    value-store-put! value-store-get value-store-has?
    value-store-close value-store-stats)

  (import (chezscheme)
          (jerboa-db encoding))

  ;; ---- Record ----

  (define-record-type vs-rec
    (fields (mutable table)        ;; equal-hash hashtable: bv-hash -> value
            (mutable hit-count)
            (mutable miss-count)
            (mutable dedup-count)))

  (define (value-store? x) (vs-rec? x))

  (define (make-value-store)
    (make-vs-rec (make-hashtable equal-hash equal?) 0 0 0))

  ;; ---- Operations ----

  (define (value-store-put! vs value)
    ;; Returns 8-byte hash bytevector. Deduplicates on second call.
    (let ([hash-bv (content-hash-bytes value)])
      (if (hashtable-contains? (vs-rec-table vs) hash-bv)
          (begin
            (vs-rec-dedup-count-set! vs (+ (vs-rec-dedup-count vs) 1))
            hash-bv)
          (begin
            (hashtable-set! (vs-rec-table vs) hash-bv value)
            hash-bv))))

  (define (value-store-get vs hash-bv)
    (let ([v (hashtable-ref (vs-rec-table vs) hash-bv #f)])
      (if v
          (begin (vs-rec-hit-count-set! vs (+ (vs-rec-hit-count vs) 1)) v)
          (begin (vs-rec-miss-count-set! vs (+ (vs-rec-miss-count vs) 1)) #f))))

  (define (value-store-has? vs hash-bv)
    (hashtable-contains? (vs-rec-table vs) hash-bv))

  (define (value-store-close vs)
    ;; In-memory: nothing to close. Hook for LevelDB backend.
    (void))

  (define (value-store-stats vs)
    (list (cons 'entries    (hashtable-size (vs-rec-table vs)))
          (cons 'hits       (vs-rec-hit-count vs))
          (cons 'misses     (vs-rec-miss-count vs))
          (cons 'dedup-saves (vs-rec-dedup-count vs))))

) ;; end library
