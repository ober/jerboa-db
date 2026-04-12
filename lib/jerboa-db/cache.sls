#!chezscheme
;;; (jerboa-db cache) — LRU datom/entity cache
;;;
;;; Wraps an LRU cache for hot datoms and entity maps.
;;; Cache invalidation is trivial because data is immutable —
;;; new transactions only add entries, never modify existing ones.

(library (jerboa-db cache)
  (export
    new-db-cache db-cache?
    cache-get cache-put! cache-clear! cache-stats
    cache-get-entity cache-put-entity!)

  (import (chezscheme))

  ;; Simple LRU cache using a hashtable + doubly-linked list (mutable pairs).
  ;; We inline this to avoid depending on (std misc lru-cache) at library level.

  (define-record-type db-cache
    (fields (mutable capacity)
            (mutable datom-ht)    ;; eq-hashtable: key -> value
            (mutable datom-order) ;; list of keys in access order (MRU first)
            (mutable entity-ht)
            (mutable entity-order)
            (mutable hits)
            (mutable misses)))

  (define (new-db-cache capacity)
    (make-db-cache capacity
      (make-hashtable equal-hash equal?) '()
      (make-hashtable equal-hash equal?) '()
      0 0))

  ;; ---- Generic cache ops over a ht + order pair ----

  (define (lru-get cache ht order-getter order-setter! key default)
    (let ([v (hashtable-ref ht key 'NOT-FOUND)])
      (if (eq? v 'NOT-FOUND)
          (begin
            (db-cache-misses-set! cache (+ (db-cache-misses cache) 1))
            default)
          (begin
            (db-cache-hits-set! cache (+ (db-cache-hits cache) 1))
            ;; Move to front
            (order-setter! cache (cons key (remq key (order-getter cache))))
            v))))

  (define (lru-put! cache ht order-getter order-setter! key value)
    (let ([cap (db-cache-capacity cache)])
      (hashtable-set! ht key value)
      (let ([new-order (cons key (remq key (order-getter cache)))])
        ;; Evict if over capacity
        (when (> (length new-order) cap)
          (let ([victim (list-ref new-order (- (length new-order) 1))])
            (hashtable-delete! ht victim)
            (set! new-order (reverse (cdr (reverse new-order))))))
        (order-setter! cache new-order))))

  ;; ---- Datom cache ----

  (define (cache-get cache key default)
    (lru-get cache (db-cache-datom-ht cache)
             db-cache-datom-order db-cache-datom-order-set!
             key default))

  (define (cache-put! cache key value)
    (lru-put! cache (db-cache-datom-ht cache)
              db-cache-datom-order db-cache-datom-order-set!
              key value))

  ;; ---- Entity cache ----

  (define (cache-get-entity cache eid default)
    (lru-get cache (db-cache-entity-ht cache)
             db-cache-entity-order db-cache-entity-order-set!
             eid default))

  (define (cache-put-entity! cache eid entity)
    (lru-put! cache (db-cache-entity-ht cache)
              db-cache-entity-order db-cache-entity-order-set!
              eid entity))

  ;; ---- Maintenance ----

  (define (cache-clear! cache)
    (hashtable-clear! (db-cache-datom-ht cache))
    (db-cache-datom-order-set! cache '())
    (hashtable-clear! (db-cache-entity-ht cache))
    (db-cache-entity-order-set! cache '())
    (db-cache-hits-set! cache 0)
    (db-cache-misses-set! cache 0))

  (define (cache-stats cache)
    (let ([hits (db-cache-hits cache)]
          [misses (db-cache-misses cache)])
      `((hits . ,hits)
        (misses . ,misses)
        (hit-rate . ,(if (= (+ hits misses) 0) 0.0
                         (inexact (/ hits (+ hits misses)))))
        (datom-size . ,(hashtable-size (db-cache-datom-ht cache)))
        (entity-size . ,(hashtable-size (db-cache-entity-ht cache))))))

) ;; end library
