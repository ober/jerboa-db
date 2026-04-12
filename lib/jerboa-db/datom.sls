#!chezscheme
;;; (jerboa-db datom) — Datom record and index comparison functions
;;;
;;; The fundamental unit of data: [entity attribute value transaction added?]
;;; Four covering indices sort datoms in different orders for different access patterns.

(library (jerboa-db datom)
  (export
    ;; Datom record
    make-datom datom? datom-e datom-a datom-v datom-tx datom-added?

    ;; Sentinel values for range query boundaries
    +min-val+ +max-val+ sentinel? sentinel-min? sentinel-max?

    ;; Comparison functions (three-way: -1, 0, 1)
    compare-values compare-datoms-eavt compare-datoms-aevt
    compare-datoms-avet compare-datoms-vaet

    ;; Datom utilities
    datom->list datom-matches?)

  (import (chezscheme))

  ;; ---- Sentinel values for range boundaries ----
  ;; Used to construct probe datoms for index range scans.
  ;; +min-val+ compares less than any real value.
  ;; +max-val+ compares greater than any real value.

  (define-record-type sentinel (fields kind))
  (define +min-val+ (make-sentinel 'min))
  (define +max-val+ (make-sentinel 'max))

  (define (sentinel-min? x) (and (sentinel? x) (eq? (sentinel-kind x) 'min)))
  (define (sentinel-max? x) (and (sentinel? x) (eq? (sentinel-kind x) 'max)))

  ;; ---- Datom record ----

  (define-record-type datom
    (fields e       ;; entity id (integer)
            a       ;; attribute id (integer, interned from keyword)
            v       ;; value (scheme value — type depends on attribute schema)
            tx      ;; transaction id (integer, monotonically increasing)
            added?  ;; #t = assertion, #f = retraction
            ))

  ;; ---- Three-way comparison helpers ----

  (define (compare-int a b)
    (cond [(< a b) -1] [(> a b) 1] [else 0]))

  ;; Generic value comparison. Handles sentinels, then dispatches on type.
  ;; Values of different types are ordered by type tag to ensure total ordering.
  (define (compare-values a b)
    (cond
      ;; Sentinels
      [(sentinel? a)
       (cond [(sentinel-min? a) (if (sentinel-min? b) 0 -1)]
             [else (if (sentinel-max? b) 0 1)])]  ;; max sentinel
      [(sentinel? b)
       (cond [(sentinel-min? b) 1]
             [else -1])]  ;; b is max sentinel
      ;; Same-type comparisons
      [(and (fixnum? a) (fixnum? b)) (compare-int a b)]
      [(and (number? a) (number? b))
       (cond [(< a b) -1] [(> a b) 1] [else 0])]
      [(and (string? a) (string? b))
       (cond [(string<? a b) -1] [(string>? a b) 1] [else 0])]
      [(and (boolean? a) (boolean? b))
       (cond [(eq? a b) 0] [a 1] [else -1])]
      [(and (symbol? a) (symbol? b))
       (let ([sa (symbol->string a)] [sb (symbol->string b)])
         (cond [(string<? sa sb) -1] [(string>? sa sb) 1] [else 0]))]
      [(and (bytevector? a) (bytevector? b))
       (let ([la (bytevector-length a)] [lb (bytevector-length b)])
         (let loop ([i 0])
           (cond
             [(and (= i la) (= i lb)) 0]
             [(= i la) -1]
             [(= i lb) 1]
             [else
              (let ([ba (bytevector-u8-ref a i)] [bb (bytevector-u8-ref b i)])
                (cond [(< ba bb) -1] [(> ba bb) 1]
                      [else (loop (+ i 1))]))])))]
      ;; Cross-type ordering by type tag
      [else
       (let ([ta (type-tag a)] [tb (type-tag b)])
         (compare-int ta tb))]))

  ;; Assign a numeric tag to each value type for total cross-type ordering
  (define (type-tag v)
    (cond
      [(boolean? v)    0]
      [(fixnum? v)     1]
      [(number? v)     2]
      [(string? v)     3]
      [(symbol? v)     4]
      [(bytevector? v) 5]
      [(pair? v)       6]
      [(vector? v)     7]
      [else            8]))

  ;; ---- Cascading comparison ----
  ;; Compares multiple fields in sequence; short-circuits on first non-zero.

  (define-syntax cascade-compare
    (syntax-rules ()
      [(_ e1) e1]
      [(_ e1 e2 ...)
       (let ([r e1])
         (if (= r 0) (cascade-compare e2 ...) r))]))

  ;; ---- Index comparison functions ----
  ;; Each returns a three-way comparator (-1, 0, 1) suitable for sorted-map.

  ;; EAVT: Entity -> Attribute -> Value -> Tx
  ;; "All attributes of entity 42"
  (define (compare-datoms-eavt a b)
    (cascade-compare
      (compare-int (datom-e a) (datom-e b))
      (compare-int (datom-a a) (datom-a b))
      (compare-values (datom-v a) (datom-v b))
      (compare-int (datom-tx a) (datom-tx b))))

  ;; AEVT: Attribute -> Entity -> Value -> Tx
  ;; "All entities with :person/name"
  (define (compare-datoms-aevt a b)
    (cascade-compare
      (compare-int (datom-a a) (datom-a b))
      (compare-int (datom-e a) (datom-e b))
      (compare-values (datom-v a) (datom-v b))
      (compare-int (datom-tx a) (datom-tx b))))

  ;; AVET: Attribute -> Value -> Entity -> Tx
  ;; "Entity where :email = 'a@b.com'" (unique lookup)
  (define (compare-datoms-avet a b)
    (cascade-compare
      (compare-int (datom-a a) (datom-a b))
      (compare-values (datom-v a) (datom-v b))
      (compare-int (datom-e a) (datom-e b))
      (compare-int (datom-tx a) (datom-tx b))))

  ;; VAET: Value -> Attribute -> Entity -> Tx
  ;; "All entities referencing entity 42" (reverse refs)
  (define (compare-datoms-vaet a b)
    (cascade-compare
      (compare-values (datom-v a) (datom-v b))
      (compare-int (datom-a a) (datom-a b))
      (compare-int (datom-e a) (datom-e b))
      (compare-int (datom-tx a) (datom-tx b))))

  ;; ---- Datom utilities ----

  (define (datom->list d)
    (list (datom-e d) (datom-a d) (datom-v d) (datom-tx d) (datom-added? d)))

  ;; Check if a datom matches given components (use #f for "any")
  (define (datom-matches? d e a v tx)
    (and (or (not e) (= (datom-e d) e))
         (or (not a) (= (datom-a d) a))
         (or (not v) (equal? (datom-v d) v))
         (or (not tx) (= (datom-tx d) tx))))

) ;; end library
