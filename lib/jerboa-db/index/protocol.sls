#!chezscheme
;;; (jerboa-db index protocol) — Pluggable index backend interface
;;;
;;; Each index backend (memory, LMDB) provides the same operations
;;; via a closure-based record. This avoids coupling to a specific
;;; storage engine.

(library (jerboa-db index protocol)
  (export
    make-dbi dbi?
    dbi-add! dbi-remove! dbi-range dbi-seek dbi-count dbi-snapshot
    dbi-datoms dbi-name

    ;; Index set (all four indices)
    make-index-set index-set?
    index-set-eavt index-set-aevt index-set-avet index-set-vaet)

  (import (chezscheme))

  ;; ---- Index interface (closure-based vtable) ----
  ;; Each field is a procedure implementing that operation.

  (define-record-type dbi
    (fields name         ;; symbol: eavt, aevt, avet, vaet
            add-fn       ;; (datom) -> void
            remove-fn    ;; (datom) -> void
            range-fn     ;; (start-datom end-datom) -> list of datoms
            seek-fn      ;; (components) -> list of datoms
            count-fn     ;; (start-datom end-datom) -> integer
            snapshot-fn  ;; () -> opaque snapshot
            datoms-fn))  ;; () -> list of all datoms

  ;; Dispatch wrappers

  (define (dbi-add! idx datom)
    ((dbi-add-fn idx) datom))

  (define (dbi-remove! idx datom)
    ((dbi-remove-fn idx) datom))

  (define (dbi-range idx start end)
    ((dbi-range-fn idx) start end))

  (define (dbi-seek idx . components)
    (apply (dbi-seek-fn idx) components))

  (define (dbi-count idx start end)
    ((dbi-count-fn idx) start end))

  (define (dbi-snapshot idx)
    ((dbi-snapshot-fn idx)))

  (define (dbi-datoms idx)
    ((dbi-datoms-fn idx)))

  ;; ---- Index set: the four covering indices ----

  (define-record-type index-set
    (fields eavt aevt avet vaet))

) ;; end library
