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

  (import (except (chezscheme)
                  make-hash-table hash-table?
                  sort sort!
                  printf fprintf
                  path-extension path-absolute?
                  with-input-from-string with-output-to-string
                  iota 1+ 1-
                  partition
                  make-date make-time)
          (jerboa prelude))

  ;; ---- Index interface (closure-based vtable) ----
  ;; Each field is a procedure implementing that operation.

  (defstruct dbi
    (name         ;; symbol: eavt, aevt, avet, vaet
     add-fn       ;; (datom) -> void
     remove-fn    ;; (datom) -> void
     range-fn     ;; (start-datom end-datom) -> list of datoms
     seek-fn      ;; (components) -> list of datoms
     count-fn     ;; (start-datom end-datom) -> integer
     snapshot-fn  ;; () -> opaque snapshot
     datoms-fn))  ;; () -> list of all datoms

  ;; Dispatch wrappers

  (def (dbi-add! idx datom)
    ((dbi-add-fn idx) datom))

  (def (dbi-remove! idx datom)
    ((dbi-remove-fn idx) datom))

  (def (dbi-range idx start end)
    ((dbi-range-fn idx) start end))

  (def (dbi-seek idx . components)
    (apply (dbi-seek-fn idx) components))

  (def (dbi-count idx start end)
    ((dbi-count-fn idx) start end))

  (def (dbi-snapshot idx)
    ((dbi-snapshot-fn idx)))

  (def (dbi-datoms idx)
    ((dbi-datoms-fn idx)))

  ;; ---- Index set: the four covering indices ----

  (defstruct index-set
    (eavt aevt avet vaet))

) ;; end library
