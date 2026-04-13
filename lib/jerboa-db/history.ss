#!chezscheme
;;; (jerboa-db history) — Database values, time-travel, and filtered views
;;;
;;; A db-value is an immutable snapshot. Queries always run against a db-value,
;;; never against the live connection. This enables:
;;;   - as-of: see the database at a past point in time
;;;   - since: see only datoms added after a point
;;;   - history: see all datoms including retracted ones

(library (jerboa-db history)
  (export
    make-db-value db-value?
    db-value-basis-tx db-value-indices db-value-schema
    db-value-as-of-tx db-value-since-tx db-value-history?

    ;; Time-travel constructors
    as-of since history

    ;; Filtered datom access
    db-datoms-eavt db-datoms-aevt db-datoms-avet db-datoms-vaet
    db-resolve-index db-filter-datom?)

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
          (jerboa-db datom)
          (jerboa-db index protocol))

  ;; ---- Database value (immutable snapshot) ----

  (defstruct db-value
    (basis-tx    ;; integer: the latest tx included in this snapshot
     indices     ;; index-set (EAVT/AEVT/AVET/VAET)
     schema      ;; schema-registry at this point in time
     as-of-tx    ;; #f for current, or tx-id for time-travel
     since-tx    ;; #f for current, or tx-id for since filter
     history?))  ;; #t to include retracted datoms

  ;; ---- Time-travel constructors ----

  ;; Return db as it was at transaction tx-id (datoms with tx <= tx-id)
  (def (as-of db tx-id)
    (make-db-value
      (db-value-basis-tx db)
      (db-value-indices db)
      (db-value-schema db)
      tx-id
      (db-value-since-tx db)
      (db-value-history? db)))

  ;; Return db showing only datoms added after tx-id
  (def (since db tx-id)
    (make-db-value
      (db-value-basis-tx db)
      (db-value-indices db)
      (db-value-schema db)
      (db-value-as-of-tx db)
      tx-id
      (db-value-history? db)))

  ;; Return db showing all datoms including retracted ones
  (def (history db)
    (make-db-value
      (db-value-basis-tx db)
      (db-value-indices db)
      (db-value-schema db)
      (db-value-as-of-tx db)
      (db-value-since-tx db)
      #t))

  ;; ---- Datom filtering ----

  ;; Check if a datom should be visible in this db-value.
  ;; This applies only temporal filters (as-of, since). It does NOT
  ;; filter by added? — current-state resolution (keeping only values
  ;; whose latest datom is an assertion) is handled by consumers
  ;; (query engine, pull, entity) since they need to see both assertions
  ;; and retractions to determine which values are current.
  (def (db-filter-datom? db d)
    (let ([tx (datom-tx d)]
          [as-of (db-value-as-of-tx db)]
          [since-tx (db-value-since-tx db)])
      (and
        ;; as-of filter: only datoms with tx <= as-of-tx
        (or (not as-of) (<= tx as-of))
        ;; since filter: only datoms with tx > since-tx
        (or (not since-tx) (> tx since-tx)))))

  ;; ---- Index access with filtering ----

  (def (db-resolve-index db index-name)
    (let ([idxs (db-value-indices db)])
      (case index-name
        [(eavt) (index-set-eavt idxs)]
        [(aevt) (index-set-aevt idxs)]
        [(avet) (index-set-avet idxs)]
        [(vaet) (index-set-vaet idxs)]
        [else (error 'db-resolve-index "unknown index" index-name)])))

  (def (filtered-datoms db index-name start end)
    (let ([idx (db-resolve-index db index-name)])
      (filter (lambda (d) (db-filter-datom? db d))
              (dbi-range idx start end))))

  (def (db-datoms-eavt db e-start e-end)
    (filtered-datoms db 'eavt e-start e-end))

  (def (db-datoms-aevt db a-start a-end)
    (filtered-datoms db 'aevt a-start a-end))

  (def (db-datoms-avet db a-start a-end)
    (filtered-datoms db 'avet a-start a-end))

  (def (db-datoms-vaet db v-start v-end)
    (filtered-datoms db 'vaet v-start v-end))

) ;; end library
