#!chezscheme
;;; (jerboa-db excision) — GDPR Excision
;;;
;;; Permanently removes datoms from ALL indices.
;;; Unlike retraction (which adds a retraction datom), excision physically
;;; deletes matching datoms so they cannot be retrieved even via history queries.
;;;
;;; WARNING: This is a destructive, irreversible operation.
;;; Use only for GDPR/legal compliance requirements.

(library (jerboa-db excision)
  (export
    excise!
    excise-entity!
    excise-attribute!)

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
          (jerboa-db schema)
          (jerboa-db index protocol)
          (jerboa-db history)
          (jerboa-db core))

  ;; ---- Internal: remove datom from all four indices ----

  (def (remove-from-all-indices! indices datom)
    (dbi-remove! (index-set-eavt indices) datom)
    (dbi-remove! (index-set-aevt indices) datom)
    ;; AVET and VAET only contain datoms for indexed/ref attributes,
    ;; but calling remove! on a non-existent datom is safe (no-op).
    (dbi-remove! (index-set-avet indices) datom)
    (dbi-remove! (index-set-vaet indices) datom))

  ;; ---- Internal: find all datoms matching a predicate in EAVT ----

  (def (scan-eavt-matching indices pred)
    ;; Scan all datoms in EAVT and collect those matching pred.
    ;; Returns a list of datoms (both assertions and retractions).
    (filter pred (dbi-datoms (index-set-eavt indices))))

  ;; ---- excise! ----
  ;; spec: alist with optional keys:
  ;;   (entity   . eid)         — match this entity only
  ;;   (attribute . attr-ident) — match this attribute only
  ;;   (before-tx . tx-id)      — match datoms with tx < tx-id only
  ;;
  ;; All matching datoms are physically removed from ALL indices.

  (def (excise! conn excision-spec)
    (let* ([current-db (db conn)]
           [indices    (db-value-indices current-db)]
           [schema     (db-value-schema current-db)]
           [eid-filter  (let ([p (assq 'entity excision-spec)])
                          (and p (cdr p)))]
           [attr-filter (let ([p (assq 'attribute excision-spec)])
                          (and p (cdr p)))]
           [tx-filter   (let ([p (assq 'before-tx excision-spec)])
                          (and p (cdr p)))]
           ;; Resolve attribute ident to id if provided
           [aid-filter  (and attr-filter
                             (let ([attr (schema-lookup-by-ident schema attr-filter)])
                               (and attr (db-attribute-id attr))))])
      ;; Validate attribute ident
      (when (and attr-filter (not aid-filter))
        (error 'excise! "Unknown attribute in excision spec" attr-filter))
      ;; Build predicate and collect matching datoms from EAVT scan
      (let ([victims (scan-eavt-matching
                       indices
                       (lambda (d)
                         (and (or (not eid-filter)  (= (datom-e d) eid-filter))
                              (or (not aid-filter)  (= (datom-a d) aid-filter))
                              (or (not tx-filter)   (< (datom-tx d) tx-filter)))))])
        ;; Remove each from all four indices
        (for-each
          (lambda (d) (remove-from-all-indices! indices d))
          victims)
        ;; Return count of excised datoms
        (length victims))))

  ;; ---- excise-entity! ----
  ;; Remove all datoms for eid from all indices.

  (def (excise-entity! conn eid)
    (excise! conn `((entity . ,eid))))

  ;; ---- excise-attribute! ----
  ;; Remove all datoms for (eid, attr-ident) from all indices.

  (def (excise-attribute! conn eid attr-ident)
    (excise! conn `((entity . ,eid) (attribute . ,attr-ident))))

) ;; end library
