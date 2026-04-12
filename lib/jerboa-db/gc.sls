#!chezscheme
;;; (jerboa-db gc) — Datom garbage collection
;;;
;;; Removes retracted datom pairs from indices to reclaim space.
;;; For attributes marked :db/noHistory, retracted datoms serve no purpose
;;; once retraction is confirmed. gc-collect-db! removes both the assertion
;;; and retraction datoms for such attribute/entity combos.
;;;
;;; The transaction log is NOT modified — GC only affects indices.
;;;
;;; Usage: call gc-collect-db! with a db-value. Wrap in core.sls:
;;;   (define (gc-collect! conn . opts) (apply gc-collect-db! (db conn) opts))

(library (jerboa-db gc)
  (export gc-collect-db! gc-stats-db)

  (import (chezscheme)
          (jerboa-db datom)
          (jerboa-db schema)
          (jerboa-db index protocol)
          (jerboa-db history))

  ;; ---- gc-collect-db! ----
  ;; Takes a db-value. Scans EAVT, groups by (e,a,v).
  ;; For each group where highest-tx is a retraction and attribute is noHistory
  ;; (or collect-all? #t), removes all datoms in the group from all indices.

  (define (gc-collect-db! db-val . opts)
    (let* ([collect-all? (and (pair? opts) (car opts))]
           [schema   (db-value-schema db-val)]
           [indices  (db-value-indices db-val)]
           [eavt     (index-set-eavt indices)]
           [aevt     (index-set-aevt indices)]
           [avet     (index-set-avet indices)]
           [vaet     (index-set-vaet indices)]
           [examined 0]
           [removed  0]
           [t0       (time-second (current-time))])

      ;; Group all EAVT datoms by (e, a, v)
      (let ([groups (make-hashtable equal-hash equal?)])
        (for-each
          (lambda (d)
            (set! examined (+ examined 1))
            (let ([key (list (datom-e d) (datom-a d) (datom-v d))])
              (hashtable-set! groups key
                (cons d (hashtable-ref groups key '())))))
          (dbi-datoms eavt))

        ;; Process each group
        (let-values ([(keys datom-lists) (hashtable-entries groups)])
          (vector-for-each
            (lambda (key datoms)
              (let* ([attr-id  (cadr key)]
                     [attr     (schema-lookup-by-id schema attr-id)]
                     [no-hist? (and attr (db-attribute-no-history? attr))]
                     [is-ref?  (and attr (ref-type? attr))]
                     [is-idx?  (and attr (indexed-attr? attr))])
                (when (or no-hist? collect-all?)
                  ;; Find highest-tx datom
                  (let* ([sorted (sort (lambda (a b) (> (datom-tx a) (datom-tx b))) datoms)]
                         [top    (car sorted)])
                    ;; GC only if the current state is retracted
                    (when (not (datom-added? top))
                      (for-each
                        (lambda (d)
                          (set! removed (+ removed 1))
                          (dbi-remove! eavt d)
                          (dbi-remove! aevt d)
                          (when is-idx? (dbi-remove! avet d))
                          (when is-ref? (dbi-remove! vaet d)))
                        datoms))))))
            keys datom-lists)))

      (let ([t1 (time-second (current-time))])
        (list (cons 'examined examined)
              (cons 'removed removed)
              (cons 'duration-ms (* 1000 (- t1 t0)))))))

  ;; ---- gc-stats-db ----
  ;; Count reclaimable datoms without removing them.

  (define (gc-stats-db db-val . opts)
    (let* ([collect-all? (and (pair? opts) (car opts))]
           [schema  (db-value-schema db-val)]
           [indices (db-value-indices db-val)]
           [eavt    (index-set-eavt indices)]
           [total   0]
           [reclaimable 0])

      (let ([groups (make-hashtable equal-hash equal?)])
        (for-each
          (lambda (d)
            (set! total (+ total 1))
            (let ([key (list (datom-e d) (datom-a d) (datom-v d))])
              (hashtable-set! groups key
                (cons d (hashtable-ref groups key '())))))
          (dbi-datoms eavt))

        (let-values ([(keys datom-lists) (hashtable-entries groups)])
          (vector-for-each
            (lambda (key datoms)
              (let* ([attr-id (cadr key)]
                     [attr    (schema-lookup-by-id schema attr-id)]
                     [no-hist? (and attr (db-attribute-no-history? attr))]
                     [sorted (sort (lambda (a b) (> (datom-tx a) (datom-tx b))) datoms)]
                     [top    (car sorted)])
                (when (and (not (datom-added? top)) (or no-hist? collect-all?))
                  (set! reclaimable (+ reclaimable (length datoms))))))
            keys datom-lists)))

      (list (cons 'total-datoms total)
            (cons 'reclaimable reclaimable))))

) ;; end library
