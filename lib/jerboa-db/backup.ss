#!chezscheme
;;; (jerboa-db backup) — Backup & Restore
;;;
;;; Snapshots the entire database (all four indices + schema + metadata)
;;; to a file using FASL encoding with optional gzip compression.
;;; Restoration creates a fresh in-memory connection and replays all datoms.

(library (jerboa-db backup)
  (export backup! restore!)

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
          (jerboa-db index memory)
          (jerboa-db history)
          (jerboa-db core))

  ;; ---- Magic header ----
  ;; First 8 bytes of every backup file.
  (def +backup-magic+ #vu8(74 68 66 75 49 48 48 48))  ;; "JDBU1000"

  ;; ---- Lazy zlib loader ----
  ;; Mirrors the LevelDB lazy loader pattern in core.sls.
  ;; The backup format works with or without zlib:
  ;;   byte 8 = 1 → gzip compressed, 0 → uncompressed
  (def zlib-loaded? #f)
  (def zlib-gzip #f)
  (def zlib-gunzip #f)

  (def (try-load-zlib!)
    (unless zlib-loaded?
      (guard (exn [#t #f])
        (eval '(import (std compress zlib)))
        (set! zlib-gzip   (eval 'gzip-bytevector))
        (set! zlib-gunzip (eval 'gunzip-bytevector))
        (set! zlib-loaded? #t))))

  ;; ---- Schema serialization ----
  ;; Convert schema registry to a plain list for FASL portability.

  (def (schema->plist schema)
    ;; Returns a list of (ident id vtype card unique index? comp? doc no-hist?)
    (map (lambda (attr)
           (list (db-attribute-ident attr)
                 (db-attribute-id attr)
                 (db-attribute-value-type attr)
                 (db-attribute-cardinality attr)
                 (db-attribute-unique attr)
                 (db-attribute-index? attr)
                 (db-attribute-is-component? attr)
                 (db-attribute-doc attr)
                 (db-attribute-no-history? attr)))
         (schema-all-attributes schema)))

  (def (plist->schema plist)
    ;; new-schema-registry bootstraps system attributes (IDs 0..19).
    ;; We then install user attributes from the plist on top of that.
    (let ([reg (new-schema-registry)])
      (for-each
        (lambda (entry)
          (let ([ident   (list-ref entry 0)]
                [id      (list-ref entry 1)]
                [vtype   (list-ref entry 2)]
                [card    (list-ref entry 3)]
                [unique  (list-ref entry 4)]
                [idx?    (list-ref entry 5)]
                [comp?   (list-ref entry 6)]
                [doc     (list-ref entry 7)]
                [no-hist (list-ref entry 8)])
            ;; Only install user attributes; system ones are already bootstrapped
            (when (>= id +first-user-attr-id+)
              (let ([attr (make-db-attribute
                            ident id vtype card unique idx? comp? doc no-hist)])
                (schema-install-attribute! reg attr)))))
        plist)
      reg))

  ;; ---- backup! ----
  ;; Serialize connection snapshot to output-path.
  ;; Format:
  ;;   [8 bytes magic] [1 byte: 0=raw, 1=gzip] [FASL payload]
  ;; FASL payload is a vector: #(basis-tx next-eid schema-plist datoms-list)
  ;; where datoms-list is a list of (e a v tx added?) 5-tuples.

  (def (backup! conn output-path)
    ;; Try to load zlib but proceed without it on failure
    (try-load-zlib!)
    (let* ([current-db (db conn)]
           [indices    (db-value-indices current-db)]
           [schema     (db-value-schema current-db)]
           [basis-tx   (db-value-basis-tx current-db)]
           ;; Read next-eid from connection (it's a list cell: (next-eid))
           ;; We reconstruct it by reading all datoms and taking max eid + 1
           [eavt       (index-set-eavt indices)]
           [all-datoms (dbi-datoms eavt)]
           ;; Serialize datoms as plain lists for FASL
           [datom-list (map datom->list all-datoms)]
           [schema-pl  (schema->plist schema)]
           ;; Compute next-eid from connection internals via db-stats
           [stats      (db-stats conn)]
           [payload    (vector basis-tx
                               ;; Store eavt-count as a marker; restore computes next-eid
                               datom-list
                               schema-pl)])
      ;; FASL-serialize payload to bytevector
      (let-values ([(port get-bv) (open-bytevector-output-port)])
        (fasl-write payload port)
        (let* ([raw-bv   (get-bv)]
               [use-gzip (and zlib-gzip #t)]
               [data-bv  (if use-gzip (zlib-gzip raw-bv) raw-bv)])
          ;; Write binary data: magic + compression flag + payload
          (let ([out (open-file-output-port output-path
                       (file-options no-fail)
                       (buffer-mode block))])
            (put-bytevector out +backup-magic+)
            (put-u8 out (if use-gzip 1 0))
            (put-bytevector out data-bv)
            (close-port out))))))

  ;; ---- restore! ----
  ;; Read a backup file and return a new in-memory connection with all
  ;; data replayed. Does NOT call transact! — datoms are inserted directly
  ;; into the indices to avoid schema validation overhead and to preserve
  ;; original transaction IDs.

  (def (restore! backup-path)
    (try-load-zlib!)
    ;; Read the file
    (let* ([in    (open-file-input-port backup-path
                    (file-options)
                    (buffer-mode block))]
           [magic (get-bytevector-n in 8)]
           [flag  (get-u8 in)]
           [rest  (get-bytevector-all in)])
      (close-port in)
      ;; Validate magic
      (unless (equal? magic +backup-magic+)
        (error 'restore! "Not a valid jerboa-db backup file" backup-path))
      ;; Decompress if needed
      (let* ([raw-bv  (if (= flag 1)
                          (if zlib-gunzip
                              (zlib-gunzip rest)
                              (error 'restore! "Backup is gzip-compressed but zlib is unavailable"))
                          rest)]
             ;; Deserialize FASL
             [port    (open-bytevector-input-port raw-bv)]
             [payload (fasl-read port)])
        (close-port port)
        (let* ([basis-tx   (vector-ref payload 0)]
               [datom-list (vector-ref payload 1)]
               [schema-pl  (vector-ref payload 2)]
               ;; Rebuild schema
               [schema     (plist->schema schema-pl)]
               ;; Create fresh in-memory index set
               [indices    (make-mem-index-set)]
               ;; Replay all datoms directly into indices
               [eavt       (index-set-eavt indices)]
               [aevt       (index-set-aevt indices)]
               [avet       (index-set-avet indices)]
               [vaet       (index-set-vaet indices)])
          ;; Insert each datom into appropriate indices
          (for-each
            (lambda (entry)
              (let* ([e     (list-ref entry 0)]
                     [a     (list-ref entry 1)]
                     [v     (list-ref entry 2)]
                     [tx    (list-ref entry 3)]
                     [added (list-ref entry 4)]
                     [d     (make-datom e a v tx added)])
                ;; Always insert into EAVT and AEVT
                (dbi-add! eavt d)
                (dbi-add! aevt d)
                ;; Insert into AVET if attribute is indexed
                (let ([attr (schema-lookup-by-id schema a)])
                  (when (and attr (indexed-attr? attr))
                    (dbi-add! avet d))
                  ;; Insert into VAET if ref type
                  (when (and attr (ref-type? attr))
                    (dbi-add! vaet d)))))
            datom-list)
          ;; Compute next-eid = max entity id + 1
          (let ([max-eid (fold-left
                           (lambda (acc entry) (max acc (list-ref entry 0)))
                           +first-user-attr-id+
                           datom-list)])
            ;; Build the initial db-value
            (let* ([initial-db (make-db-value basis-tx indices schema #f #f #f #f)]
                   [conn (make-connection
                           initial-db
                           (list (+ max-eid 1))
                           '()
                           (new-db-cache 10000)
                           ":memory:"
                           #f     ;; db-handles
                           #f     ;; fulltext-index
                           #f)])
              conn))))))

) ;; end library
