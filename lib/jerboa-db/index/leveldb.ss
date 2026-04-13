#!chezscheme
;;; (jerboa-db index leveldb) — LevelDB-backed persistent index
;;;
;;; Each covering index (EAVT, AEVT, AVET, VAET) is stored in a
;;; separate LevelDB database directory. Keys are 28-byte encoded
;;; datom keys (from encoding.sls) that sort correctly via bytewise
;;; comparison. Values are FASL-encoded datom records.

(library (jerboa-db index leveldb)
  (export make-leveldb-index-set close-leveldb-index-set)

  (import (except (chezscheme)
                  make-hash-table hash-table?
                  sort sort!
                  printf fprintf
                  path-extension path-absolute?
                  with-input-from-string with-output-to-string
                  iota 1+ 1-
                  partition
                  make-date make-time)
          (jerboa prelude)
          (jerboa-db datom)
          (jerboa-db encoding)
          (jerboa-db index protocol)
          (std db leveldb))

  ;; ---- FASL encoding for datom values ----
  ;; We store the full datom as the LevelDB value so we can
  ;; reconstruct it without a separate value store.

  (def (datom->fasl-bytevector d)
    (let-values ([(port extract) (open-bytevector-output-port)])
      (fasl-write (datom->list d) port)
      (extract)))

  (def (fasl-bytevector->datom bv)
    (let* ([port (open-bytevector-input-port bv)]
           [lst (fasl-read port)])
      (apply make-datom lst)))

  ;; ---- Key encoding per index ----

  (def (datom-value-hash d)
    (content-hash-bytes (datom-v d)))

  (def (encode-key-for index-name d)
    (let ([vh (datom-value-hash d)])
      (case index-name
        [(eavt) (encode-eavt-key (datom-e d) (datom-a d) vh
                                  (datom-tx d) (datom-added? d))]
        [(aevt) (encode-aevt-key (datom-a d) (datom-e d) vh
                                  (datom-tx d) (datom-added? d))]
        [(avet) (encode-avet-key (datom-a d) vh (datom-e d)
                                  (datom-tx d) (datom-added? d))]
        [(vaet) (encode-vaet-key vh (datom-a d) (datom-e d)
                                  (datom-tx d) (datom-added? d))])))

  ;; ---- Boundary keys for range scans ----
  ;; Build min/max 28-byte keys from partial datom components.

  (def (make-zero-8) (make-bytevector 8 0))
  (def (make-max-8)  (make-bytevector 8 #xFF))
  (def (make-zero-4) (make-bytevector 4 0))
  (def (make-max-4)  (make-bytevector 4 #xFF))

  (def (range-lo-key index-name d)
    (let ([e (datom-e d)]
          [a (datom-a d)]
          [v (datom-v d)]
          [tx (datom-tx d)])
      (let ([e-bv (encode-eid e)]
            [a-bv (encode-aid a)]
            [vh   (if (sentinel? v) (if (sentinel-min? v) (make-zero-8) (make-max-8))
                      (content-hash-bytes v))]
            [tx-bv (encode-tx+op tx #t)])
        (case index-name
          [(eavt) (bv-concat e-bv a-bv vh tx-bv)]
          [(aevt) (bv-concat a-bv e-bv vh tx-bv)]
          [(avet) (bv-concat a-bv vh e-bv tx-bv)]
          [(vaet) (bv-concat vh a-bv e-bv tx-bv)]))))

  (def (range-hi-key index-name d)
    (let ([e (datom-e d)]
          [a (datom-a d)]
          [v (datom-v d)]
          [tx (datom-tx d)])
      (let ([e-bv (encode-eid e)]
            [a-bv (encode-aid a)]
            [vh   (if (sentinel? v) (if (sentinel-min? v) (make-zero-8) (make-max-8))
                      (content-hash-bytes v))]
            [tx-bv (encode-tx+op tx #t)])
        (case index-name
          [(eavt) (bv-concat e-bv a-bv vh tx-bv)]
          [(aevt) (bv-concat a-bv e-bv vh tx-bv)]
          [(avet) (bv-concat a-bv vh e-bv tx-bv)]
          [(vaet) (bv-concat vh a-bv e-bv tx-bv)]))))

  (def (bv-concat . bvs)
    (let* ([total (apply + (map bytevector-length bvs))]
           [out (make-bytevector total 0)])
      (let loop ([bvs bvs] [off 0])
        (if (null? bvs)
            out
            (let ([bv (car bvs)] [len (bytevector-length (car bvs))])
              (bytevector-copy! bv 0 out off len)
              (loop (cdr bvs) (+ off len)))))))

  ;; Bytevector comparison (for range termination)
  (def (bytevector<=? a b)
    (let ([alen (bytevector-length a)]
          [blen (bytevector-length b)])
      (let loop ([i 0])
        (cond
          [(and (= i alen) (= i blen)) #t]  ;; equal
          [(= i alen) #t]   ;; a shorter = a < b
          [(= i blen) #f]   ;; b shorter = a > b
          [(< (bytevector-u8-ref a i) (bytevector-u8-ref b i)) #t]
          [(> (bytevector-u8-ref a i) (bytevector-u8-ref b i)) #f]
          [else (loop (+ i 1))]))))

  ;; ---- Single LevelDB index ----

  (def (make-leveldb-index name db-handle)
    ;; db-handle: an open leveldb instance for this specific index

    (def (add! datom)
      (let ([key (encode-key-for name datom)]
            [val (datom->fasl-bytevector datom)])
        (leveldb-put db-handle key val)))

    (def (remove! datom)
      (let ([key (encode-key-for name datom)])
        (leveldb-delete db-handle key)))

    (def (range-query start end)
      ;; Scan from lo-key to hi-key using iterator
      (let ([lo (range-lo-key name start)]
            [hi (range-hi-key name end)])
        (leveldb-fold db-handle
          (lambda (key val acc)
            (cons (fasl-bytevector->datom val) acc))
          '()
          lo hi)))

    (def (seek . components)
      ;; Prefix-scan with given components
      ;; For now, delegate to range-query
      (error 'leveldb-index-seek "use range-query instead"))

    (def (count-range start end)
      (let ([lo (range-lo-key name start)]
            [hi (range-hi-key name end)])
        (leveldb-fold-keys db-handle
          (lambda (key acc) (+ acc 1))
          0
          lo hi)))

    (def (snapshot)
      (leveldb-snapshot db-handle))

    (def (all-datoms)
      ;; Full table scan — expensive, use sparingly
      (reverse
        (leveldb-fold db-handle
          (lambda (key val acc)
            (cons (fasl-bytevector->datom val) acc))
          '())))

    (make-dbi name add! remove! range-query seek count-range snapshot all-datoms))

  ;; ---- Create the four covering indices ----

  ;; Returns: (values index-set db-handles-list)
  ;; Caller must pass db-handles-list to close-leveldb-index-set on shutdown.
  (def (make-leveldb-index-set data-path)
    (let ([opts (leveldb-options 'create-if-missing #t
                                 'compression #t
                                 'lru-cache-capacity (* 64 1024 1024)
                                 'bloom-filter-bits 10)])
      (let ([eavt-db (leveldb-open (string-append data-path "/eavt") opts)]
            [aevt-db (leveldb-open (string-append data-path "/aevt") opts)]
            [avet-db (leveldb-open (string-append data-path "/avet") opts)]
            [vaet-db (leveldb-open (string-append data-path "/vaet") opts)])
        (values
          (make-index-set
            (make-leveldb-index 'eavt eavt-db)
            (make-leveldb-index 'aevt aevt-db)
            (make-leveldb-index 'avet avet-db)
            (make-leveldb-index 'vaet vaet-db))
          (list eavt-db aevt-db avet-db vaet-db)))))

  ;; Close all four LevelDB databases.
  ;; Accepts the list of db-handles returned as second value from make-leveldb-index-set.
  (def (close-leveldb-index-set handles)
    (for-each leveldb-close handles))

) ;; end library
