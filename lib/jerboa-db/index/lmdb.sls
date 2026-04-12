#!chezscheme
;;; (jerboa-db index lmdb) — LMDB-backed index backend
;;;
;;; Implements the index protocol using LMDB for persistent, crash-safe storage.
;;; Keys are 28-byte bytevectors with bytewise comparison = datom order.
;;; Values are full datom data encoded with FASL.

(library (jerboa-db index lmdb)
  (export make-lmdb-index-set close-lmdb-index-set)

  (import (chezscheme)
          (jerboa-db datom)
          (jerboa-db encoding)
          (jerboa-db index protocol)
          (thunderchez lmdb))

  ;; ---- LMDB environment setup ----

  (define +map-size+ (* 1024 1024 1024))  ;; 1 GB default
  (define +max-dbs+ 10)

  (define (open-lmdb-env path)
    (mdb-library-init)
    (unless (file-exists? path) (mkdir path))
    (let ([env* (mdb-alloc-env*)])
      (let ([env (mdb-env-create env*)])
        (when (not (= env MDB_SUCCESS))
          (error 'open-lmdb-env "Failed to create LMDB env" (mdb-strerror env)))
        (let ([env (foreign-ref 'void* env* 0)])
          (mdb-env-set-mapsize env +map-size+)
          (mdb-env-set-maxdbs env +max-dbs+)
          (let ([rc (mdb-env-open env path 0 #o664)])
            (when (not (= rc MDB_SUCCESS))
              (error 'open-lmdb-env "Failed to open LMDB env" (mdb-strerror rc))))
          env))))

  ;; ---- Open a named database ----

  (define (open-named-db env name)
    (let ([txn* (mdb-alloc-txn*)]
          [dbi (mdb-alloc-dbi)])
      (let ([rc (mdb-txn-begin env (mdb-null-txn) 0 txn*)])
        (when (not (= rc MDB_SUCCESS))
          (error 'open-named-db "Failed to begin txn" (mdb-strerror rc)))
        (let ([txn (foreign-ref 'void* txn* 0)])
          (let ([rc2 (mdb-dbi-open txn name MDB_CREATE dbi)])
            (when (not (= rc2 MDB_SUCCESS))
              (mdb-txn-abort txn)
              (error 'open-named-db "Failed to open dbi" (mdb-strerror rc2)))
            (let ([db-handle (foreign-ref 'unsigned-32 dbi 0)])
              (mdb-txn-commit txn)
              db-handle))))))

  ;; ---- Create a single LMDB-backed index ----

  (define (make-lmdb-index env dbi-handle name encode-key-fn decode-key-fn)

    (define (add! datom)
      (let ([key-bv (encode-key-fn datom)]
            [val-bv (datom->bytevector datom)])
        (let ([txn* (mdb-alloc-txn*)])
          (let ([rc (mdb-txn-begin env (mdb-null-txn) 0 txn*)])
            (when (= rc MDB_SUCCESS)
              (let ([txn (foreign-ref 'void* txn* 0)]
                    [k (make-mdb-val key-bv)]
                    [v (make-mdb-val val-bv)])
                (let ([rc2 (mdb-put txn dbi-handle k v 0)])
                  (if (= rc2 MDB_SUCCESS)
                      (mdb-txn-commit txn)
                      (mdb-txn-abort txn)))))))))

    (define (remove! datom)
      (let ([key-bv (encode-key-fn datom)])
        (let ([txn* (mdb-alloc-txn*)])
          (let ([rc (mdb-txn-begin env (mdb-null-txn) 0 txn*)])
            (when (= rc MDB_SUCCESS)
              (let ([txn (foreign-ref 'void* txn* 0)]
                    [k (make-mdb-val key-bv)])
                (mdb-del txn dbi-handle k (mdb-null-val))
                (mdb-txn-commit txn)))))))

    (define (range-query start end)
      (let ([start-key (encode-key-fn start)]
            [end-key (encode-key-fn end)]
            [results '()])
        (let ([txn* (mdb-alloc-txn*)])
          (let ([rc (mdb-txn-begin env (mdb-null-txn) MDB_RDONLY txn*)])
            (when (= rc MDB_SUCCESS)
              (let ([txn (foreign-ref 'void* txn* 0)])
                (let ([cursor* (foreign-alloc (foreign-sizeof 'void*))])
                  (let ([rc2 (mdb-cursor-open txn dbi-handle cursor*)])
                    (when (= rc2 MDB_SUCCESS)
                      (let ([cursor (foreign-ref 'void* cursor* 0)]
                            [k (make-mdb-val start-key)]
                            [v (mdb-alloc-val)])
                        ;; Seek to start
                        (let ([rc3 (mdb-cursor-get cursor k v 'set-range)])
                          (when (= rc3 MDB_SUCCESS)
                            (let loop ()
                              (let ([key-data (mdb-val->bytevector k)]
                                    [val-data (mdb-val->bytevector v)])
                                (when (bytevector<=? key-data end-key)
                                  (set! results
                                    (cons (bytevector->datom val-data) results))
                                  (let ([rc4 (mdb-cursor-get cursor k v 'next)])
                                    (when (= rc4 MDB_SUCCESS)
                                      (loop))))))))
                        (mdb-cursor-close cursor)))
                    (foreign-free cursor*)))
                (mdb-txn-abort txn)))))
        (reverse results)))

    (define (seek . components)
      ;; For LMDB, seek builds range from components (like memory backend)
      (range-query (apply make-datom (append components (list #t)))
                   (apply make-datom (append components (list #t)))))

    (define (count-range start end)
      (length (range-query start end)))

    (define (snapshot) dbi-handle)

    (define (all-datoms)
      (range-query
        (make-datom 0 0 +min-val+ 0 #t)
        (make-datom (greatest-fixnum) (greatest-fixnum) +max-val+
                    (greatest-fixnum) #t)))

    (make-dbi name add! remove! range-query seek count-range snapshot all-datoms))

  ;; ---- Datom serialization for LMDB values ----

  (define (datom->bytevector d)
    (let ([port (open-output-bytevector)])
      (fasl-write (vector (datom-e d) (datom-a d) (datom-v d)
                          (datom-tx d) (datom-added? d))
                  port)
      (get-output-bytevector port)))

  (define (bytevector->datom bv)
    (let* ([port (open-input-bytevector bv)]
           [v (fasl-read port)])
      (make-datom (vector-ref v 0) (vector-ref v 1) (vector-ref v 2)
                  (vector-ref v 3) (vector-ref v 4))))

  ;; ---- Bytevector comparison ----

  (define (bytevector<=? a b)
    (let ([la (bytevector-length a)] [lb (bytevector-length b)])
      (let loop ([i 0])
        (cond
          [(and (= i la) (= i lb)) #t]  ;; equal
          [(= i la) #t]                   ;; a shorter = a < b
          [(= i lb) #f]                   ;; b shorter = a > b
          [else
           (let ([ba (bytevector-u8-ref a i)] [bb (bytevector-u8-ref b i)])
             (cond [(< ba bb) #t]
                   [(> ba bb) #f]
                   [else (loop (+ i 1))]))]))))

  ;; ---- Key encoders per index ----

  (define (eavt-key-encoder d)
    (encode-eavt-key (datom-e d) (datom-a d)
                     (encode-value-hash (datom-v d))
                     (datom-tx d) (datom-added? d)))

  (define (aevt-key-encoder d)
    (encode-aevt-key (datom-a d) (datom-e d)
                     (encode-value-hash (datom-v d))
                     (datom-tx d) (datom-added? d)))

  (define (avet-key-encoder d)
    (encode-avet-key (datom-a d) (encode-value-hash (datom-v d))
                     (datom-e d) (datom-tx d) (datom-added? d)))

  (define (vaet-key-encoder d)
    (encode-vaet-key (encode-value-hash (datom-v d))
                     (datom-a d) (datom-e d)
                     (datom-tx d) (datom-added? d)))

  ;; ---- Create all four indices ----

  (define (make-lmdb-index-set path)
    (let ([env (open-lmdb-env path)])
      (let ([eavt-dbi (open-named-db env "eavt")]
            [aevt-dbi (open-named-db env "aevt")]
            [avet-dbi (open-named-db env "avet")]
            [vaet-dbi (open-named-db env "vaet")])
        (make-index-set
          (make-lmdb-index env eavt-dbi 'eavt eavt-key-encoder #f)
          (make-lmdb-index env aevt-dbi 'aevt aevt-key-encoder #f)
          (make-lmdb-index env avet-dbi 'avet avet-key-encoder #f)
          (make-lmdb-index env vaet-dbi 'vaet vaet-key-encoder #f)))))

  (define (close-lmdb-index-set idx-set)
    ;; LMDB environments are closed by the GC guardian, but explicit close
    ;; is good practice.
    (void))

) ;; end library
