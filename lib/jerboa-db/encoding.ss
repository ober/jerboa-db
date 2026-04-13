#!chezscheme
;;; (jerboa-db encoding) — Binary encoding for datom keys and values
;;;
;;; LMDB keys must be bytevectors with correct bytewise sort order.
;;; All integers are big-endian so memcmp gives numeric ordering.
;;; The added? flag is encoded in the high bit of the tx field.

(library (jerboa-db encoding)
  (export
    ;; Integer encoding
    encode-eid decode-eid encode-aid decode-aid
    encode-tx+op decode-tx+op

    ;; Value encoding
    encode-value-inline encode-value-hash
    decode-value-inline

    ;; Full datom key encoding (28 bytes per index)
    encode-eavt-key encode-aevt-key encode-avet-key encode-vaet-key
    decode-eavt-key decode-aevt-key decode-avet-key decode-vaet-key

    ;; Sortable double encoding
    encode-f64-sortable decode-f64-sortable

    ;; Content hashing for variable-length values
    content-hash-bytes)

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

  ;; ---- Entity ID: big-endian unsigned 64-bit ----

  (def (encode-eid eid)
    (let ([bv (make-bytevector 8 0)])
      (bytevector-u64-set! bv 0 eid (endianness big))
      bv))

  (def (decode-eid bv offset)
    (bytevector-u64-ref bv offset (endianness big)))

  ;; ---- Attribute ID: big-endian unsigned 32-bit ----

  (def (encode-aid aid)
    (let ([bv (make-bytevector 4 0)])
      (bytevector-u32-set! bv 0 aid (endianness big))
      bv))

  (def (decode-aid bv offset)
    (bytevector-u32-ref bv offset (endianness big)))

  ;; ---- Transaction ID + added? flag ----
  ;; High bit of 64-bit tx: 0 = assertion, 1 = retraction

  (def +retract-bit+ #x8000000000000000)

  (def (encode-tx+op tx added?)
    (let ([bv (make-bytevector 8 0)]
          [encoded (if added? tx (bitwise-ior tx +retract-bit+))])
      (bytevector-u64-set! bv 0 encoded (endianness big))
      bv))

  (def (decode-tx+op bv offset)
    (let ([raw (bytevector-u64-ref bv offset (endianness big))])
      (if (bitwise-bit-set? raw 63)
          (values (bitwise-and raw (- +retract-bit+ 1)) #f)  ;; retraction
          (values raw #t))))                                   ;; assertion

  ;; ---- Value encoding ----
  ;; Values that fit in 8 bytes are encoded inline.
  ;; Variable-length values are content-hashed to 8 bytes.

  (def (encode-value-inline value type)
    (let ([bv (make-bytevector 8 0)])
      (case type
        [(db.type/long)
         ;; Signed integer: offset by min-fixnum so bytewise order = numeric order
         (bytevector-u64-set! bv 0
           (bitwise-and (+ value #x8000000000000000) #xFFFFFFFFFFFFFFFF)
           (endianness big))]
        [(db.type/double)
         (let ([encoded (encode-f64-sortable value)])
           (bytevector-copy! encoded 0 bv 0 8))]
        [(db.type/boolean)
         (bytevector-u8-set! bv 7 (if value 1 0))]
        [(db.type/instant)
         ;; Epoch nanoseconds or seconds as u64
         (bytevector-u64-set! bv 0 (if (integer? value) value 0) (endianness big))]
        [(db.type/ref)
         (bytevector-u64-set! bv 0 value (endianness big))]
        [else
         ;; Variable-length: use hash
         (bytevector-copy! (content-hash-bytes value) 0 bv 0 8)])
      bv))

  (def (decode-value-inline bv offset type)
    (case type
      [(db.type/long)
       (- (bytevector-u64-ref bv offset (endianness big)) #x8000000000000000)]
      [(db.type/double)
       (let ([sub (make-bytevector 8)])
         (bytevector-copy! bv offset sub 0 8)
         (decode-f64-sortable sub))]
      [(db.type/boolean)
       (= (bytevector-u8-ref bv (+ offset 7)) 1)]
      [(db.type/instant)
       (bytevector-u64-ref bv offset (endianness big))]
      [(db.type/ref)
       (bytevector-u64-ref bv offset (endianness big))]
      [else #f]))  ;; variable-length needs value-store lookup

  (def (encode-value-hash value)
    (content-hash-bytes value))

  ;; ---- Sortable IEEE 754 double encoding ----
  ;; XOR sign bit, flip all bits if negative.
  ;; This makes bytewise comparison equal numeric comparison.

  (def (encode-f64-sortable d)
    (let ([bv (make-bytevector 8)])
      (bytevector-ieee-double-set! bv 0 d (endianness big))
      (let ([high (bytevector-u8-ref bv 0)])
        (if (bitwise-bit-set? high 7)
            ;; Negative: flip all bits
            (let ([out (make-bytevector 8)])
              (do ([i 0 (+ i 1)])
                  ((= i 8) out)
                (bytevector-u8-set! out i
                  (bitwise-xor (bytevector-u8-ref bv i) #xFF))))
            ;; Positive or zero: flip sign bit
            (begin
              (bytevector-u8-set! bv 0 (bitwise-xor high #x80))
              bv)))))

  (def (decode-f64-sortable bv)
    (let ([high (bytevector-u8-ref bv 0)])
      (if (bitwise-bit-set? high 7)
          ;; Was positive: flip sign bit back
          (let ([out (make-bytevector 8)])
            (bytevector-copy! bv 0 out 0 8)
            (bytevector-u8-set! out 0 (bitwise-xor high #x80))
            (bytevector-ieee-double-ref out 0 (endianness big)))
          ;; Was negative: flip all bits back
          (let ([out (make-bytevector 8)])
            (do ([i 0 (+ i 1)])
                ((= i 8))
              (bytevector-u8-set! out i
                (bitwise-xor (bytevector-u8-ref bv i) #xFF)))
            (bytevector-ieee-double-ref out 0 (endianness big))))))

  ;; ---- Content hashing (FNV-1a 64-bit) ----
  ;; Used for variable-length values in index keys.

  (def +fnv-offset+ 14695981039346656037)
  (def +fnv-prime+  1099511628211)

  (def (content-hash-bytes value)
    (let* ([data (cond
                   [(string? value) (string->utf8 value)]
                   [(bytevector? value) value]
                   [(symbol? value) (string->utf8 (symbol->string value))]
                   [else (string->utf8 (format "~a" value))])]
           [hash (fnv1a-64 data)]
           [bv (make-bytevector 8)])
      (bytevector-u64-set! bv 0 hash (endianness big))
      bv))

  (def (fnv1a-64 data)
    (let ([len (bytevector-length data)])
      (let loop ([i 0] [h +fnv-offset+])
        (if (>= i len)
            (bitwise-and h #xFFFFFFFFFFFFFFFF)
            (let* ([byte (bytevector-u8-ref data i)]
                   [h2 (bitwise-xor h byte)]
                   [h3 (bitwise-and (* h2 +fnv-prime+) #xFFFFFFFFFFFFFFFF)])
              (loop (+ i 1) h3))))))

  ;; ---- Full 28-byte index key construction ----

  (def (encode-eavt-key e a v-hash tx added?)
    (let ([bv (make-bytevector 28 0)])
      (bytevector-copy! (encode-eid e) 0 bv 0 8)
      (bytevector-copy! (encode-aid a) 0 bv 8 4)
      (bytevector-copy! v-hash 0 bv 12 8)
      (bytevector-copy! (encode-tx+op tx added?) 0 bv 20 8)
      bv))

  (def (encode-aevt-key a e v-hash tx added?)
    (let ([bv (make-bytevector 28 0)])
      (bytevector-copy! (encode-aid a) 0 bv 0 4)
      (bytevector-copy! (encode-eid e) 0 bv 4 8)
      (bytevector-copy! v-hash 0 bv 12 8)
      (bytevector-copy! (encode-tx+op tx added?) 0 bv 20 8)
      bv))

  (def (encode-avet-key a v-hash e tx added?)
    (let ([bv (make-bytevector 28 0)])
      (bytevector-copy! (encode-aid a) 0 bv 0 4)
      (bytevector-copy! v-hash 0 bv 4 8)
      (bytevector-copy! (encode-eid e) 0 bv 12 8)
      (bytevector-copy! (encode-tx+op tx added?) 0 bv 20 8)
      bv))

  (def (encode-vaet-key v-hash a e tx added?)
    (let ([bv (make-bytevector 28 0)])
      (bytevector-copy! v-hash 0 bv 0 8)
      (bytevector-copy! (encode-aid a) 0 bv 8 4)
      (bytevector-copy! (encode-eid e) 0 bv 12 8)
      (bytevector-copy! (encode-tx+op tx added?) 0 bv 20 8)
      bv))

  ;; ---- Key decoding ----

  (def (decode-eavt-key bv)
    (let-values ([(tx added?) (decode-tx+op bv 20)])
      (values (decode-eid bv 0) (decode-aid bv 8) tx added?)))

  (def (decode-aevt-key bv)
    (let-values ([(tx added?) (decode-tx+op bv 20)])
      (values (decode-aid bv 0) (decode-eid bv 4) tx added?)))

  (def (decode-avet-key bv)
    (let-values ([(tx added?) (decode-tx+op bv 20)])
      (values (decode-aid bv 0) (decode-eid bv 12) tx added?)))

  (def (decode-vaet-key bv)
    (let-values ([(tx added?) (decode-tx+op bv 20)])
      (values (decode-aid bv 8) (decode-eid bv 12) tx added?)))

) ;; end library
