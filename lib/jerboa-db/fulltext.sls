#!chezscheme
;;; (jerboa-db fulltext) — In-memory fulltext search over string attributes
;;;
;;; Attributes marked with :db/fulltext true are indexed into an inverted word
;;; index. Supports word/substring search.

(library (jerboa-db fulltext)
  (export
    make-fulltext-index fulltext-index?
    fulltext-index-datoms! fulltext-remove-datoms!
    ft-search fulltext-stats)

  (import (chezscheme)
          (jerboa-db datom)
          (jerboa-db schema))

  ;; ---- Internal record ----
  ;; word-table: hashtable word -> list of (eid . attr-id)
  ;; value-table: hashtable (eid . attr-id) -> value string

  (define-record-type ft-idx
    (fields (mutable word-table)
            (mutable value-table)
            (mutable entry-count)))

  (define (make-fulltext-index)
    (make-ft-idx (make-hashtable string-hash string=?)
                 (make-hashtable equal-hash equal?)
                 0))

  (define (fulltext-index? x) (ft-idx? x))

  ;; ---- Tokenization ----

  (define (tokenize str)
    (let loop ([chars (string->list str)] [current '()] [result '()])
      (cond
        [(null? chars)
         (if (null? current)
             result
             (cons (list->string (reverse current)) result))]
        [(or (char-alphabetic? (car chars)) (char-numeric? (car chars)))
         (loop (cdr chars)
               (cons (char-downcase (car chars)) current)
               result)]
        [else
         (if (null? current)
             (loop (cdr chars) '() result)
             (loop (cdr chars) '()
                   (cons (list->string (reverse current)) result)))])))

  ;; ---- Indexing ----

  (define (fulltext-index-datoms! ft schema datoms)
    (for-each
      (lambda (d)
        (let ([attr (schema-lookup-by-id schema (datom-a d))])
          (when (and attr (db-attribute-fulltext? attr) (string? (datom-v d)))
            (if (datom-added? d)
                (index-value! ft (datom-e d) (datom-a d) (datom-v d))
                (remove-value! ft (datom-e d) (datom-a d) (datom-v d))))))
      datoms))

  (define (fulltext-remove-datoms! ft schema datoms)
    (for-each
      (lambda (d)
        (let ([attr (schema-lookup-by-id schema (datom-a d))])
          (when (and attr (db-attribute-fulltext? attr) (string? (datom-v d)))
            (remove-value! ft (datom-e d) (datom-a d) (datom-v d)))))
      datoms))

  (define (index-value! ft eid attr-id value)
    (let ([key (cons eid attr-id)]
          [wt  (ft-idx-word-table ft)]
          [vt  (ft-idx-value-table ft)])
      ;; Remove any previous index for this key
      (let ([old (hashtable-ref vt key #f)])
        (when old (remove-words! wt key (tokenize old))))
      ;; Store and index new value
      (hashtable-set! vt key value)
      (for-each
        (lambda (word)
          (let ([existing (hashtable-ref wt word '())])
            (unless (member key existing)
              (hashtable-set! wt word (cons key existing)))))
        (tokenize value))
      (ft-idx-entry-count-set! ft (+ (ft-idx-entry-count ft) 1))))

  (define (remove-value! ft eid attr-id value)
    (let ([key (cons eid attr-id)]
          [wt  (ft-idx-word-table ft)]
          [vt  (ft-idx-value-table ft)])
      (hashtable-delete! vt key)
      (remove-words! wt key (tokenize value))
      (when (> (ft-idx-entry-count ft) 0)
        (ft-idx-entry-count-set! ft (- (ft-idx-entry-count ft) 1)))))

  (define (remove-words! wt key words)
    (for-each
      (lambda (word)
        (let ([existing (hashtable-ref wt word '())])
          (let ([updated (filter (lambda (k) (not (equal? k key))) existing)])
            (if (null? updated)
                (hashtable-delete! wt word)
                (hashtable-set! wt word updated)))))
      words))

  ;; ---- Search ----

  ;; Returns list of (eid . value) pairs for entities whose attr value
  ;; contains text as a substring (case-insensitive word-level match).

  (define (ft-search ft schema attr-ident text)
    (let ([attr (schema-lookup-by-ident schema attr-ident)])
      (unless attr
        (error 'ft-search "Unknown attribute" attr-ident))
      (let* ([attr-id (db-attribute-id attr)]
             [wt (ft-idx-word-table ft)]
             [vt (ft-idx-value-table ft)]
             [needle (string-downcase* text)])
        ;; Walk word table, find words containing needle
        (let ([matching-keys '()])
          (let-values ([(words key-lists) (hashtable-entries wt)])
            (vector-for-each
              (lambda (word keys)
                (when (string-contains? word needle)
                  (for-each
                    (lambda (key)
                      (when (and (= (cdr key) attr-id)
                                 (not (member key matching-keys)))
                        (set! matching-keys (cons key matching-keys))))
                    keys)))
              words key-lists))
          ;; Build result
          (filter-map
            (lambda (key)
              (let ([val (hashtable-ref vt key #f)])
                (and val (cons (car key) val))))
            matching-keys)))))

  ;; ---- Stats ----

  (define (fulltext-stats ft)
    (let-values ([(words _) (hashtable-entries (ft-idx-word-table ft))])
      (list (cons 'indexed-values (ft-idx-entry-count ft))
            (cons 'unique-words (vector-length words)))))

  ;; ---- Utilities ----

  (define (string-downcase* s)
    (list->string (map char-downcase (string->list s))))

  (define (string-contains? haystack needle)
    (let ([hlen (string-length haystack)]
          [nlen (string-length needle)])
      (if (zero? nlen)
          #t
          (let loop ([i 0])
            (cond
              [(> (+ i nlen) hlen) #f]
              [(string=? (substring haystack i (+ i nlen)) needle) #t]
              [else (loop (+ i 1))])))))

  (define (filter-map f lst)
    (let loop ([lst lst] [acc '()])
      (if (null? lst) (reverse acc)
          (let ([r (f (car lst))])
            (loop (cdr lst) (if r (cons r acc) acc))))))

) ;; end library
