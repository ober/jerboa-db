#!chezscheme
;;; (jerboa-db index memory) — In-memory index backend using sorted maps
;;;
;;; Each index is a mutable cell holding a sorted-map. Datoms are the keys;
;;; values are #t (we only need key presence). Range scans use sorted-map-range
;;; with probe datoms at the boundaries.

(library (jerboa-db index memory)
  (export make-mem-index-set)

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

  ;; ---- Sorted map (minimal in-library implementation) ----
  ;; We use a red-black tree to avoid external module dependencies in Phase 1.
  ;; Keys are datoms compared with an index-specific comparator.

  ;; RB-tree node: #f (empty) or (color left key value right)
  ;; color: 'R or 'B

  (def (rb-empty) #f)
  (def (rb-empty? t) (not t))
  (def (rb-node color left key val right) (vector color left key val right))
  (def (rb-color n) (vector-ref n 0))
  (def (rb-left n)  (vector-ref n 1))
  (def (rb-key n)   (vector-ref n 2))
  (def (rb-val n)   (vector-ref n 3))
  (def (rb-right n) (vector-ref n 4))

  (def (rb-balance color left key val right)
    ;; Okasaki balance for red-black trees
    (cond
      ;; Case 1: left-left red
      [(and (eq? color 'B) (not (rb-empty? left)) (eq? (rb-color left) 'R)
            (not (rb-empty? (rb-left left))) (eq? (rb-color (rb-left left)) 'R))
       (rb-node 'R
         (rb-node 'B (rb-left (rb-left left))
                     (rb-key (rb-left left)) (rb-val (rb-left left))
                     (rb-right (rb-left left)))
         (rb-key left) (rb-val left)
         (rb-node 'B (rb-right left) key val right))]
      ;; Case 2: left-right red
      [(and (eq? color 'B) (not (rb-empty? left)) (eq? (rb-color left) 'R)
            (not (rb-empty? (rb-right left))) (eq? (rb-color (rb-right left)) 'R))
       (rb-node 'R
         (rb-node 'B (rb-left left) (rb-key left) (rb-val left)
                     (rb-left (rb-right left)))
         (rb-key (rb-right left)) (rb-val (rb-right left))
         (rb-node 'B (rb-right (rb-right left)) key val right))]
      ;; Case 3: right-left red
      [(and (eq? color 'B) (not (rb-empty? right)) (eq? (rb-color right) 'R)
            (not (rb-empty? (rb-left right))) (eq? (rb-color (rb-left right)) 'R))
       (rb-node 'R
         (rb-node 'B left key val (rb-left (rb-left right)))
         (rb-key (rb-left right)) (rb-val (rb-left right))
         (rb-node 'B (rb-right (rb-left right))
                     (rb-key right) (rb-val right)
                     (rb-right right)))]
      ;; Case 4: right-right red
      [(and (eq? color 'B) (not (rb-empty? right)) (eq? (rb-color right) 'R)
            (not (rb-empty? (rb-right right))) (eq? (rb-color (rb-right right)) 'R))
       (rb-node 'R
         (rb-node 'B left key val (rb-left right))
         (rb-key right) (rb-val right)
         (rb-node 'B (rb-left (rb-right right))
                     (rb-key (rb-right right)) (rb-val (rb-right right))
                     (rb-right (rb-right right))))]
      [else (rb-node color left key val right)]))

  (def (rb-insert tree key val cmp)
    (define (ins t)
      (if (rb-empty? t)
          (rb-node 'R (rb-empty) key val (rb-empty))
          (let ([c (cmp key (rb-key t))])
            (cond
              [(< c 0) (rb-balance (rb-color t)
                         (ins (rb-left t)) (rb-key t) (rb-val t) (rb-right t))]
              [(> c 0) (rb-balance (rb-color t)
                         (rb-left t) (rb-key t) (rb-val t) (ins (rb-right t)))]
              [else t]))))  ;; duplicate — no change
    (let ([r (ins tree)])
      (rb-node 'B (rb-left r) (rb-key r) (rb-val r) (rb-right r))))

  (def (rb-delete tree key cmp)
    ;; Simplified delete — rebuild without the key
    ;; For Phase 1 correctness, this is O(n) but works reliably.
    (let ([pairs '()])
      (rb-fold (lambda (k v acc)
                 (if (= (cmp k key) 0) acc (cons (cons k v) acc)))
               '() tree)
      (let ([filtered (rb-fold (lambda (k v acc)
                                 (if (= (cmp k key) 0) acc (cons (cons k v) acc)))
                               '() tree)])
        (fold-left (lambda (t pair) (rb-insert t (car pair) (cdr pair) cmp))
                   (rb-empty) filtered))))

  ;; In-order fold: (proc key value accumulator) -> accumulator
  (def (rb-fold proc init tree)
    (if (rb-empty? tree)
        init
        (let* ([left-result (rb-fold proc init (rb-left tree))]
               [mid-result (proc (rb-key tree) (rb-val tree) left-result)])
          (rb-fold proc mid-result (rb-right tree)))))

  ;; Range fold: fold over keys in [lo, hi] (inclusive) by comparator
  (def (rb-range-fold proc init tree lo hi cmp)
    (if (rb-empty? tree)
        init
        (let ([k (rb-key tree)]
              [v (rb-val tree)])
          (let ([cmp-lo (cmp k lo)]
                [cmp-hi (cmp k hi)])
            (let* ([acc (if (>= cmp-lo 0)
                            (rb-range-fold proc init (rb-left tree) lo hi cmp)
                            init)]
                   [acc (if (and (>= cmp-lo 0) (<= cmp-hi 0))
                            (proc k v acc)
                            acc)]
                   [acc (if (<= cmp-hi 0)
                            (rb-range-fold proc acc (rb-right tree) lo hi cmp)
                            acc)])
              acc)))))

  ;; Collect all keys in-order
  (def (rb-keys tree)
    (reverse (rb-fold (lambda (k v acc) (cons k acc)) '() tree)))

  ;; Count nodes
  (def (rb-size tree)
    (rb-fold (lambda (k v acc) (+ acc 1)) 0 tree))

  ;; ---- Memory index implementation ----

  (def (make-mem-index name comparator)
    (let ([tree-cell (list (rb-empty))])  ;; mutable cell
      (define (get-tree) (car tree-cell))
      (define (set-tree! t) (set-car! tree-cell t))

      (define (add! datom)
        (set-tree! (rb-insert (get-tree) datom #t comparator)))

      (define (remove! datom)
        (set-tree! (rb-delete (get-tree) datom comparator)))

      (define (range-query start end)
        (reverse
          (rb-range-fold (lambda (k v acc) (cons k acc))
                         '() (get-tree) start end comparator)))

      (define (seek . components)
        ;; Build probe datoms from components for a prefix scan.
        ;; components is an alist of known fields.
        (let-values ([(e a v tx) (apply values components)])
          (let ([lo (make-datom
                      (or e 0)
                      (or a 0)
                      (if v v +min-val+)
                      (or tx 0)
                      #t)]
                [hi (make-datom
                      (or e (greatest-fixnum))
                      (or a (greatest-fixnum))
                      (if v v +max-val+)
                      (or tx (greatest-fixnum))
                      #t)])
            (range-query lo hi))))

      (define (count-range start end)
        (rb-range-fold (lambda (k v acc) (+ acc 1))
                       0 (get-tree) start end comparator))

      (define (snapshot) (get-tree))

      (define (all-datoms) (rb-keys (get-tree)))

      (make-dbi name add! remove! range-query seek count-range snapshot all-datoms)))

  ;; ---- Create the four covering indices ----

  (def (make-mem-index-set)
    (make-index-set
      (make-mem-index 'eavt compare-datoms-eavt)
      (make-mem-index 'aevt compare-datoms-aevt)
      (make-mem-index 'avet compare-datoms-avet)
      (make-mem-index 'vaet compare-datoms-vaet)))

) ;; end library
