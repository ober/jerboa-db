#!/usr/bin/env scheme --libdirs lib:~/mine/jerboa/lib --script
;;; MBrainz benchmark runner for Jerboa-DB
;;;
;;; Runs the 8 standard Datahike MBrainz benchmark queries with timing.
;;;
;;; Usage:
;;;   make mbrainz              -- full scale, all 8 queries
;;;   make mbrainz-quick        -- 1% scale, quick smoke test
;;;   scheme ... --script benchmarks/mbrainz-bench.ss [--quick] [--scale 0.1]

(import (jerboa prelude)
        (jerboa-db core))

;; ---- Config ----

(def *scale* 1.0)
(def *runs* 5)        ;; repetitions per query for timing
(def *warmup* 1)      ;; warmup runs (not counted)

(def (parse-args! args)
  (let loop ([as args])
    (cond
      [(null? as) (void)]
      [(string=? (car as) "--quick")
       (set! *scale* 0.01)
       (set! *runs* 3)
       (loop (cdr as))]
      [(and (string=? (car as) "--scale") (pair? (cdr as)))
       (set! *scale* (string->number (cadr as)))
       (loop (cddr as))]
      [(and (string=? (car as) "--runs") (pair? (cdr as)))
       (set! *runs* (string->number (cadr as)))
       (loop (cddr as))]
      [else (loop (cdr as))])))

;; ---- Timing ----

(def (now-ms)
  (let ([t (current-time 'time-monotonic)])
    (+ (* 1000.0 (time-second t))
       (/ (time-nanosecond t) 1e6))))

(def (time-median thunk n warmup)
  ;; Run thunk n+warmup times, discard warmup, return (median-ms result)
  (let loop ([i 0] [times '()] [last-result #f])
    (if (>= i (+ n warmup))
        (let* ([sorted (sort times <)]
               [mid (quotient (length sorted) 2)]
               [med (list-ref sorted mid)])
          (list med last-result))
        (let ([t0 (now-ms)])
          (let ([r (thunk)])
            (let ([ms (- (now-ms) t0)])
              (loop (+ i 1)
                    (if (>= i warmup) (cons ms times) times)
                    r)))))))

;; ---- Formatting ----

(def (pad-right s n)
  (let ([l (string-length s)])
    (if (>= l n) s (string-append s (make-string (- n l) #\space)))))

(def (pad-left s n)
  (let ([l (string-length s)])
    (if (>= l n) s (string-append (make-string (- n l) #\space) s))))

(def (fmt-ms ms)
  (let* ([rounded (inexact->exact (round ms))]
         [s (string-append (number->string rounded) " ms")])
    (pad-left s 9)))

;; ---- Schema (inline) ----

(def (mbrainz-schema-ops)
  (list
    `((db/ident . abstract/gid) (db/valueType . db.type/string) (db/cardinality . db.cardinality/one) (db/unique . db.unique/identity) (db/index . #t))
    `((db/ident . artist/gid)   (db/valueType . db.type/string) (db/cardinality . db.cardinality/one) (db/unique . db.unique/identity) (db/index . #t))
    `((db/ident . artist/name)  (db/valueType . db.type/string) (db/cardinality . db.cardinality/one) (db/index . #t) (db/fulltext . #t))
    `((db/ident . artist/sortName) (db/valueType . db.type/string) (db/cardinality . db.cardinality/one))
    `((db/ident . artist/type)  (db/valueType . db.type/string) (db/cardinality . db.cardinality/one))
    `((db/ident . artist/gender) (db/valueType . db.type/string) (db/cardinality . db.cardinality/one))
    `((db/ident . artist/country) (db/valueType . db.type/string) (db/cardinality . db.cardinality/one) (db/index . #t))
    `((db/ident . artist/startYear) (db/valueType . db.type/long) (db/cardinality . db.cardinality/one) (db/index . #t))
    `((db/ident . artist/endYear)   (db/valueType . db.type/long) (db/cardinality . db.cardinality/one) (db/index . #t))
    `((db/ident . release/gid)  (db/valueType . db.type/string) (db/cardinality . db.cardinality/one) (db/unique . db.unique/identity) (db/index . #t))
    `((db/ident . release/name) (db/valueType . db.type/string) (db/cardinality . db.cardinality/one) (db/index . #t) (db/fulltext . #t))
    `((db/ident . release/artists) (db/valueType . db.type/ref) (db/cardinality . db.cardinality/many))
    `((db/ident . release/year)  (db/valueType . db.type/long) (db/cardinality . db.cardinality/one) (db/index . #t))
    `((db/ident . release/status) (db/valueType . db.type/string) (db/cardinality . db.cardinality/one) (db/index . #t))
    `((db/ident . release/country) (db/valueType . db.type/string) (db/cardinality . db.cardinality/one) (db/index . #t))
    `((db/ident . medium/release) (db/valueType . db.type/ref) (db/cardinality . db.cardinality/one))
    `((db/ident . medium/format) (db/valueType . db.type/string) (db/cardinality . db.cardinality/one))
    `((db/ident . medium/position) (db/valueType . db.type/long) (db/cardinality . db.cardinality/one))
    `((db/ident . medium/tracks) (db/valueType . db.type/ref) (db/cardinality . db.cardinality/many))
    `((db/ident . track/name)    (db/valueType . db.type/string) (db/cardinality . db.cardinality/one) (db/fulltext . #t))
    `((db/ident . track/position) (db/valueType . db.type/long) (db/cardinality . db.cardinality/one))
    `((db/ident . track/duration) (db/valueType . db.type/long) (db/cardinality . db.cardinality/one) (db/index . #t))
    `((db/ident . track/artists) (db/valueType . db.type/ref) (db/cardinality . db.cardinality/many))
    `((db/ident . label/gid)   (db/valueType . db.type/string) (db/cardinality . db.cardinality/one) (db/unique . db.unique/identity) (db/index . #t))
    `((db/ident . label/name)  (db/valueType . db.type/string) (db/cardinality . db.cardinality/one) (db/index . #t))
    `((db/ident . label/type)  (db/valueType . db.type/string) (db/cardinality . db.cardinality/one))
    `((db/ident . label/country) (db/valueType . db.type/string) (db/cardinality . db.cardinality/one) (db/index . #t))
    `((db/ident . label/startYear) (db/valueType . db.type/long) (db/cardinality . db.cardinality/one))
    `((db/ident . release/labels) (db/valueType . db.type/ref) (db/cardinality . db.cardinality/many))))

;; ---- Synthetic data generation ----

(def *rng-state* 42424242424242)
(def (next-rand!) (set! *rng-state* (modulo (+ (* *rng-state* 6364136223846793005) 1442695040888963407) (expt 2 64))) *rng-state*)
(def (rand-int! lo hi) (+ lo (modulo (next-rand!) (- hi lo))))
(def (rand-pick! lst) (list-ref lst (modulo (next-rand!) (length lst))))
(def (make-uuid!) (let ([h (lambda () (let ([s (number->string (modulo (next-rand!) 65536) 16)]) (string-append (make-string (- 4 (string-length s)) #\0) s)))]) (string-append (h)(h) "-" (h) "-" (h) "-" (h) "-" (h)(h)(h))))

(def *countries*    '("US" "GB" "DE" "FR" "JP" "CA" "AU" "SE" "NL" "NO"))
(def *statuses*     '("Official" "Promotion" "Bootleg"))
(def *first-names*  '("James" "John" "Robert" "Michael" "David" "Paul" "George" "Bob" "Eric" "Jimmy"))
(def *last-names*   '("Smith" "Johnson" "Williams" "Jones" "Brown" "Davis" "Young" "Page" "Plant"))
(def *band-words*   '("The" "Dark" "Electric" "Blue" "Black" "Iron" "Golden" "Wild" "Stone" "Fire"))
(def *band-nouns*   '("Kings" "Stars" "Birds" "Wolves" "Angels" "Heroes" "Warriors" "Giants"))
(def *album-adjs*   '("Dark" "Blue" "Eternal" "Lost" "Rising" "Broken" "Silent" "Electric" "Wild"))
(def *album-nouns*  '("Side" "Road" "Dream" "Fire" "Night" "Sky" "Stone" "River" "Soul" "Heart"))
(def *artist-types* '("Group" "Person" "Orchestra"))

(def (make-artist-name!) (if (< (modulo (next-rand!) 3) 1) (string-append (rand-pick! *band-words*) " " (rand-pick! *band-nouns*)) (string-append (rand-pick! *first-names*) " " (rand-pick! *last-names*))))
(def (make-album-name!)  (string-append (rand-pick! *album-adjs*) " " (rand-pick! *album-nouns*)))

;; Flush batch via transact!, return resolved entity IDs
(def (flush-batch! conn batch)
  (if (null? batch)
      '()
      (map cdr (tx-report-tempids (transact! conn (reverse batch))))))

(def (load-synthetic-data! conn scale batch-size)
  (let ([num-artists        (inexact->exact (round (* 262000 scale)))]
        [releases-per-artist 5]
        [tracks-per-artist  50])  ;; 5 releases × 10 tracks, stored flat per artist

    ;; ---- Schema ----
    (transact! conn (mbrainz-schema-ops))

    ;; ---- Phase 1: artists — collect eids for later reference ----
    (let ([artist-eids '()])
      (let loop ([i 0] [batch '()])
        (cond
          [(= i num-artists)
           (set! artist-eids (append (flush-batch! conn batch) artist-eids))]
          [else
           (let* ([op `((db/id . ,(tempid))
                        (artist/gid       . ,(make-uuid!))
                        (artist/name      . ,(make-artist-name!))
                        (artist/type      . ,(rand-pick! *artist-types*))
                        (artist/country   . ,(rand-pick! *countries*))
                        (artist/startYear . ,(rand-int! 1920 2010)))]
                  [b (cons op batch)])
             (if (>= (length b) batch-size)
                 (begin
                   (set! artist-eids (append (flush-batch! conn b) artist-eids))
                   (loop (+ i 1) '()))
                 (loop (+ i 1) b)))]))

      ;; ---- Phase 2: releases — flat pass, batch by batch ----
      (let ([total-releases 0])
        (let loop ([eids artist-eids] [batch '()])
          (cond
            [(null? eids)
             (set! total-releases (+ total-releases (length batch)))
             (flush-batch! conn batch)]
            [else
             (let* ([aeid (car eids)]
                    [new-ops (map (lambda (_)
                                    `((db/id . ,(tempid))
                                      (release/gid     . ,(make-uuid!))
                                      (release/name    . ,(make-album-name!))
                                      (release/artists . ,aeid)
                                      (release/year    . ,(rand-int! 1950 2024))
                                      (release/status  . ,(rand-pick! *statuses*))
                                      (release/country . ,(rand-pick! *countries*))))
                                  (iota releases-per-artist))]
                    [b (append new-ops batch)])
               (if (>= (length b) batch-size)
                   (begin
                     (set! total-releases (+ total-releases (length b)))
                     (flush-batch! conn b)
                     (loop (cdr eids) '()))
                   (loop (cdr eids) b)))]))

        ;; ---- Phase 3: tracks — flat pass, batch by batch ----
        (let ([total-tracks 0])
          (let loop ([eids artist-eids] [batch '()])
            (cond
              [(null? eids)
               (set! total-tracks (+ total-tracks (length batch)))
               (flush-batch! conn batch)]
              [else
               (let* ([aeid (car eids)]
                      [new-ops (map (lambda (ti)
                                      `((track/name     . ,(string-append "Track " (number->string ti)))
                                        (track/position . ,ti)
                                        (track/duration . ,(rand-int! 120000 480000))
                                        (track/artists  . ,aeid)))
                                    (iota tracks-per-artist 1))]
                      [b (append new-ops batch)])
                 (if (>= (length b) batch-size)
                     (begin
                       (set! total-tracks (+ total-tracks (length b)))
                       (flush-batch! conn b)
                       (loop (cdr eids) '()))
                     (loop (cdr eids) b)))]))


          (list 'artists (length artist-eids)
                'releases total-releases
                'tracks total-tracks))))))

;; ---- The 8 MBrainz benchmark queries ----

;; Q1: Simple attribute lookup — find an artist by exact name
;; Tests: AVET point lookup
(def (q1 db name)
  (q '((find ?e)
       (in $ ?name)
       (where (?e artist/name ?name)))
     db name))

;; Q2: Two-clause join — find releases by artist name
;; Tests: AEVT scan + join
(def (q2 db artist-name)
  (q '((find ?r ?rname)
       (in $ ?aname)
       (where (?a artist/name ?aname)
              (?r release/artists ?a)
              (?r release/name ?rname)))
     db artist-name))

;; Q3: Range predicate — artists with startYear before threshold
;; Tests: AVET range scan with predicate pushdown
(def (q3 db year-threshold)
  (q '((find ?e ?name ?year)
       (in $ ?thresh)
       (where (?e artist/startYear ?year)
              (?e artist/name ?name)
              ((< ?year ?thresh))))
     db year-threshold))

;; Q4: Multi-join — tracks with duration above threshold on releases by artist
;; Tests: deep join, 4 data patterns + predicate
(def (q4 db min-duration)
  (q '((find ?track-name ?duration ?release-name)
       (in $ ?min-dur)
       (where (?t track/duration ?duration)
              (?t track/name ?track-name)
              (?t track/artists ?a)
              (?r release/artists ?a)
              (?r release/name ?release-name)
              ((> ?duration ?min-dur))))
     db min-duration))

;; Q5: Aggregation — count releases per country
;; Tests: aggregation + grouping
(def (q5 db)
  (q '((find ?country (count ?r))
       (where (?r release/country ?country)))
     db))

;; Q6: Reverse ref navigation — find all releases for an artist entity
;; Tests: cardinality/many reverse lookup via VAET
(def (q6 db artist-eid)
  (q '((find ?r ?rname)
       (in $ ?artist)
       (where (?r release/artists ?artist)
              (?r release/name ?rname)))
     db artist-eid))

;; Q7: Pull pattern — artist with all attributes
;; Tests: pull API
(def (q7 db artist-eid)
  (pull db '[artist/name artist/type artist/country artist/startYear] artist-eid))

;; Q8: Complex aggregation — avg track duration by release status
;; Tests: aggregation + join + grouping
(def (q8 db)
  (q '((find ?status (count ?t) (avg ?dur))
       (where (?t track/duration ?dur)
              (?t track/artists ?a)
              (?r release/artists ?a)
              (?r release/status ?status)))
     db))

;; ---- Benchmark runner ----

(def (run-benchmark conn sample-artist-eid sample-artist-name)
  (let ([db (db conn)])

    (displayln "")
    (displayln "=== Jerboa-DB MBrainz Benchmark ===")
    (displayln "")
    (displayln (pad-right "Query" 55) (pad-left "Median" 10) (pad-left "Result" 15))
    (displayln (make-string 80 #\-))

    (let ([bench (lambda (name label thunk)
                   (let* ([result (time-median thunk *runs* *warmup*)]
                          [ms (car result)]
                          [r (cadr result)]
                          [n (cond [(list? r) (length r)]
                                   [(number? r) r]
                                   [else 1])])
                     (displayln (pad-right (string-append name ": " label) 55)
                                (fmt-ms ms)
                                (pad-left (string-append (number->string n) " rows") 15))
                     (cons name ms)))])

      (let ([results
        (list
          (bench "Q1" "Artist exact name lookup"
                 (lambda () (q1 db sample-artist-name)))
          (bench "Q2" "Releases by artist name"
                 (lambda () (q2 db sample-artist-name)))
          (bench "Q3" "Artists with startYear < 1960"
                 (lambda () (q3 db 1960)))
          (bench "Q4" "Tracks > 240s on shared-artist releases"
                 (lambda () (q4 db 240000)))
          (bench "Q5" "Count releases per country"
                 (lambda () (q5 db)))
          (bench "Q6" "All releases for one artist (reverse ref)"
                 (lambda () (q6 db sample-artist-eid)))
          (bench "Q7" "Pull artist attributes"
                 (lambda () (q7 db sample-artist-eid)))
          (bench "Q8" "Avg track duration by release status"
                 (lambda () (q8 db))))])

        (displayln (make-string 80 #\-))
        (let ([total (apply + (map cdr results))])
          (displayln (pad-right "Total" 55) (fmt-ms total)))
        (displayln "")
        results))))

;; ---- Main ----

(parse-args! (cdr (command-line)))

(displayln "MBrainz Benchmark — scale=" *scale* "  runs=" *runs*)
(displayln "")

(def conn (connect ":memory:"))

;; Load data
(display "Loading data... ")
(def t0 (now-ms))
(def load-stats (load-synthetic-data! conn *scale* 500))
(displayln "done in " (inexact->exact (round (- (now-ms) t0))) " ms")
(displayln "  " load-stats)

;; Find a sample artist for queries that need one
(def sample-artist
  (let ([rows (q '((find ?e ?name)
                   (where (?e artist/name ?name)))
                 (db conn))])
    (if (pair? rows)
        (car rows)
        (list 1 "Unknown Artist"))))

(def sample-artist-eid  (car sample-artist))
(def sample-artist-name (cadr sample-artist))

(displayln "  Sample artist: eid=" sample-artist-eid " name=" sample-artist-name)

;; Run benchmark
(run-benchmark conn sample-artist-eid sample-artist-name)
