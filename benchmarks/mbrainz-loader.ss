#!/usr/bin/env scheme --libdirs lib:~/mine/jerboa/lib --script
;;; MBrainz bulk data loader for Jerboa-DB
;;;
;;; Loads either actual MBrainz EDN files or generates synthetic data at scale.
;;;
;;; Usage:
;;;   make bench                           -- full scale (6.6M entities)
;;;   scheme ... --script benchmarks/mbrainz-loader.ss --quick    -- 66K entities
;;;   scheme ... --script benchmarks/mbrainz-loader.ss --scale 0.1
;;;
;;; Returns a connection ready for benchmarking.

(import (jerboa prelude)
        (jerboa-db core))

;; ---- Import schema installer ----
;; (loaded inline since we can't import .ss files directly)

;; ---- Config ----

(def *scale* 1.0)   ;; 1.0 = full MBrainz scale (~6.6M entities)
(def *batch-size* 1000)

(def (parse-args! args)
  (let loop ([as args])
    (cond
      [(null? as) (void)]
      [(string=? (car as) "--quick")
       (set! *scale* 0.01)
       (loop (cdr as))]
      [(and (string=? (car as) "--scale") (pair? (cdr as)))
       (set! *scale* (string->number (cadr as)))
       (loop (cddr as))]
      [else (loop (cdr as))])))

;; ---- Timing ----

(def (now-ms)
  (let ([t (current-time 'time-monotonic)])
    (+ (* 1000.0 (time-second t))
       (/ (time-nanosecond t) 1e6))))

;; ---- Pseudo-random data generation ----
;; Simple LCG for reproducible synthetic data

(def *rng-state* 12345678901234567)

(def (next-rand!)
  (set! *rng-state* (modulo (+ (* *rng-state* 6364136223846793005) 1442695040888963407)
                            (expt 2 64)))
  *rng-state*)

(def (rand-int! lo hi)
  (+ lo (modulo (next-rand!) (- hi lo))))

(def (rand-pick! lst)
  (list-ref lst (modulo (next-rand!) (length lst))))

(def (make-uuid!)
  ;; Generate a fake UUID string
  (def (hex4) (let ([n (modulo (next-rand!) 65536)])
                (let ([s (number->string n 16)])
                  (string-append (make-string (- 4 (string-length s)) #\0) s))))
  (string-append (hex4) (hex4) "-" (hex4) "-" (hex4) "-" (hex4) "-" (hex4) (hex4) (hex4)))

;; ---- Synthetic data ----

(def *artist-types*  '("Group" "Person" "Orchestra" "Choir" "Other"))
(def *genders*       '("Male" "Female" "Other"))
(def *countries*     '("US" "GB" "DE" "FR" "JP" "CA" "AU" "SE" "NL" "NO"))
(def *statuses*      '("Official" "Promotion" "Bootleg" "Pseudo-Release"))
(def *formats*       '("CD" "Vinyl" "Digital Media" "Cassette" "DVD" "Blu-ray"))
(def *label-types*   '("Original Production" "Bootleg Production" "Reissue Production" "Publisher"))

(def *first-names* '("James" "John" "Robert" "Michael" "William" "David" "Richard" "Charles"
                      "Mary" "Patricia" "Jennifer" "Linda" "Barbara" "Susan" "Jessica" "Sarah"
                      "Paul" "George" "Ringo" "Keith" "Mick" "Bob" "Eric" "Jimmy" "Roger"))
(def *last-names*  '("Smith" "Johnson" "Williams" "Jones" "Brown" "Davis" "Miller" "Wilson"
                      "Moore" "Taylor" "Anderson" "Jackson" "White" "Harris" "Martin" "Garcia"
                      "Young" "Page" "Plant" "Clapton" "Richards" "Jagger" "Dylan"))
(def *band-words*  '("The" "Dark" "Electric" "Blue" "Black" "Red" "Iron" "Golden" "Silver"
                      "Wild" "Stone" "Fire" "Night" "Dream" "Magic" "Cosmic" "Atomic"))
(def *band-nouns*  '("Kings" "Stars" "Lights" "Birds" "Wolves" "Dragons" "Phoenix" "Riders"
                      "Angels" "Demons" "Heroes" "Legends" "Warriors" "Giants" "Travelers"))

(def *album-adjectives* '("Dark" "Blue" "Eternal" "Lost" "Rising" "Broken" "Silent" "Electric"
                           "Wild" "Golden" "Sacred" "Hollow" "Ancient" "Burning" "Fading"))
(def *album-nouns*      '("Side" "Road" "Dream" "Fire" "Night" "Sky" "Stone" "River" "Soul"
                           "Heart" "Mind" "World" "Time" "Space" "Light" "Shadow" "Voice"))

(def *track-verbs*  '("Running" "Flying" "Dancing" "Crying" "Falling" "Rising" "Burning" "Fading"))
(def *track-nouns*  '("Away" "Home" "Free" "Wild" "Alone" "Together" "Higher" "Deeper"))

(def (make-artist-name!)
  (if (< (modulo (next-rand!) 3) 1)
      ;; Band name
      (string-append (rand-pick! *band-words*) " " (rand-pick! *band-nouns*))
      ;; Person name
      (string-append (rand-pick! *first-names*) " " (rand-pick! *last-names*))))

(def (make-album-name!)
  (string-append (rand-pick! *album-adjectives*) " " (rand-pick! *album-nouns*)))

(def (make-track-name! n)
  (string-append (rand-pick! *track-verbs*) " " (rand-pick! *track-nouns*)
                 " " (number->string n)))

;; ---- Scale targets ----
;; MBrainz full scale:
;;   ~262K artists, ~1.4M releases, ~1.4M media, ~4.8M tracks = ~7.9M total entities
;;   (The 6.6M figure is the commonly cited entity count for a subset)

(def (entity-counts scale)
  ;; Returns (artists releases-per-artist tracks-per-release)
  (let ([artists (inexact->exact (round (* 262000 scale)))])
    (list artists 5 10)))   ;; avg 5 releases/artist, 10 tracks/release

;; ---- Batch loading ----

(def (batch-transact! conn ops progress-label total-batches batch-num)
  (transact! conn ops)
  ;; Show progress every 100 batches
  (when (= 0 (modulo batch-num 100))
    (display (string-append "\r  " progress-label ": "
                            (number->string (* batch-num *batch-size*))
                            "/" (number->string (* total-batches *batch-size*))
                            " entities                "))
    (flush-output-port (current-output-port))))

;; ---- Schema installation (inline) ----

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

;; ---- Main loader ----

(def (load-mbrainz! conn . opts)
  (let ([scale (if (null? opts) *scale* (car opts))])
    (let ([t0 (now-ms)])

      ;; 1. Install schema
      (display "Installing MBrainz schema... ")
      (transact! conn (mbrainz-schema-ops))
      (displayln "done (" (inexact->exact (round (- (now-ms) t0))) " ms)")

      ;; 2. Load artists
      (let-values ([(num-artists releases-per-artist tracks-per-release)
                    (apply values (entity-counts scale))])
        (displayln "Loading " num-artists " artists ("
                   (inexact->exact (round (* num-artists releases-per-artist))) " releases, "
                   (inexact->exact (round (* num-artists releases-per-artist tracks-per-release))) " tracks)...")

        ;; Phase A: Load artists in batches, collect eids
        (let ([t1 (now-ms)]
              [artist-eids (make-vector num-artists 0)])

          (let loop ([i 0] [batch '()] [batch-count 0])
            (cond
              [(= i num-artists)
               ;; Flush final batch
               (when (pair? batch)
                 (let* ([tid-list (map (lambda (op) (cdr (assq 'db/id op))) batch)]
                        [report (transact! conn (reverse batch))]
                        [tempids (tx-report-tempids report)])
                   (for-each
                     (lambda (tid-pair)
                       (let* ([tid (car tid-pair)]
                              [eid (cdr tid-pair)]
                              [idx (- (length tid-list) 1 (length (memv tid tid-list)))])
                         (when (< idx num-artists)
                           (vector-set! artist-eids idx eid))))
                     tempids)))]
              [else
               (let* ([tid (tempid)]
                      [name (make-artist-name!)]
                      [op `((db/id . ,tid)
                             (artist/gid . ,(make-uuid!))
                             (artist/name . ,name)
                             (artist/sortName . ,name)
                             (artist/type . ,(rand-pick! *artist-types*))
                             (artist/country . ,(rand-pick! *countries*))
                             (artist/startYear . ,(rand-int! 1900 2020)))])
                      [new-batch (cons op batch)])
                 (if (= (length new-batch) *batch-size*)
                     (let* ([tid-list (map (lambda (o) (cdr (assq 'db/id o))) new-batch)]
                            [report (transact! conn (reverse new-batch))]
                            [tempids (tx-report-tempids report)])
                       ;; Record artist eids
                       (let ([batch-start (* batch-count *batch-size*)])
                         (for-each
                           (lambda (tid-pair)
                             (let* ([tid (car tid-pair)]
                                    [eid (cdr tid-pair)])
                               (let find-idx ([tl tid-list] [j 0])
                                 (cond
                                   [(null? tl) (void)]
                                   [(equal? tid (car tl))
                                    (let ([idx (+ batch-start j)])
                                      (when (< idx num-artists)
                                        (vector-set! artist-eids idx eid)))]
                                   [else (find-idx (cdr tl) (+ j 1))]))))
                           tempids))
                       (when (= 0 (modulo (+ batch-count 1) 50))
                         (display (string-append "\r  Artists: "
                                   (number->string (* (+ batch-count 1) *batch-size*))
                                   "/" (number->string num-artists) "             "))
                         (flush-output-port (current-output-port)))
                       (loop (+ i 1) '() (+ batch-count 1)))
                     (loop (+ i 1) new-batch batch-count)))]))

          (displayln "\r  Artists loaded in " (inexact->exact (round (- (now-ms) t1))) " ms        ")

          ;; Phase B: Load releases + tracks
          (let ([t2 (now-ms)]
                [release-batch '()]
                [track-batch '()]
                [total-releases 0]
                [total-tracks 0])

            ;; For each artist, create releases
            (let artist-loop ([ai 0])
              (when (< ai num-artists)
                (let ([artist-eid (vector-ref artist-eids ai)])
                  (when (> artist-eid 0)
                    (let release-loop ([ri 0])
                      (when (< ri releases-per-artist)
                        (let* ([rel-tid (tempid)]
                               [rel-year (rand-int! 1960 2024)]
                               [rel-op `((db/id . ,rel-tid)
                                          (release/gid . ,(make-uuid!))
                                          (release/name . ,(make-album-name!))
                                          (release/artists . ,artist-eid)
                                          (release/year . ,rel-year)
                                          (release/status . ,(rand-pick! *statuses*))
                                          (release/country . ,(rand-pick! *countries*)))])
                          ;; Add to release batch
                          (set! release-batch (cons rel-op release-batch))
                          (set! total-releases (+ total-releases 1))

                          ;; Flush release batch when full
                          (when (>= (length release-batch) *batch-size*)
                            (let* ([report (transact! conn (reverse release-batch))]
                                   [tempids (tx-report-tempids report)])
                              ;; Build rel-eid map for track creation
                              (let ([rel-eids (map cdr tempids)])
                                ;; Create tracks for each release in this batch
                                (for-each
                                  (lambda (rel-eid)
                                    (let track-loop ([ti 0])
                                      (when (< ti tracks-per-release)
                                        (let ([trk-op `((track/name . ,(make-track-name! ti))
                                                         (track/position . ,(+ ti 1))
                                                         (track/duration . ,(rand-int! 120000 480000))
                                                         (track/artists . ,artist-eid))])
                                          (set! track-batch (cons trk-op track-batch))
                                          (set! total-tracks (+ total-tracks 1))
                                          (when (>= (length track-batch) *batch-size*)
                                            (transact! conn (reverse track-batch))
                                            (set! track-batch '())))
                                        (track-loop (+ ti 1)))))
                                  rel-eids)))
                              (set! release-batch '()))
                            (when (= 0 (modulo total-releases (* *batch-size* 10)))
                              (display (string-append "\r  Releases: "
                                        (number->string total-releases) "  Tracks: "
                                        (number->string total-tracks) "             "))
                              (flush-output-port (current-output-port)))))
                        (release-loop (+ ri 1))))))
                (artist-loop (+ ai 1))))

            ;; Flush remaining batches
            (when (pair? release-batch)
              (transact! conn (reverse release-batch)))
            (when (pair? track-batch)
              (transact! conn (reverse track-batch)))

            (displayln "\r  Releases: " total-releases "  Tracks: " total-tracks
                       "  (" (inexact->exact (round (- (now-ms) t2))) " ms)          ")

            (let ([total-ms (inexact->exact (round (- (now-ms) t0)))])
              (displayln "Load complete: " (+ num-artists total-releases total-tracks)
                         " entities in " total-ms " ms"
                         " (" (inexact->exact (round (/ (* (+ num-artists total-releases total-tracks) 1000.0) total-ms)))
                         " entities/sec)")
              (list 'artists num-artists
                    'releases total-releases
                    'tracks total-tracks
                    'total-ms total-ms))))))))

;; ---- Main (if run as script) ----

(parse-args! (cdr (command-line)))

(displayln "MBrainz Loader — scale=" *scale* "  batch-size=" *batch-size*)
(def conn (connect ":memory:"))
(load-mbrainz! conn *scale*)
(let ([stats (db-stats conn)])
  (displayln "DB stats: " stats))
