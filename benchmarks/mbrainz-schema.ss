#!/usr/bin/env scheme --libdirs lib:~/mine/jerboa/lib --script
;;; MBrainz schema definition for Jerboa-DB
;;;
;;; All attribute definitions for the MusicBrainz dataset.
;;; Use: (import (jerboa-db core)) then call (install-mbrainz-schema! conn)

(import (jerboa prelude)
        (jerboa-db core))

;; ---- Schema definition ----
;; Returns a list of tx-op alists to be passed to (transact! conn ops)

(def (mbrainz-schema-ops)
  (list
    ;; ---- Abstract entity (artist / label base) ----
    `((db/ident . abstract/gid)
      (db/valueType . db.type/string)
      (db/cardinality . db.cardinality/one)
      (db/unique . db.unique/identity)
      (db/index . #t)
      (db/doc . "MusicBrainz globally unique identifier (UUID string)"))

    `((db/ident . abstract/name)
      (db/valueType . db.type/string)
      (db/cardinality . db.cardinality/one)
      (db/index . #t)
      (db/doc . "Primary name of the entity"))

    ;; ---- Artist ----
    `((db/ident . artist/gid)
      (db/valueType . db.type/string)
      (db/cardinality . db.cardinality/one)
      (db/unique . db.unique/identity)
      (db/index . #t)
      (db/doc . "Artist MusicBrainz GUID"))

    `((db/ident . artist/name)
      (db/valueType . db.type/string)
      (db/cardinality . db.cardinality/one)
      (db/index . #t)
      (db/fulltext . #t)
      (db/doc . "Artist name"))

    `((db/ident . artist/sortName)
      (db/valueType . db.type/string)
      (db/cardinality . db.cardinality/one)
      (db/doc . "Artist sort name (e.g. 'Beatles, The')"))

    `((db/ident . artist/type)
      (db/valueType . db.type/string)
      (db/cardinality . db.cardinality/one)
      (db/doc . "Artist type: person, group, orchestra, choir, character, other"))

    `((db/ident . artist/gender)
      (db/valueType . db.type/string)
      (db/cardinality . db.cardinality/one)
      (db/doc . "Artist gender: male, female, other"))

    `((db/ident . artist/country)
      (db/valueType . db.type/string)
      (db/cardinality . db.cardinality/one)
      (db/index . #t)
      (db/doc . "ISO 3166-1 country code"))

    `((db/ident . artist/startYear)
      (db/valueType . db.type/long)
      (db/cardinality . db.cardinality/one)
      (db/index . #t)
      (db/doc . "Year the artist started activity"))

    `((db/ident . artist/startMonth)
      (db/valueType . db.type/long)
      (db/cardinality . db.cardinality/one)
      (db/doc . "Month the artist started activity"))

    `((db/ident . artist/startDay)
      (db/valueType . db.type/long)
      (db/cardinality . db.cardinality/one)
      (db/doc . "Day the artist started activity"))

    `((db/ident . artist/endYear)
      (db/valueType . db.type/long)
      (db/cardinality . db.cardinality/one)
      (db/index . #t)
      (db/doc . "Year the artist ended activity"))

    `((db/ident . artist/endMonth)
      (db/valueType . db.type/long)
      (db/cardinality . db.cardinality/one)
      (db/doc . "Month the artist ended activity"))

    `((db/ident . artist/endDay)
      (db/valueType . db.type/long)
      (db/cardinality . db.cardinality/one)
      (db/doc . "Day the artist ended activity"))

    ;; ---- Release ----
    `((db/ident . release/gid)
      (db/valueType . db.type/string)
      (db/cardinality . db.cardinality/one)
      (db/unique . db.unique/identity)
      (db/index . #t)
      (db/doc . "Release MusicBrainz GUID"))

    `((db/ident . release/name)
      (db/valueType . db.type/string)
      (db/cardinality . db.cardinality/one)
      (db/index . #t)
      (db/fulltext . #t)
      (db/doc . "Release name"))

    `((db/ident . release/artists)
      (db/valueType . db.type/ref)
      (db/cardinality . db.cardinality/many)
      (db/doc . "Artists credited on this release"))

    `((db/ident . release/year)
      (db/valueType . db.type/long)
      (db/cardinality . db.cardinality/one)
      (db/index . #t)
      (db/doc . "Release year"))

    `((db/ident . release/month)
      (db/valueType . db.type/long)
      (db/cardinality . db.cardinality/one)
      (db/doc . "Release month"))

    `((db/ident . release/day)
      (db/valueType . db.type/long)
      (db/cardinality . db.cardinality/one)
      (db/doc . "Release day"))

    `((db/ident . release/status)
      (db/valueType . db.type/string)
      (db/cardinality . db.cardinality/one)
      (db/index . #t)
      (db/doc . "Release status: official, promotion, bootleg, pseudo-release"))

    `((db/ident . release/country)
      (db/valueType . db.type/string)
      (db/cardinality . db.cardinality/one)
      (db/index . #t)
      (db/doc . "Release country ISO code"))

    `((db/ident . release/barcode)
      (db/valueType . db.type/string)
      (db/cardinality . db.cardinality/one)
      (db/doc . "EAN/UPC barcode"))

    ;; ---- Medium ----
    `((db/ident . medium/release)
      (db/valueType . db.type/ref)
      (db/cardinality . db.cardinality/one)
      (db/doc . "The release this medium belongs to"))

    `((db/ident . medium/format)
      (db/valueType . db.type/string)
      (db/cardinality . db.cardinality/one)
      (db/doc . "Medium format: CD, vinyl, digital media, etc."))

    `((db/ident . medium/position)
      (db/valueType . db.type/long)
      (db/cardinality . db.cardinality/one)
      (db/doc . "Disc number within the release"))

    `((db/ident . medium/trackCount)
      (db/valueType . db.type/long)
      (db/cardinality . db.cardinality/one)
      (db/doc . "Number of tracks on this medium"))

    `((db/ident . medium/tracks)
      (db/valueType . db.type/ref)
      (db/cardinality . db.cardinality/many)
      (db/doc . "Tracks on this medium"))

    ;; ---- Track ----
    `((db/ident . track/name)
      (db/valueType . db.type/string)
      (db/cardinality . db.cardinality/one)
      (db/fulltext . #t)
      (db/doc . "Track name"))

    `((db/ident . track/position)
      (db/valueType . db.type/long)
      (db/cardinality . db.cardinality/one)
      (db/doc . "Track position on the medium"))

    `((db/ident . track/duration)
      (db/valueType . db.type/long)
      (db/cardinality . db.cardinality/one)
      (db/index . #t)
      (db/doc . "Track duration in milliseconds"))

    `((db/ident . track/artists)
      (db/valueType . db.type/ref)
      (db/cardinality . db.cardinality/many)
      (db/doc . "Artists performing on this track"))

    ;; ---- Label ----
    `((db/ident . label/gid)
      (db/valueType . db.type/string)
      (db/cardinality . db.cardinality/one)
      (db/unique . db.unique/identity)
      (db/index . #t)
      (db/doc . "Label MusicBrainz GUID"))

    `((db/ident . label/name)
      (db/valueType . db.type/string)
      (db/cardinality . db.cardinality/one)
      (db/index . #t)
      (db/doc . "Label name"))

    `((db/ident . label/sortName)
      (db/valueType . db.type/string)
      (db/cardinality . db.cardinality/one)
      (db/doc . "Label sort name"))

    `((db/ident . label/type)
      (db/valueType . db.type/string)
      (db/cardinality . db.cardinality/one)
      (db/doc . "Label type: original production, bootleg production, etc."))

    `((db/ident . label/country)
      (db/valueType . db.type/string)
      (db/cardinality . db.cardinality/one)
      (db/index . #t)
      (db/doc . "Label country ISO code"))

    `((db/ident . label/startYear)
      (db/valueType . db.type/long)
      (db/cardinality . db.cardinality/one)
      (db/doc . "Year label was founded"))

    `((db/ident . label/endYear)
      (db/valueType . db.type/long)
      (db/cardinality . db.cardinality/one)
      (db/doc . "Year label ceased operations"))

    ;; ---- Release-Label join ----
    `((db/ident . release/labels)
      (db/valueType . db.type/ref)
      (db/cardinality . db.cardinality/many)
      (db/doc . "Labels associated with this release"))))

;; ---- Installer ----

(def (install-mbrainz-schema! conn)
  (displayln "Installing MBrainz schema...")
  (let ([report (transact! conn (mbrainz-schema-ops))])
    (let ([n (length (tx-report-tx-data report))])
      (displayln "  Installed " (quotient n 3) " attributes (" n " datoms)"))
    report))

;; ---- If run as script ----

(def (main)
  (let ([conn (connect ":memory:")])
    (install-mbrainz-schema! conn)
    (let ([attrs (schema-for conn)])
      (displayln "  Schema attributes: " (length attrs))
      (displayln "  Sample: " (map db-attribute-ident (take attrs 5))))))

(main)
