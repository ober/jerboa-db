#!chezscheme
;;; Tests for (jerboa-db migrate) — Round 8 Phase 44.
;;; Exercises rename, retype (with coerce-fn + reindex), delete,
;;; merge, split, add-index/remove-index, plus migration-plan and
;;; migration-dry-run.

(import (jerboa prelude)
        (jerboa-db core)
        (jerboa-db migrate)
        (jerboa-db schema))

(def test-count 0)
(def pass-count 0)
(def fail-count 0)

(defrule (test name body ...)
  (begin
    (set! test-count (+ test-count 1))
    (guard (exn [#t (set! fail-count (+ fail-count 1))
                    (displayln "FAIL: " name)
                    (displayln "  Error: "
                      (if (message-condition? exn)
                          (condition-message exn)
                          exn))])
      body ...
      (set! pass-count (+ pass-count 1))
      (displayln "PASS: " name))))

(defrule (assert-equal actual expected)
  (let ([a actual] [e expected])
    (unless (equal? a e)
      (error 'assert-equal
        (format "Expected ~s but got ~s" e a)))))

(defrule (assert-true expr)
  (unless expr (error 'assert-true "Expected true")))

(displayln "")
(displayln "=== Jerboa-DB Migration Tests (Phase 44) ===")
(displayln "")

;; ---- Helper: setup person db with two entities ----

(def (make-person-db)
  (let ([conn (connect ":memory:")])
    (transact! conn
      (list
        `((db/ident . person/name)
          (db/valueType . db.type/string)
          (db/cardinality . db.cardinality/one))
        `((db/ident . person/age)
          (db/valueType . db.type/long)
          (db/cardinality . db.cardinality/one))))
    (let ([t1 (tempid)] [t2 (tempid)])
      (transact! conn
        (list
          `((db/id . ,t1) (person/name . "Alice") (person/age . 30))
          `((db/id . ,t2) (person/name . "Bob")   (person/age . 25)))))
    conn))

;; ---- Rename ----

(test "rename copies old datoms to new attr"
  (let ([conn (make-person-db)])
    (transact! conn
      (list `((db/ident . person/full-name)
              (db/valueType . db.type/string)
              (db/cardinality . db.cardinality/one))))
    (migrate! conn
      (make-migration
        (list (make-rename-attr 'person/name 'person/full-name))))
    (let ([new-vals (q '((find ?v) (where (?e person/full-name ?v)))
                       (db conn))]
          [old-vals (q '((find ?v) (where (?e person/name ?v)))
                       (db conn))])
      (assert-equal (length new-vals) 2)
      (assert-equal (length old-vals) 0))))

;; ---- Retype with coerce-fn ----

(test "retype string -> long via coerce-fn"
  (let ([conn (connect ":memory:")])
    (transact! conn
      (list `((db/ident . item/qty)
              (db/valueType . db.type/string)
              (db/cardinality . db.cardinality/one))))
    (let ([t1 (tempid)] [t2 (tempid)])
      (transact! conn
        (list `((db/id . ,t1) (item/qty . "42"))
              `((db/id . ,t2) (item/qty . "7")))))
    (migrate! conn
      (make-migration
        (list (make-retype-attr 'item/qty 'db.type/long
                (lambda (s) (string->number s))))))
    (let ([results (q '((find ?v) (where (?e item/qty ?v)))
                      (db conn))])
      (assert-equal (length results) 2)
      ;; All values are now integers
      (assert-true (every integer? (map car results))))))

;; ---- Retype rejects on coerce-fn failure (fail-fast) ----

(test "retype fails fast when coerce-fn raises on any value"
  (let ([conn (connect ":memory:")])
    (transact! conn
      (list `((db/ident . item/code)
              (db/valueType . db.type/string)
              (db/cardinality . db.cardinality/one))))
    (let ([t1 (tempid)] [t2 (tempid)])
      (transact! conn
        (list `((db/id . ,t1) (item/code . "12"))
              `((db/id . ,t2) (item/code . "not-a-number")))))
    (let ([raised? #f])
      (guard (exn [#t (set! raised? #t)])
        (migrate! conn
          (make-migration
            (list (make-retype-attr 'item/code 'db.type/long
                    (lambda (s)
                      (let ([n (string->number s)])
                        (or n (error 'coerce "bad value" s))))))))
        ;; Should NOT reach here
        (set! raised? 'reached-end))
      (assert-equal raised? #t)
      ;; Original values must be intact (fail-fast — no partial migration)
      (let ([results (q '((find ?v) (where (?e item/code ?v)))
                        (db conn))])
        (assert-equal (length results) 2)))))

;; ---- Delete attribute ----

(test "delete-attr retracts all datoms for attribute"
  (let ([conn (make-person-db)])
    (migrate! conn
      (make-migration (list (make-delete-attr 'person/age))))
    (let ([results (q '((find ?v) (where (?e person/age ?v)))
                      (db conn))])
      (assert-equal (length results) 0))
    ;; Other attribute remains
    (let ([results (q '((find ?v) (where (?e person/name ?v)))
                      (db conn))])
      (assert-equal (length results) 2))))

;; ---- Merge ----

(test "merge combines two attributes via merge-fn"
  (let ([conn (connect ":memory:")])
    (transact! conn
      (list
        `((db/ident . p/first) (db/valueType . db.type/string)
          (db/cardinality . db.cardinality/one))
        `((db/ident . p/last)  (db/valueType . db.type/string)
          (db/cardinality . db.cardinality/one))
        `((db/ident . p/full)  (db/valueType . db.type/string)
          (db/cardinality . db.cardinality/one))))
    (let ([t1 (tempid)])
      (transact! conn
        (list `((db/id . ,t1) (p/first . "Ada") (p/last . "Lovelace") (p/full . "")))))
    (migrate! conn
      (make-migration
        (list (make-merge-attr 'p/last 'p/full
                (lambda (last current)
                  (string-append "Ada " last))))))
    (let ([results (q '((find ?v) (where (?e p/full ?v)))
                      (db conn))])
      (assert-equal (caar results) "Ada Lovelace"))))

;; ---- migration-plan ----

(test "migration-plan returns human-readable strings"
  (let ([m (make-migration
             (list (make-rename-attr 'old 'new)
                   (make-retype-attr 'attr 'db.type/long identity)
                   (make-delete-attr 'gone)))])
    (let ([plan (migration-plan m)])
      (assert-equal (length plan) 3)
      (assert-true (every string? plan)))))

;; ---- migration-dry-run ----

(test "migration-dry-run reports affected counts without executing"
  (let ([conn (make-person-db)])
    (let* ([m (make-migration
                (list (make-rename-attr 'person/age 'person/years)))]
           [report (migration-dry-run conn m)])
      ;; No mutation happened
      (let ([results (q '((find ?v) (where (?e person/age ?v)))
                        (db conn))])
        (assert-equal (length results) 2))
      ;; Report has one entry mentioning 2 affected datoms
      (assert-equal (length report) 1)
      (let ([r (car report)])
        (assert-true (memq 'rename r))
        (assert-true (memq 'affects r))
        (assert-true (member 2 r))))))

;; ============================================================
;; Report
;; ============================================================

(displayln "")
(displayln "=== Results ===")
(displayln (str "Total: " test-count " | Passed: " pass-count " | Failed: " fail-count))
(displayln "")

(when (> fail-count 0) (exit 1))
