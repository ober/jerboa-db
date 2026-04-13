(import (jerboa prelude)
        (jerboa-db core)
        (jerboa-db datom)
        (jerboa-db schema)
        (jerboa-db history)
        (jerboa-db entity)
        (jerboa-db spec)
        (jerboa-db value-store))

;; ---- Test harness ----

(def test-count 0)
(def pass-count 0)
(def fail-count 0)

(defrule (test name body ...)
  (begin
    (set! test-count (+ test-count 1))
    (guard (exn [#t (set! fail-count (+ fail-count 1))
                    (displayln "FAIL: " name)
                    (displayln "  Error: " (if (message-condition? exn)
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
  (unless expr
    (error 'assert-true "Expected true")))

(defrule (assert-false expr)
  (when expr
    (error 'assert-false "Expected false")))

;; Helper: setup a fresh conn with person schema
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
    conn))

;; ============================================================
;; Tests
;; ============================================================

(displayln "")
(displayln "=== Jerboa-DB Core Tests ===")
(displayln "")

;; ---- Connection and basic setup ----

(test "connect creates in-memory database"
  (let* ([conn (connect ":memory:")]
         [d (db conn)])
    (assert-true (connection? conn))
    (assert-true (db-value? d))
    (assert-equal (db-value-basis-tx d) 0)))

;; ---- Schema installation ----

(test "transact schema attributes"
  (let* ([conn (connect ":memory:")]
         [_ (transact! conn
              (list
                `((db/ident . person/name)
                  (db/valueType . db.type/string)
                  (db/cardinality . db.cardinality/one))
                `((db/ident . person/age)
                  (db/valueType . db.type/long)
                  (db/cardinality . db.cardinality/one))
                `((db/ident . person/email)
                  (db/valueType . db.type/string)
                  (db/cardinality . db.cardinality/one)
                  (db/unique . db.unique/identity)
                  (db/index . #t))))]
         [d (db conn)]
         [schema (db-value-schema d)]
         [attr (schema-lookup-by-ident schema 'person/name)])
    (assert-true (db-attribute? attr))
    (assert-equal (db-attribute-value-type attr) 'db.type/string)
    (assert-equal (db-attribute-cardinality attr) 'db.cardinality/one)))

;; ---- Entity creation ----

(test "transact entities with tempids"
  (let* ([conn (make-person-db)]
         [tid1 (tempid)]
         [tid2 (tempid)]
         [report (transact! conn
                   (list
                     `((db/id . ,tid1) (person/name . "Alice") (person/age . 30))
                     `((db/id . ,tid2) (person/name . "Bob") (person/age . 25))))]
         [alice-eid (cdr (assv tid1 (tx-report-tempids report)))]
         [bob-eid (cdr (assv tid2 (tx-report-tempids report)))])
    (assert-true (tx-report? report))
    (assert-true (pair? (tx-report-tempids report)))
    (assert-true (> alice-eid 0))
    (assert-true (> bob-eid 0))
    (assert-true (not (= alice-eid bob-eid)))))

;; ---- Datalog queries ----

(test "basic two-clause query"
  (let* ([conn (make-person-db)]
         [_ (transact! conn
              (list
                `((person/name . "Alice") (person/age . 30))
                `((person/name . "Bob") (person/age . 25))
                `((person/name . "Carol") (person/age . 42))))]
         [results (q '((find ?name ?age)
                       (where (?e person/name ?name)
                              (?e person/age ?age)
                              ((> ?age 30))))
                     (db conn))])
    (assert-equal (length results) 1)
    (assert-equal (car results) '("Carol" 42))))

(test "parameterized query with :in"
  (let* ([conn (make-person-db)]
         [_ (transact! conn
              (list
                `((person/name . "Alice") (person/age . 30))
                `((person/name . "Bob") (person/age . 25))
                `((person/name . "Carol") (person/age . 42))))]
         [results (q '((find ?name)
                       (in $ ?min-age)
                       (where (?e person/name ?name)
                              (?e person/age ?age)
                              ((>= ?age ?min-age))))
                     (db conn) 30)])
    (assert-equal (length results) 2)))

;; ---- Cardinality/one auto-retraction ----

(test "cardinality/one auto-retracts old value"
  (let* ([conn (make-person-db)]
         [tid (tempid)]
         [r1 (transact! conn (list `((db/id . ,tid) (person/name . "Alice"))))]
         [eid (cdr (assv tid (tx-report-tempids r1)))]
         [_ (transact! conn (list `((db/id . ,eid) (person/name . "Alicia"))))]
         [results (q '((find ?name) (where (?e person/name ?name))) (db conn))])
    (assert-equal (length results) 1)
    (assert-equal (caar results) "Alicia")))

;; ---- Pull API ----

(test "pull simple attributes"
  (let* ([conn (make-person-db)]
         [tid (tempid)]
         [r (transact! conn
              (list `((db/id . ,tid) (person/name . "Alice") (person/age . 30))))]
         [eid (cdr (assv tid (tx-report-tempids r)))]
         [result (pull (db conn) '(person/name person/age) eid)])
    (assert-equal (cdr (assq 'person/name result)) "Alice")
    (assert-equal (cdr (assq 'person/age result)) 30)))

;; ---- Time-travel ----

(test "as-of returns historical state"
  (let* ([conn (make-person-db)]
         [tid (tempid)]
         [r1 (transact! conn (list `((db/id . ,tid) (person/name . "Alice"))))]
         [eid (cdr (assv tid (tx-report-tempids r1)))]
         [tx1 (db-value-basis-tx (tx-report-db-after r1))]
         [_ (transact! conn (list `((db/id . ,eid) (person/name . "Alicia"))))]
         [current (q '((find ?name) (where (?e person/name ?name))) (db conn))]
         [historical (q '((find ?name) (where (?e person/name ?name)))
                        (as-of (db conn) tx1))])
    (assert-equal (caar current) "Alicia")
    (assert-equal (caar historical) "Alice")))

;; ---- Entity API ----

(test "entity lazy access"
  (let* ([conn (make-person-db)]
         [tid (tempid)]
         [r (transact! conn
              (list `((db/id . ,tid) (person/name . "Alice") (person/age . 30))))]
         [eid (cdr (assv tid (tx-report-tempids r)))]
         [ent (entity (db conn) eid)])
    (assert-true (entity-map? ent))
    (assert-equal (entity-get ent 'person/name) "Alice")
    (assert-equal (entity-get ent 'person/age) 30)))

;; ---- Retraction ----

(test "explicit retraction"
  (let* ([conn (make-person-db)]
         [tid (tempid)]
         [r (transact! conn (list `((db/id . ,tid) (person/name . "Alice"))))]
         [eid (cdr (assv tid (tx-report-tempids r)))]
         [_ (transact! conn (list `(db/retract ,eid person/name "Alice")))]
         [results (q '((find ?name) (where (?e person/name ?name))) (db conn))])
    (assert-equal (length results) 0)))

;; ---- Entity retraction ----

(test "retract entire entity"
  (let* ([conn (make-person-db)]
         [tid (tempid)]
         [r (transact! conn
              (list `((db/id . ,tid) (person/name . "Alice") (person/age . 30))))]
         [eid (cdr (assv tid (tx-report-tempids r)))]
         [_ (transact! conn (list `(db/retractEntity ,eid)))]
         [results (q '((find ?name) (where (?e person/name ?name))) (db conn))])
    (assert-equal (length results) 0)))

;; ---- History ----

(test "history shows all datoms including retracted"
  (let* ([conn (make-person-db)]
         [tid (tempid)]
         [r (transact! conn (list `((db/id . ,tid) (person/name . "Alice"))))]
         [eid (cdr (assv tid (tx-report-tempids r)))]
         [_ (transact! conn (list `((db/id . ,eid) (person/name . "Alicia"))))]
         [results (q '((find ?name ?tx ?added)
                       (where (?e person/name ?name ?tx ?added)))
                     (history (db conn)))])
    ;; Should have at least 3 datoms: Alice assert, Alice retract, Alicia assert
    (assert-true (>= (length results) 3))))

;; ---- Aggregation ----

(test "count aggregate"
  (let* ([conn (make-person-db)]
         [_ (transact! conn
              (list
                `((person/name . "Alice") (person/age . 30))
                `((person/name . "Bob") (person/age . 25))
                `((person/name . "Carol") (person/age . 42))))]
         [results (q '((find (count ?e))
                       (where (?e person/name ?name)))
                     (db conn))])
    (assert-equal (caar results) 3)))

;; ---- db-stats ----

(test "db-stats returns index sizes"
  (let* ([conn (make-person-db)]
         [_ (transact! conn (list `((person/name . "Alice"))))]
         [stats (db-stats conn)])
    (assert-true (> (cdr (assq 'eavt-count stats)) 0))))

;; ---- Compare-and-swap ----

(test "CAS succeeds when current value matches"
  (let* ([conn (make-person-db)]
         [tid (tempid)]
         [r (transact! conn (list `((db/id . ,tid) (person/age . 30))))]
         [eid (cdr (assv tid (tx-report-tempids r)))]
         [_ (transact! conn (list `(db/cas ,eid person/age 30 31)))]
         [results (q '((find ?age) (where (?e person/age ?age))) (db conn))])
    (assert-equal (caar results) 31)))

;; ---- Not clause ----

(test "not clause excludes matching entities"
  (let* ([conn (make-person-db)]
         [_ (transact! conn
              (list
                `((db/ident . person/banned)
                  (db/valueType . db.type/boolean)
                  (db/cardinality . db.cardinality/one))))]
         [_ (transact! conn
              (list
                `((person/name . "Alice") (person/age . 30))
                `((person/name . "Bob") (person/age . 25) (person/banned . #t))
                `((person/name . "Carol") (person/age . 42))))]
         [results (q '((find ?name)
                       (where (?e person/name ?name)
                              (not (?e person/banned #t))))
                     (db conn))])
    (assert-equal (length results) 2)
    ;; Alice and Carol should be present, Bob excluded
    (assert-true (member '("Alice") results))
    (assert-true (member '("Carol") results))
    (assert-false (member '("Bob") results))))

;; ---- Or clause ----

(test "or clause unions alternatives"
  (let* ([conn (make-person-db)]
         [_ (transact! conn
              (list
                `((person/name . "Alice") (person/age . 30))
                `((person/name . "Bob") (person/age . 25))
                `((person/name . "Carol") (person/age . 42))))]
         [results (q '((find ?name)
                       (where (or (?e person/name "Alice")
                                  (?e person/name "Carol"))
                              (?e person/name ?name)))
                     (db conn))])
    (assert-equal (length results) 2)
    (assert-true (member '("Alice") results))
    (assert-true (member '("Carol") results))))

;; ---- Collection binding ----

(test "collection binding with (?x ...)"
  (let* ([conn (make-person-db)]
         [_ (transact! conn
              (list
                `((person/name . "Alice") (person/age . 30))
                `((person/name . "Bob") (person/age . 25))
                `((person/name . "Carol") (person/age . 42))))]
         [results (q '((find ?name ?age)
                       (in $ (?name ...))
                       (where (?e person/name ?name)
                              (?e person/age ?age)))
                     (db conn) '("Alice" "Carol"))])
    (assert-equal (length results) 2)
    (assert-true (member '("Alice" 30) results))
    (assert-true (member '("Carol" 42) results))))

;; ---- Tuple binding ----

(test "tuple binding with (?x ?y)"
  (let* ([conn (make-person-db)]
         [_ (transact! conn
              (list
                `((person/name . "Alice") (person/age . 30))
                `((person/name . "Bob") (person/age . 25))))]
         [results (q '((find ?name ?age)
                       (in $ (?name ?min-age))
                       (where (?e person/name ?name)
                              (?e person/age ?age)
                              ((>= ?age ?min-age))))
                     (db conn) '("Alice" 25))])
    (assert-equal (length results) 1)
    (assert-equal (car results) '("Alice" 30))))

;; ---- Relation binding ----

(test "relation binding with ((?x ?y))"
  (let* ([conn (make-person-db)]
         [_ (transact! conn
              (list
                `((person/name . "Alice") (person/age . 30))
                `((person/name . "Bob") (person/age . 25))
                `((person/name . "Carol") (person/age . 42))))]
         [results (q '((find ?name ?age)
                       (in $ ((?name ?age)))
                       (where (?e person/name ?name)
                              (?e person/age ?age)))
                     (db conn) '(("Alice" 30) ("Carol" 42)))])
    (assert-equal (length results) 2)
    (assert-true (member '("Alice" 30) results))
    (assert-true (member '("Carol" 42) results))))

;; ---- Built-in predicates ----

(test "built-in predicates zero? pos? neg? even? odd?"
  (let* ([conn (make-person-db)]
         [_ (transact! conn
              (list
                `((person/name . "Alice") (person/age . 30))
                `((person/name . "Bob") (person/age . 25))
                `((person/name . "Carol") (person/age . 42))))]
         [even-results (q '((find ?name)
                           (where (?e person/name ?name)
                                  (?e person/age ?age)
                                  ((even? ?age))))
                         (db conn))]
         [odd-results (q '((find ?name)
                          (where (?e person/name ?name)
                                 (?e person/age ?age)
                                 ((odd? ?age))))
                        (db conn))])
    (assert-equal (length even-results) 2)  ;; 30 and 42 are even
    (assert-equal (length odd-results) 1)))  ;; 25 is odd

(test "string predicates"
  (let* ([conn (make-person-db)]
         [_ (transact! conn
              (list
                `((person/name . "Alice"))
                `((person/name . "Alicia"))
                `((person/name . "Bob"))))]
         [results (q '((find ?name)
                       (where (?e person/name ?name)
                              ((string-starts-with? ?name "Ali"))))
                     (db conn))])
    (assert-equal (length results) 2)))

;; ---- Built-in functions ----

(test "built-in functions str, upper-case, inc"
  (let* ([conn (make-person-db)]
         [_ (transact! conn
              (list `((person/name . "Alice") (person/age . 30))))]
         [results (q '((find ?upper ?next-age)
                       (where (?e person/name ?name)
                              (?e person/age ?age)
                              ((upper-case ?name) ?upper)
                              ((inc ?age) ?next-age)))
                     (db conn))])
    (assert-equal (length results) 1)
    (assert-equal (car results) '("ALICE" 31))))

;; ---- Aggregates ----

(test "count-distinct aggregate"
  (let* ([conn (make-person-db)]
         [_ (transact! conn
              (list
                `((person/name . "Alice") (person/age . 30))
                `((person/name . "Bob") (person/age . 30))
                `((person/name . "Carol") (person/age . 42))))]
         [results (q '((find (count-distinct ?age))
                       (where (?e person/age ?age)))
                     (db conn))])
    (assert-equal (caar results) 2)))  ;; 30 and 42

(test "median aggregate"
  (let* ([conn (make-person-db)]
         [_ (transact! conn
              (list
                `((person/name . "Alice") (person/age . 30))
                `((person/name . "Bob") (person/age . 25))
                `((person/name . "Carol") (person/age . 42))))]
         [results (q '((find (median ?age))
                       (where (?e person/age ?age)))
                     (db conn))])
    (assert-equal (caar results) 30)))  ;; median of 25,30,42

;; ---- Variance and stddev aggregates ----

(test "variance aggregate (population)"
  (let* ([conn (make-person-db)]
         [_ (transact! conn
              (list
                `((person/name . "Alice") (person/age . 10))
                `((person/name . "Bob") (person/age . 20))
                `((person/name . "Carol") (person/age . 30))))]
         ;; population variance of 10,20,30: mean=20, ss=(100+0+100)=200, var=200/3≈66.67
         [results (q '((find (variance ?age))
                       (where (?e person/age ?age)))
                     (db conn))])
    (let ([v (caar results)])
      (assert-true (> v 66.0))
      (assert-true (< v 67.0)))))

(test "stddev aggregate (population)"
  (let* ([conn (make-person-db)]
         [_ (transact! conn
              (list
                `((person/name . "Alice") (person/age . 10))
                `((person/name . "Bob") (person/age . 20))
                `((person/name . "Carol") (person/age . 30))))]
         ;; stddev = sqrt(200/3) ≈ 8.165
         [results (q '((find (stddev ?age))
                       (where (?e person/age ?age)))
                     (db conn))])
    (let ([s (caar results)])
      (assert-true (> s 8.0))
      (assert-true (< s 8.5)))))

(test "variance-sample aggregate (Bessel-corrected)"
  (let* ([conn (make-person-db)]
         [_ (transact! conn
              (list
                `((person/name . "Alice") (person/age . 10))
                `((person/name . "Bob") (person/age . 20))
                `((person/name . "Carol") (person/age . 30))))]
         ;; sample variance of 10,20,30: ss=200, denom=2, var=100
         [results (q '((find (variance-sample ?age))
                       (where (?e person/age ?age)))
                     (db conn))])
    (assert-equal (caar results) 100.0)))

;; ---- Lookup refs ----

(test "lookup ref in transaction"
  (let* ([conn (make-person-db)]
         [_ (transact! conn
              (list
                `((db/ident . person/email)
                  (db/valueType . db.type/string)
                  (db/cardinality . db.cardinality/one)
                  (db/unique . db.unique/identity)
                  (db/index . #t))))]
         [_ (transact! conn
              (list
                `((person/name . "Alice") (person/email . "alice@example.com") (person/age . 30))))]
         ;; Update using lookup ref
         [_ (transact! conn
              (list
                `((db/id . (person/email "alice@example.com")) (person/age . 31))))]
         [results (q '((find ?name ?age)
                       (where (?e person/name ?name)
                              (?e person/age ?age)))
                     (db conn))])
    (assert-equal (length results) 1)
    (assert-equal (car results) '("Alice" 31))))

;; ---- Explain query ----

(test "explain-query returns plan without executing"
  (let* ([conn (make-person-db)]
         [plan (explain-query
                 '((find ?name ?age)
                   (where (?e person/name ?name)
                          (?e person/age ?age)
                          ((> ?age 30))))
                 (db conn))])
    (assert-true (pair? plan))
    ;; Plan should have find, in, and plan sections
    (assert-equal (caar plan) 'find)
    (assert-equal (caadr plan) 'in)
    (assert-equal (caaddr plan) 'plan)))

;; ============================================================
;; Phase 8 Tests — Advanced Features
;; ============================================================

;; ---- Composite tuples ----

(test "composite tuple auto-generated when components transacted"
  (let* ([conn (connect ":memory:")]
         ;; Schema: component attrs + composite
         [_ (transact! conn
              (list
                `((db/ident . order/customer)
                  (db/valueType . db.type/string)
                  (db/cardinality . db.cardinality/one))
                `((db/ident . order/product)
                  (db/valueType . db.type/string)
                  (db/cardinality . db.cardinality/one))
                `((db/ident . order/by-cust-prod)
                  (db/valueType . db.type/tuple)
                  (db/cardinality . db.cardinality/one)
                  (db/tupleAttrs . (order/customer order/product)))))]
         ;; Transact entity with both components
         [tid (tempid)]
         [r (transact! conn
              (list `((db/id . ,tid)
                      (order/customer . "alice")
                      (order/product  . "widget"))))]
         [eid (cdr (assv tid (tx-report-tempids r)))]
         ;; Query for composite attribute
         [results (q '((find ?composite)
                        (where (?e order/by-cust-prod ?composite)))
                     (db conn))])
    (assert-equal (length results) 1)
    (assert-equal (caar results) '("alice" "widget"))))

;; ---- Entity specs ----

(test "entity spec validation passes when all attrs present"
  (let* ([conn (make-person-db)]
         ;; Add email attr
         [_ (transact! conn
              (list `((db/ident . person/email)
                      (db/valueType . db.type/string)
                      (db/cardinality . db.cardinality/one)
                      (db/unique . db.unique/identity)
                      (db/index . #t))))]
         ;; Define spec
         [_ (transact! conn (list (define-spec 'person-spec '(person/name person/email))))]
         ;; Transact complete entity
         [tid (tempid)]
         [r (transact! conn
              (list `((db/id . ,tid)
                      (person/name . "Alice")
                      (person/email . "alice@example.com"))))]
         [eid (cdr (assv tid (tx-report-tempids r)))]
         [result (check-entity-spec (db conn) eid 'person-spec)])
    (assert-equal (car result) 'ok)
    (assert-equal (cdr result) eid)))

(test "entity spec validation fails when attr missing"
  (let* ([conn (make-person-db)]
         [_ (transact! conn
              (list `((db/ident . person/email)
                      (db/valueType . db.type/string)
                      (db/cardinality . db.cardinality/one))))]
         [_ (transact! conn (list (define-spec 'person-spec '(person/name person/email))))]
         ;; Entity missing person/email
         [tid (tempid)]
         [r (transact! conn (list `((db/id . ,tid) (person/name . "Bob"))))]
         [eid (cdr (assv tid (tx-report-tempids r)))]
         [result (check-entity-spec (db conn) eid 'person-spec)])
    (assert-equal (car result) 'err)
    ;; missing list should contain person/email
    (assert-true (member 'person/email (cdr result)))))

;; ---- Fulltext search ----

(test "fulltext search finds entities with matching word"
  (let* ([conn (connect ":memory:")]
         [_ (transact! conn
              (list `((db/ident . article/body)
                      (db/valueType . db.type/string)
                      (db/cardinality . db.cardinality/one)
                      (db/fulltext . #t))))]
         [tid1 (tempid)]
         [tid2 (tempid)]
         [r (transact! conn
              (list
                `((db/id . ,tid1) (article/body . "Clojure is a functional language"))
                `((db/id . ,tid2) (article/body . "Python is popular for data science"))))]
         [eid1 (cdr (assv tid1 (tx-report-tempids r)))]
         [results (fulltext-search conn 'article/body "clojure")])
    (assert-equal (length results) 1)
    (assert-equal (caar results) eid1)))

(test "fulltext search is case insensitive"
  (let* ([conn (connect ":memory:")]
         [_ (transact! conn
              (list `((db/ident . article/title)
                      (db/valueType . db.type/string)
                      (db/cardinality . db.cardinality/one)
                      (db/fulltext . #t))))]
         [tid (tempid)]
         [r (transact! conn (list `((db/id . ,tid) (article/title . "Advanced Scheme Techniques"))))]
         [eid (cdr (assv tid (tx-report-tempids r)))]
         ;; Search with uppercase
         [results (fulltext-search conn 'article/title "SCHEME")])
    (assert-equal (length results) 1)
    (assert-equal (caar results) eid)))

;; ---- Datom GC ----

(test "gc-collect! removes retracted no-history datoms"
  (let* ([conn (connect ":memory:")]
         [_ (transact! conn
              (list `((db/ident . session/token)
                      (db/valueType . db.type/string)
                      (db/cardinality . db.cardinality/one)
                      (db/noHistory . #t))))]
         [tid (tempid)]
         [r (transact! conn (list `((db/id . ,tid) (session/token . "abc123"))))]
         [eid (cdr (assv tid (tx-report-tempids r)))]
         ;; Retract the token
         [_ (transact! conn (list `(db/retract ,eid session/token "abc123")))]
         ;; Verify it's retracted (should not appear in current query)
         [before-gc (q '((find ?tok) (where (?e session/token ?tok))) (db conn))]
         ;; Run GC
         [gc-result (gc-collect! conn)]
         ;; Stats should show removed datoms
         [removed (cdr (assq 'removed gc-result))])
    (assert-equal (length before-gc) 0)  ;; already not visible
    (assert-true (> removed 0))))        ;; GC removed the datom pairs

;; ---- Value store deduplication ----

(test "value store deduplicates repeated values"
  (let* ([vs (make-value-store)]
         [hash1 (value-store-put! vs "hello world")]
         [hash2 (value-store-put! vs "hello world")]
         [hash3 (value-store-put! vs "different value")]
         [stats (value-store-stats vs)])
    ;; Same value -> same hash
    (assert-equal hash1 hash2)
    ;; Different value -> different hash
    (assert-true (not (equal? hash1 hash3)))
    ;; Dedup count should be 1 (second "hello world" was deduplicated)
    (assert-equal (cdr (assq 'dedup-saves stats)) 1)
    ;; Retrieval works
    (assert-equal (value-store-get vs hash1) "hello world")
    (assert-equal (value-store-get vs hash3) "different value")))

;; ============================================================
;; Report
;; ============================================================

(displayln "")
(displayln "=== Results ===")
(displayln (str "Total: " test-count " | Passed: " pass-count " | Failed: " fail-count))
(displayln "")

(when (> fail-count 0)
  (exit 1))
