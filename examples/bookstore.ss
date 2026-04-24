;;; examples/bookstore.ss — Clojure-parity showcase for Jerboa-DB
;;;
;;; A 12-section tour that exercises the Clojure-parity library surface
;;; together with the Datomic-style store. Run with:
;;;
;;;   make showcase
;;;       ... or directly:
;;;   scheme --libdirs lib:$HOME/mine/jerboa/lib --script examples/bookstore.ss
;;;
;;; The script prints a numbered banner before each section so you can
;;; follow along. A successful run exits 0 with every section printing.

(import (except (jerboa prelude) defmethod)
        (jerboa-db core)
        (jerboa-db history)
        (std misc atom)
        (std pmap)
        (std stm)
        (std agent)
        (std protocol)
        (std multi)
        (std spec)
        (std transducer))

;; ---------------------------------------------------------------------
;; Tiny section-print helper.
;; ---------------------------------------------------------------------

(def (section n title)
  (newline)
  (displayln "------------------------------------------------------------")
  (displayln (str "Section " n ": " title))
  (displayln "------------------------------------------------------------"))

;; =====================================================================
;; Section 1 — Schema + seed transact (jerboa-db)
;; =====================================================================

(section 1 "Schema + seed transact")

(def conn (connect ":memory:"))

(transact! conn
  (list
    '((db/ident . book/title)
      (db/valueType . db.type/string)
      (db/cardinality . db.cardinality/one))
    '((db/ident . book/author)
      (db/valueType . db.type/string)
      (db/cardinality . db.cardinality/one))
    '((db/ident . book/price)
      (db/valueType . db.type/long)
      (db/cardinality . db.cardinality/one))
    '((db/ident . book/stock)
      (db/valueType . db.type/long)
      (db/cardinality . db.cardinality/one))
    '((db/ident . book/isbn)
      (db/valueType . db.type/string)
      (db/cardinality . db.cardinality/one)
      (db/unique . db.unique/identity)
      (db/index . #t))
    '((db/ident . book/banned)
      (db/valueType . db.type/boolean)
      (db/cardinality . db.cardinality/one))))

(def t1 (tempid))
(def t2 (tempid))
(def t3 (tempid))
(def t4 (tempid))
(def seed-report
  (transact! conn
    (list
      `((db/id . ,t1) (book/title . "Red Mars")    (book/author . "Kim Stanley Robinson")
        (book/price . 15) (book/stock . 4) (book/isbn . "978-0553560732"))
      `((db/id . ,t2) (book/title . "Green Mars")  (book/author . "Kim Stanley Robinson")
        (book/price . 15) (book/stock . 2) (book/isbn . "978-0553572391"))
      `((db/id . ,t3) (book/title . "Blue Mars")   (book/author . "Kim Stanley Robinson")
        (book/price . 16) (book/stock . 0) (book/isbn . "978-0553573350"))
      `((db/id . ,t4) (book/title . "Dhalgren")    (book/author . "Samuel R. Delany")
        (book/price . 18) (book/stock . 3) (book/isbn . "978-0375706684")
        (book/banned . #t)))))

(displayln "seeded "
  (length (tx-report-tempids seed-report))
  " books; basis tx = "
  (db-value-basis-tx (tx-report-db-after seed-report)))

(def red-mars-eid   (cdr (assv t1 (tx-report-tempids seed-report))))
(def green-mars-eid (cdr (assv t2 (tx-report-tempids seed-report))))
(def blue-mars-eid  (cdr (assv t3 (tx-report-tempids seed-report))))
(def dhalgren-eid   (cdr (assv t4 (tx-report-tempids seed-report))))

;; =====================================================================
;; Section 2 — Datalog :in + not clause + variance aggregate
;; =====================================================================

(section 2 "Datalog query with :in, not clause, and variance")

(def in-stock-under
  (q '((find ?title ?price ?stock)
       (in $ ?max-price)
       (where (?e book/title ?title)
              (?e book/price ?price)
              (?e book/stock ?stock)
              ((<= ?price ?max-price))
              ((> ?stock 0))
              (not (?e book/banned #t))))
     (db conn) 16))

(displayln "in-stock books priced <= 16, not banned:")
(for-each displayln in-stock-under)

(def price-variance
  (q '((find (variance ?price))
       (where (?e book/title ?title)
              (?e book/price ?price)))
     (db conn)))

(displayln "population variance of catalog prices: " (caar price-variance))

;; =====================================================================
;; Section 3 — Pull + as-of time travel
;; =====================================================================

(section 3 "Pull API + as-of time travel")

(def basis-after-seed
  (db-value-basis-tx (tx-report-db-after seed-report)))

;; Mutate one book: reduce Red Mars stock and reprice.
(transact! conn
  (list
    `((db/id . ,red-mars-eid)
      (book/stock . 1)
      (book/price . 14))))

(def red-now  (pull (db conn) '(book/title book/price book/stock) red-mars-eid))
(def red-then (pull (as-of (db conn) basis-after-seed)
                    '(book/title book/price book/stock) red-mars-eid))

(displayln "Red Mars NOW:  " red-now)
(displayln "Red Mars THEN: " red-then)

;; =====================================================================
;; Section 4 — Persistent map from query results
;; =====================================================================

(section 4 "Persistent map built from query results")

(def catalog-tuples
  (q '((find ?isbn ?title ?stock)
       (where (?e book/isbn ?isbn)
              (?e book/title ?title)
              (?e book/stock ?stock)))
     (db conn)))

(def catalog-pmap
  (fold-left
    (lambda (acc row)
      (let ([isbn  (car row)]
            [title (cadr row)]
            [stock (caddr row)])
        (persistent-map-set acc isbn (list title stock))))
    pmap-empty
    catalog-tuples))

(displayln "catalog size: " (persistent-map-size catalog-pmap))
(displayln "lookup by ISBN 978-0553560732 -> "
  (persistent-map-ref catalog-pmap "978-0553560732"))

;; =====================================================================
;; Section 5 — Atom with validator (per-session cart)
;; =====================================================================

(section 5 "Atom with validator (cart total under $100)")

(def cart (atom '()))

(set-validator! cart
  (lambda (items)
    (<= (apply + (map cadr items)) 100)))

(def (add-to-cart item)
  (lambda (items) (cons item items)))

(swap! cart (add-to-cart (list "Red Mars"   14)))
(swap! cart (add-to-cart (list "Green Mars" 15)))
(displayln "cart after 2 adds:  " (deref cart))

(def reject-result
  (guard (exn [else 'rejected])
    (swap! cart (add-to-cart (list "Expensive Encyclopedia" 200)))
    'accepted))

(displayln "attempt to add $200 item -> " reject-result)
(displayln "cart unchanged: " (deref cart))

;; =====================================================================
;; Section 6 — STM dosync (transactional inventory decrement + audit)
;; =====================================================================

(section 6 "STM: atomic inventory decrement + audit log (with io!)")

(def audit-log (make-ref '()))

(def (checkout-book! title price delta)
  ;; Look up current stock from the DB, then decrement atomically via STM.
  (let* ([eid (car (car (q '((find ?e)
                             (in $ ?t)
                             (where (?e book/title ?t)))
                           (db conn) title)))]
         [now-stock (car (car (q '((find ?s)
                                   (in $ ?e)
                                   (where (?e book/stock ?s)))
                                 (db conn) eid)))]
         [stock-ref (make-ref now-stock)]
         [result
          (dosync
            (let ([cur (ref-deref stock-ref)])
              (when (< cur delta)
                (error 'checkout "insufficient stock" title cur delta))
              (alter stock-ref - delta)
              (alter audit-log
                     (lambda (entries)
                       (cons (list title price delta) entries)))
              (ref-deref stock-ref)))])
    ;; Persist the decrement outside the STM (io! guards against accidental
    ;; side effects from inside a transaction).
    (io!
      (transact! conn
        (list `((db/id . ,eid) (book/stock . ,result)))))
    result))

(displayln "stock after checkout Red Mars -1: "
  (checkout-book! "Red Mars" 14 1))
(displayln "audit-log entries: " (length (ref-deref audit-log)))

(def io-in-tx-result
  (guard (exn [else 'io-blocked-inside-dosync])
    (dosync
      (io! (displayln "  (should never print — io! inside dosync)")))
    'completed))

(displayln "io! inside dosync -> " io-in-tx-result)

;; =====================================================================
;; Section 7 — Agent with error handler + await-for timeout
;; =====================================================================

(section 7 "Agent: error handler, failing send, await-for timeout")

(def report-agent (agent '()))

(set-error-mode! report-agent 'continue)
(set-error-handler! report-agent
  (lambda (a err)
    (displayln "  error-handler saw: "
      (if (message-condition? err)
          (condition-message err)
          "error"))))

(send report-agent
  (lambda (acc) (cons 'received-order acc)))

(send report-agent
  (lambda (_) (error 'report "simulated failure")))

(send report-agent
  (lambda (acc) (cons 'recovered acc)))

(await report-agent)

(displayln "agent value after error + recovery: "
  (agent-value report-agent))

;; await-for with a 50ms budget on an agent whose action busy-spins ~300ms.
(def (now-ns)
  (let ([t (current-time)])
    (+ (time-nanosecond t) (* (time-second t) 1000000000))))

(def slow-agent (agent 0))
(send slow-agent
  (lambda (n)
    (let ([deadline (+ (now-ns) 300000000)])  ;; ~300ms of spinning
      (let loop ()
        (when (< (now-ns) deadline) (loop))))
    n))

(def timed-out? (not (await-for 50 slow-agent)))
(displayln "await-for 50ms timed out on slow action? " timed-out?)
(shutdown-agent! slow-agent)

;; =====================================================================
;; Section 8 — Protocol with extend-type for Book / Order / User
;; =====================================================================

(section 8 "Protocol Renderable with extend-type")

(defprotocol Renderable
  (render (self)))

(defstruct book-view (title price))
(defstruct order-view (lines))
(defstruct user-view (name))

(extend-type book-view::t Renderable
  (render (b)
    (str "[Book] " (book-view-title b) " — $" (book-view-price b))))

(extend-type order-view::t Renderable
  (render (o)
    (str "[Order] " (length (order-view-lines o)) " line(s)")))

(extend-type user-view::t Renderable
  (render (u)
    (str "[User] " (user-view-name u))))

(for-each
  (lambda (obj) (displayln (render obj)))
  (list
    (make-book-view "Red Mars" 14)
    (make-order-view '("Red Mars" "Green Mars"))
    (make-user-view "Alice")))

(displayln "Renderable extenders: "
  (length (extenders Renderable)))
(displayln "book-view extends Renderable? "
  (extends? Renderable book-view::t))

;; =====================================================================
;; Section 9 — Multimethod with derive hierarchy
;; =====================================================================

(section 9 "Multimethod dispatch with derive hierarchy")

(def events-h (make-hierarchy))
(derive events-h 'order/new-member-order 'order/any)
(derive events-h 'order/returning-order  'order/any)
(derive events-h 'order/any              'event/any)
(derive events-h 'user/signup            'event/any)

;; Dispatch resolves the most specific ancestor that has a method.
(defmulti handle-event
  (lambda (evt)
    (let ([t (cdr (assq 'type evt))])
      (or (find (lambda (tag) (get-method handle-event tag))
                (cons t (ancestors events-h t)))
          t))))

(defmethod handle-event 'order/new-member-order (e)
  (str "welcome-discount applied to order " (cdr (assq 'id e))))

(defmethod handle-event 'order/any (e)
  (str "logged order " (cdr (assq 'id e))))

(defmethod handle-event 'event/any (e)
  (str "generic audit for " (cdr (assq 'type e))))

;; Register the hierarchy with the multimethod via isa?-style dispatch.
;; In Jerboa's std/multi, defmethod with a symbol key matches exactly;
;; hierarchy-aware dispatch uses the global-hierarchy if you call
;; `isa?` directly. The point here is to show `derive`/`ancestors`.
(displayln "ancestors of order/new-member-order: "
  (ancestors events-h 'order/new-member-order))
(displayln "isa? order/new-member-order -> event/any: "
  (isa? events-h 'order/new-member-order 'event/any))

(displayln (handle-event '((type . order/new-member-order) (id . 1001))))
(displayln (handle-event '((type . order/returning-order)  (id . 1002))))
(displayln (handle-event '((type . user/signup)            (id . 42))))

;; =====================================================================
;; Section 10 — Spec: s-def + s-fdef + s-instrument
;; =====================================================================

(section 10 "Spec: s-def + s-fdef + s-instrument")

(s-def ::isbn   (s-and string? (s-pred (lambda (s) (> (string-length s) 10)))))
(s-def ::qty    (s-and integer? (s-pred positive?)))
(s-def ::order  (s-cat ':isbn '::isbn
                       ':qty  '::qty))

(def (raw-place-order isbn qty)
  (list 'order isbn qty))

(def (place-order isbn qty)
  (unless (s-valid? '::order (list isbn qty))
    (error 'place-order "args do not conform to ::order"
           (s-explain-str '::order (list isbn qty))))
  (raw-place-order isbn qty))

(displayln "valid call returns: "
  (place-order "978-0553560732" 2))

(def bad-result
  (guard (exn [else 'rejected-by-spec])
    (place-order "short" -1)))
(displayln "invalid call rejected by spec? -> " bad-result)

(displayln "explain-str for bad args: "
  (s-explain-str '::order '("short" -1)))

;; =====================================================================
;; Section 11 — Transducer pipeline
;; =====================================================================

(section 11 "Transducer pipeline: filter + map + take")

(def catalog-rows
  (q '((find ?title ?price ?stock)
       (where (?e book/title ?title)
              (?e book/price ?price)
              (?e book/stock ?stock)))
     (db conn)))

(def cheap-titles
  (into '()
        (compose-transducers
          (filtering (lambda (row) (positive? (caddr row))))  ;; in stock
          (mapping   (lambda (row) (list (car row) (cadr row))))
          (taking 2))
        catalog-rows))

(displayln "first 2 in-stock books (title + price):")
(for-each displayln cheap-titles)

(def total-in-stock
  (transduce
    (filtering positive?)
    (rf-sum)
    0
    (map caddr catalog-rows)))

(displayln "total units in stock (via transduce + rf-sum): " total-in-stock)

;; =====================================================================
;; Section 12 — Threading macros
;; =====================================================================

(section 12 "Threading macros: -> / ->> / some->")

;; -> threads the previous expr as the FIRST arg of the next form.
(def formatted-title
  (-> "red mars"
      string-upcase
      (string-append " (2026 ed.)")))
(displayln "-> pipeline built: " formatted-title)

(def collected
  (->> catalog-rows
       (filter (lambda (row) (>= (cadr row) 15)))
       (map car)
       (list-sort string<?)))
(displayln "->> filter + map + sort: " collected)

;; some-> threads first-arg and short-circuits on #f.
(def (safe-title eid)
  (some-> (pull (db conn) '(book/title) eid)
          cdr   ;; ((book/title . "...")) -> ((book/title . "..."))
          car   ;; -> (book/title . "...")
          cdr   ;; -> "..."
          string-upcase))

(displayln "some-> over existing entity: " (safe-title dhalgren-eid))
(displayln "some-> short-circuits on #f: " (some-> #f string-upcase))

;; ---------------------------------------------------------------------
;; Done!
;; ---------------------------------------------------------------------
(newline)
(displayln "============================================================")
(displayln "bookstore.ss — 12 sections complete")
(displayln "============================================================")
