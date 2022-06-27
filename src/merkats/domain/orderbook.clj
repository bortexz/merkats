(ns merkats.domain.orderbook
  (:require [clojure.alpha.spec :as s]
            [merkats.extensions.spec :as es]
            [merkats.extensions.sorted :as sort]
            [merkats.extensions.core :refer [ascending-comparator descending-comparator queue]]
            [merkats.domain.transaction :as tx]
            [merkats.domain.market :as market]
            [clojure.set :as set]))
; --------------------------------------------------------------------------------------------------
; Attributes

(s/def ::bids (s/map-of ::tx/price ::tx/size :kind sorted?))
(s/def ::asks (s/map-of ::tx/price ::tx/size :kind sorted?))

(s/def ::depth pos-int?)

(s/def ::book (s/schema [::market/symbol
                         
                         ::bids 
                         ::asks 
                         ::depth]))

(s/def ::top-bid (s/tuple ::tx/price ::tx/size))
(s/def ::top-ask (s/tuple ::tx/price ::tx/size))

(s/def ::quote (s/schema [::market/symbol
                          
                          ::top-bid
                          ::top-ask]))

; Orderbook update

(es/def ::rows (s/coll-of (s/tuple ::tx/side ::tx/price (s/nilable ::tx/size))))

(es/def ::update
  "An orderbook update. Contains a collection of new rows to be applied to an orderbook.
   When size is nil, then that level should be removed."
  (s/schema [::market/symbol
             ::rows]))

; Attributes
; --------------------------------------------------------------------------------------------------

; --------------------------------------------------------------------------------------------------
; Fns

(defn book->quote
  "Returns quote from orderbook"
  [orderbook]
  (let [{::keys [bids asks]} orderbook]
    (merge orderbook
           {::top-bid (sort/first bids)
            ::top-ask (sort/first asks)})))

(defn quote->spread
  "Returns the current spread given `quote`"
  [quote]
  (let [{::keys [top-bid top-ask]} quote
        [bid-price _] top-bid
        [ask-price _] top-ask]
    (- ask-price bid-price)))

(defn book->spread
  "Returns current spread given `orderbook`"
  [orderbook]
  (quote->spread (book->quote orderbook)))

(defn update-book
  "Returns an updated `orderbook` with `book-update` applied."
  [{::keys [bids asks] :as ob} rows]
  ;;  micro-optimization to traverse rows once
  (let [new-bids (volatile! (transient bids))
        new-asks (volatile! (transient asks))]
    (run! (fn [[side price size]]
            (let [s (case side ::tx/buy new-bids ::tx/sell new-asks)]
              (vswap! s (fn [sc]
                          (if (some? size)
                            (assoc! sc price size)
                            (dissoc! sc price))))))
          rows)
    (-> ob
        (assoc ::bids (persistent! @new-bids))
        (assoc ::asks (persistent! @new-asks)))))

(defn book
  "Creates a new book, empty or from given rows"
  ([] {::bids (sort/sorted-map-by descending-comparator)
       ::asks (sort/sorted-map-by ascending-comparator)})
  ([rows]
   (update-book (book) rows)))

(comment
  (def bids-rows (mapv (fn [i] [::tx/buy i i]) (range 50)))
  (def asks-rows (mapv (fn [i] [::tx/sell i i]) (range 50 100)))
  (def sample-book (book (concat bids-rows asks-rows)))
  sample-book

  (book->quote sample-book)

  (quote->spread (book->quote sample-book))
  (book->spread sample-book)

  (def update-rows (mapv (fn [i]
                           [(if (<= i 25) ::tx/buy ::tx/sell)
                            (rand-int i)
                            (let [v (rand-int i) add? (<= v 25)]
                              (when add? v))])
                         (range 50)))

  update-rows
  (update-book sample-book update-rows)
  )

; Fns
; --------------------------------------------------------------------------------------------------