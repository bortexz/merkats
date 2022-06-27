(ns merkats.domain.order.book
  "Order book where each price level is a map of {::order/id order}
   Useful to keep track of own orders inside an orderbook, not to be confused with ::orderbook, which is
   a more 'public-facing' orderbook representation without specific order information, with sizes aggregated.
   
   TODO: Use ordered-map to keep order insertion. Allow for partially consuming levels?"
  (:refer-clojure :exclude [when-let])
  (:require [merkats.domain.order :as order]
            [merkats.domain.transaction :as tx]
            [merkats.extensions.spec :as es]
            [merkats.extensions.sorted :as sort]
            [clojure.alpha.spec :as s]
            [better-cond.core :refer [when-let]]
            [merkats.extensions.core :refer [ascending-comparator descending-comparator]]))

; --------------------------------------------------------------------------------------------------
; Spec

(es/def ::bids (s/map-of ::order/book-price ::order/index))
(es/def ::asks (s/map-of ::order/book-price ::order/index))
(es/def ::book (s/schema [::bids ::asks]))

; Spec
; --------------------------------------------------------------------------------------------------

(def ^:private maker-book-side
  "Side of the orderbook where maker orders are placed."
  {::tx/buy  ::bids
   ::tx/sell ::asks})

(def ^:private taker-book-side
  "Side of the orderbook that is consumed by taker side."
  {::tx/buy  ::asks
   ::tx/sell ::bids})

(defn- order->maker-book-side
  "Returns nil if order doesn't have a parameters.price, otherwise the result of order-side->book-side"
  [{::order/keys [parameters] :as o}]
  (when-let [_price (::tx/price parameters)
             side  (::tx/side parameters)]
    (maker-book-side side)))

(defn- side-ingest-orders
  [side orders]
  (-> (group-by (comp ::tx/price ::order/parameters) orders)
      (update-vals (fn [xs] (into {} (map (juxt ::order/id identity)) xs)))
      (->> (merge-with merge side))))

(defn- bids
  ([] (sort/sorted-map-by descending-comparator))
  ([orders]
   (->> orders
        (filter (comp #{::bids} order->maker-book-side))
        (side-ingest-orders (bids)))))

(defn- asks
  ([] (sort/sorted-map-by ascending-comparator))
  ([orders]
   (->> orders
        (filter (comp #{::asks} order->maker-book-side))
        (side-ingest-orders (asks)))))

(defn book
  ([]
   {::bids (bids)
    ::asks (asks)})
  ([orders]
   {::bids (bids orders)
    ::asks (asks orders)}))

(defn- touched-levels
  [sc price pass-through?]
  (let [touch-cmp (if pass-through? < <=)
        levels (subseq sc touch-cmp price)]
    levels))

(defn add-order
  "Places the given order on the corresponding side of the limit-book, returns updated limit-book"
  [book {::order/keys [parameters id] :as o}]
  (let [{::tx/keys [side price]} parameters]
    (update-in book [(maker-book-side side) price] assoc id o)))

(defn remove-order
  "Removes the given order from the limit-book, returning the updated limit-book"
  [book {::order/keys [parameters id] :as o}]
  (let [{::tx/keys [price side]} parameters
        book-side (maker-book-side side)]
    (-> book
        (update-in [book-side price] dissoc id)
        (update book-side (fn [side]
                            (cond-> side
                              (empty? (get side price)) (dissoc price)))))))

(defn touch
  "'Touches' the orderbook with a given transaction, returning {::book ... ?::order/orders ...}
   where ::book is an updated version of the book passed without touched orders, and order/orders is
   a collection of orders that would no longer exist in the given book after the given tx.
   
   The price level at exact price of the given tx will be touched when pass-through? is true, 
   otherwise the exact level will not be considered touched. One use case for pass-through? is simulations,
   when you only want to consider your orders filled whenever an historical trade 'passes through'
   the level's price, and not when a trade occurs at the same price.
   (i.e when using 'pass-through? you simulate that your orders are at the 'end' of the queue of 
   the price level)

   The size of the tx is not used, and even a tx smaller in size than size of orders, will 'touch' and 
   return all orders at touched levels."
  ([book tx] (touch book tx true))
  ([book {::tx/keys [price side]} pass-through?]
   (let [side-key (taker-book-side side)
         touched-levels (touched-levels (book side-key) price pass-through?)
         orders-touched (->> touched-levels
                             (mapcat (comp vals val)))]
     {::order/orders orders-touched
      ::book (update book
                     side-key
                     (fn [sc]
                       (apply dissoc sc (map first touched-levels))))})))