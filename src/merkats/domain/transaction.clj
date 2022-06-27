(ns merkats.domain.transaction
  (:require [merkats.domain.market :as market]
            [merkats.extensions.spec :as es]
            [clojure.alpha.spec :as s]))

(es/def ::price decimal?)
(es/def ::side #{::buy ::sell})
(es/def ::actor #{::taker ::maker})
(es/def ::size decimal?)
(es/def ::value decimal?)

(defmulti calculate-value
  "Calculates the value of a given transaction."
  (fn [tx mkt] (::market/direction mkt)))

(defmethod calculate-value ::market/linear calculate-linear-value
  [{::keys [size price]} _]
  (* size price))

(defmethod calculate-value ::market/inverse calculate-inverse-value
  [{::keys [size price]} _]
  (/ size price))

(defn add-value [tx mkt] (assoc tx ::value (calculate-value tx mkt)))

(defmulti calculate-size (fn [tx mkt] (::market/direction mkt)))

(defmethod calculate-size ::market/linear
  [{::keys [value price]} _]
  (/ value price))

(defmethod calculate-size ::market/inverse
  [{::keys [value price]} _]
  (* value price))

(defmulti calculate-avg-price (fn [txs mkt] (::market/direction mkt)))

(defmethod calculate-avg-price ::market/linear calculate-linear-avg-price
  [txs _]
  (let [total-size  (reduce + 0M (map ::size txs))
        total-value (reduce + 0M (map ::value txs))]
    (/ total-value total-size)))

(defmethod calculate-avg-price ::market/inverse calculate-inverse-avg-price
  [txs _]
  (let [total-size  (reduce + 0M (map ::size txs))
        total-value (reduce + 0M (map ::value txs))]
    (/ total-size total-value)))

(defn split
  "Splits the given tx at given `at-size`, returning [tx1 tx2] where tx1 has `at-size` size, and tx2 has 
   (tx.size - at-size) size.
   New size and value are merged into the original tx."
  [{::keys [size value] :as tx} at-size]
  (let [uval (/ value size)
        tx1-size at-size
        tx1-val (* tx1-size uval)
        tx2-size (- size at-size)
        tx2-val (* tx2-size uval)]
    [(merge tx {::size tx1-size ::value tx1-val})
     (merge tx {::size tx2-size ::value tx2-val})]))

(defn inverse-side
  [side]
  (case side ::buy ::sell ::sell ::buy))

(defn ->signed-size
  "When side is ::buy, returns ::size.
   When side is ::sell, returns negated ::size"
  [tx]
  (if (#{::sell} (::side tx))
    (- (::size tx))
    (::size tx)))

(defn <-signed-size
  "Returns a tx that has (abs size) and:
   - ::buy ::side when size is positive
   - ::sell ::side when size is negative
   - no side when size is 0M"
  [size]
  (cond
    (pos? size) {::side ::buy ::size size}
    (neg? size) {::side ::sell ::size (- size)}
    (zero? size) {::size size}))

(defn towards
  "Returns tx needed to go `from` towards `to`, or nil if they are the same"
  [from to]
  (let [signed-diff (- (->signed-size to) (->signed-size from))]
    (when (not (zero? signed-diff))
      (<-signed-size signed-diff))))

(defn join
  "Joins sizes and sides of txs, returning a single transaction of the aggregated txs"
  [txs]
  (<-signed-size (reduce + 0M (map ->signed-size txs))))
