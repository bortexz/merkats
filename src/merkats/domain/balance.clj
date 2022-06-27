(ns merkats.domain.balance
  (:require [merkats.extensions.spec :as es]
            [merkats.domain.asset :as asset]
            [clojure.alpha.spec :as s]))

(es/def ::available decimal?)
(es/def ::change decimal?)

(es/def ::balance (s/schema [::asset/symbol
                             ::available]))

(es/def ::balances (s/coll-of ::balance))

(es/def ::delta (s/schema [::asset/symbol
                           ::change]))

(defn accumulate-deltas
  "Returns a single update that is the accumulation of given updates
   
   All updates must be of the same asset/symbol."
  [us]
  (reduce (fn [u {::keys [change]}]
            (update u ::change + change))
          (first us)
          (rest us)))

(defn apply-delta
  "Applies the given update `u` to available balance in `b`.
   Both `b` and `u` must have same symbol"
  [b u]
  (update b ::available (fnil + 0M) (::change u)))
