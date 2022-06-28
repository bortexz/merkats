(ns merkats.domain.fee
  (:require [merkats.domain.balance :as balance]
            [merkats.domain.asset :as asset]
            [clojure.alpha.spec :as s]
            [merkats.extensions.spec :as es]))

; --------------------------------------------------------------------------------------------------
; Attributes

(es/def ::rate
  "Percentage of fee to be applied. Negative is paid, positive is rebated."
  decimal?)

(es/def ::fee
  "Fee paid/received. Notice that the balance/change will be negative for paid fees, and positive
   for rebated fees."
  (s/schema [::asset/symbol
             ::balance/change
             ::rate]))

; Attributes
; --------------------------------------------------------------------------------------------------

; --------------------------------------------------------------------------------------------------
; Fns

(defn calculate-balance-change
  "Applies fee `rate` to given `gross` amount"
  [gross rate]
  (* gross rate))

(defn calculate-gross
  "Given a fee, returns the gross amount that would correspond to given fee."
  [{::keys [rate] ::balance/keys [change]}]
  (/ change rate))

(defn calculate-rate
  [{::balance/keys [change]} gross]
  (/ change gross))

(defn accumulate-fees
  "Accumulates the given `fees` into one single fee, adding up all balance/changes and properly
   calculating the rate of the given fees combined.
   asset/symbol must be the same in all fees, or not present at all."
  [fees]
  (let [new-bch  (reduce + 0M (map ::balance/change fees))
        gross    (reduce + 0M (map calculate-gross fees))
        new-rate (calculate-rate {::balance/change new-bch} gross)
        asset    (::asset/symbol (first fees))]
    (merge {::rate new-rate
            ::balance/change new-bch}
           (when asset {::asset/symbol asset}))))

(comment
  (with-precision 25
    (accumulate-fees [{::rate 0.5M ::balance/change 50M}
                      {::rate 0.05M ::balance/change 5M}
                      {::rate 0.1M ::balance/change 10M}
                      {::rate 0.25M ::balance/change 30M}]))

  ,)

; Fns
; --------------------------------------------------------------------------------------------------
