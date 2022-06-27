(ns merkats.domain.ledgers.single-derivative
  "Namespace with accounting suitable for a single derivative market with a balance."
  (:require [merkats.extensions.spec :as es]
            [clojure.alpha.spec :as s]
            [merkats.domain.position :as position]
            [merkats.domain.market :as market]
            [merkats.domain.fee :as fee]
            [merkats.domain.balance :as balance]))

(es/def ::ledger (s/schema [::market/market
                            ::position/position
                            ::balance/balance]))

(defn ingest-trade
  [{mkt ::market/market b ::balance/balance pos ::position/position :as l} {fee ::fee/fee :as t}]
  (let [new-pos (position/ingest-trade pos t mkt)
        new-bal (-> b
                    (balance/apply-delta new-pos)
                    (cond->
                     (some? fee) (balance/apply-delta fee)))]
    (merge l {::position/position (position/clean-update new-pos)
              ::balance/balance new-bal})))

(defn ingest-current-price
  "Ingest current market price, updating the performance of ::position/position"
  [{mkt ::market/market :as l} price]
  (cond-> l
    (position/has-entry? (::position/position l))
    (update ::position/position position/add-performance price mkt)))

(defn total-balance
  [{pos ::position/position b ::balance/balance}]
  (+ (or (-> pos ::position/performance ::position/equity) 0M)
     (::balance/available b)))