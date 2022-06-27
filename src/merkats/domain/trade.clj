(ns merkats.domain.trade
  (:require [merkats.extensions.spec :as es]
            [merkats.domain.market :as market]
            [merkats.domain.fee :as fee]
            [merkats.domain.transaction :as tx]
            [merkats.domain.time :as time]
            [clojure.alpha.spec :as s]))

(es/def ::id
  "Trade unique identifier."
  string?)

(s/def ::trade (s/schema [::id

                          ::market/symbol

                          ::tx/size
                          ::tx/price
                          ::tx/side
                          ::tx/actor
                          ::tx/value

                          ::fee/fee
                          
                          ::time/stamp]))

(s/def ::trades (s/coll-of ::trade))
