(ns merkats.domain.market
  (:require [merkats.domain.asset :as asset]
            [merkats.extensions.spec :as es]
            [clojure.alpha.spec :as s]))

(es/def ::symbol
  "Symbol identifying this market."
  string?)

(es/def ::base-asset ::asset/symbol)
(es/def ::quote-asset ::asset/symbol)

(es/def ::contract?
  "Wether this market exchanges contracts instead of the underlying assets."
  boolean?)

(es/def ::contract-asset 
  "Symbol of the asset representing the contracts of this market, only present when contract?
   is true."
  ::asset/symbol)

(es/def ::cash-asset
  "Whenever a market is contract?, the cash asset is the asset used as cash to exchange by a contract."
  ::asset/symbol)

(es/def ::direction
  "::linear and ::inverse supported by default. 
   Support for ::quanto in the future.
   Open set of keywords to allow customization."
  keyword?)

(es/def ::price-tick decimal?)
(es/def ::lot-size decimal?)

(es/def ::market (s/schema [::symbol
                            ::base-asset
                            ::quote-asset
                            ::contract?
                            ::cash-asset

                            ::direction

                            ::price-tick
                            ::lot-size]))
