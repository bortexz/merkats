(ns merkats.domain.trigger
  (:require [merkats.extensions.spec :as es]
            [merkats.domain.transaction :as tx]
            [clojure.alpha.spec :as s]))

; --------------------------------------------------------------------------------------------------
; Attributes

(es/def ::trigger (s/schema [::tx/price ::tx/side]))

; Attributes
; --------------------------------------------------------------------------------------------------

; --------------------------------------------------------------------------------------------------
; Fns

(defn stop-loss
  [orig-tx-side price]
  {::tx/side (tx/inverse-side orig-tx-side)
   ::tx/price price})

(defn take-profit
  [orig-tx-side price]
  {::tx/side orig-tx-side
   ::tx/price price})

(defn hit?
  "Returns true when the given trigger would be hit at given `hit-price`"
  [{::tx/keys [price side]} hit-price]
  (let [hitf (case side ::tx/buy >= ::tx/sell <=)]
    (hitf hit-price price)))

; Fns
; --------------------------------------------------------------------------------------------------
