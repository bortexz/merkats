(ns merkats.domain.asset
  (:require [merkats.extensions.spec :as es]
            [clojure.alpha.spec :as s]))

; --------------------------------------------------------------------------------------------------
; Attributes

(es/def ::symbol string?)
(es/def ::scale nat-int?)

(es/def ::asset (s/schema [::symbol ::scale]))

; Attributes
; --------------------------------------------------------------------------------------------------
