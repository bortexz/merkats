(ns merkats.domain.time
  (:require [merkats.extensions.spec :as es]
            [tick.core :as t]))

(es/def ::stamp t/instant?)
(es/def ::frame t/duration?)
