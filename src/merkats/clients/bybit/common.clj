(ns merkats.clients.bybit.common
  (:require [buddy.core.codecs :as crypto.codecs]
            [buddy.core.mac :as crypto.mac]
            [jsonista.core :as json]))

(defn sign
  [to-sign key-secret]
  (-> (crypto.mac/hash to-sign {:key key-secret :alg :hmac+sha256})
      (crypto.codecs/bytes->hex)))

(def json-mapper (json/object-mapper {:encode-key-fn true
                                      :decode-key-fn true
                                      :bigdecimals   true}))
