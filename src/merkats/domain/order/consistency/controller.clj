(ns merkats.domain.order.consistency.controller
  (:refer-clojure :exclude [if-let])
  (:require [better-cond.core :refer [if-let]]
            [merkats.domain.order.consistency :as order-cy]
            [merkats.domain.order :as order]
            [merkats.domain.market :as market]
            [merkats.anomalies :as anomalies]
            [merkats.extensions.spec :as es]
            [clojure.alpha.spec :as s]))

(es/def ::controller (s/schema [::market/market 
                                ::order/index]))

(es/def ::update (s/union ::controller
                          [::order/updates]))

(def ^:private conj-vec (fnil conj []))

(defn ingest-open-orders
  "Returns ::update. Orders that already exist on the controller fail with anomalies/invalid-params,
   while correctly ingested open orders have their execution initialized with ::order/in-flight 
   status."
  [ctrl os]
  (reduce
   (fn [{idx ::order/index :as ctrl} {::order/keys [id] :as o}]
     (if (get idx id)
       (update ctrl ::order/updates conj-vec (assoc o ::anomalies/category anomalies/invalid-params))
       (let [oex (order/init-execution o)]
         (-> ctrl
             (update ::order/index assoc id oex)
             (update ::order/updates conj-vec oex)))))
   (dissoc ctrl ::order/updates)
   os))

(defn ingest-cancel-orders
  "Retturns ::update.
   If cancellation cannot occur on an order, order/update for given order will contain anomalies/invalid-params.
   If cancellation can occur, order/update contains currently stored order with ::order/cancellation ::order/in-flight"
  [ctrl os]
  (reduce
   (fn [{idx ::order/index :as ctrl} {::order/keys [id]}]
     (if-let [o (get idx id)
              :let [cancellation (::order/cancellation o)
                    exec-status (-> o ::order/execution ::order/status)]
              _? (or (not cancellation)
                     (#{::order/rejected} cancellation))
              co (assoc o ::order/cancellation ::order/in-flight)]
       (-> ctrl
           (update ::order/index assoc id co)
           (update ::order/updates conj-vec co))
       ctrl))
   (dissoc ctrl ::order/updates)
   os))

(defn ingest-remote-order-updates
  [ctrl ous]
  (reduce 
   (fn [{idx ::order/index mkt ::market/market :as ctrl} {::order/keys [id] :as ou}]
     (if-let [o (get idx id)
              :let [-ou (order-cy/ingest-update o ou mkt)
                    finished? (order/finished? -ou)]]
       (-> ctrl
           (update ::order/updates conj-vec -ou)
           (update ::order/index (fn [idx] 
                                   (if finished?
                                     (dissoc idx id)
                                     (assoc idx id -ou)))))
       ctrl))
   (dissoc ctrl ::order/updates)
   ous))


(defn out-of-sync-orders
  "Returns sequence of orders whose ::order-cy/remote is out of sync with current execution."
  [ctrl]
  (sequence
   (comp (map val)
         (filter (fn [o] (or (order-cy/missing-trades? o)
                             (order-cy/remote-outdated? o)))))
   (::order/index ctrl)))
