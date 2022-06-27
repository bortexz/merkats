(ns merkats.domain.market.simulator
  "Simulator that works by feeding it historical trades.
   
   TODO: Consider time-in-force for taker orders.
   TODO: Review fill-taker-orders.
   TODO: open order that already exists should not return rejected, only anomaly.
   TODO: Allow an option to use orderbook for taker orders, for realtime simulator when the orderbook
   is a real one. Can do by adding ingest-orderbook, that will consume remaining taker orders."
  (:refer-clojure :exclude [cond])
  (:require [clojure.alpha.spec :as s]
            [merkats.extensions.core :refer [queue queue?]]
            [better-cond.core :refer [cond]]
            [merkats.extensions.spec :as es]
            [merkats.anomalies :as anomalies]
            [merkats.domain.balance :as balance]
            [merkats.domain.order :as order]
            [merkats.domain.order.book :as order-lb]
            [merkats.domain.market :as market]
            [merkats.domain.time :as time]
            [merkats.domain.fee :as fee]
            [merkats.domain.trade :as trade]
            [merkats.domain.transaction :as tx]))

(es/def ::latest-trade ::trade/trade)
(es/def ::taker-buys (s/coll-of ::order/order :kind queue?))
(es/def ::taker-sells (s/coll-of ::order/order :kind queue?))
(es/def ::maker-fee ::fee/rate)
(es/def ::taker-fee ::fee/rate)

(es/def ::simulator (s/schema [::market/market
                               ::order-lb/book
                               ::order/index
                               ::time/stamp

                               ::latest-trade
                               ::taker-buys
                               ::taker-sells
                               ::maker-fee
                               ::taker-fee]))

(es/def ::update (s/union ::simulator [::order/updates]))

(defn initialize
  "Fee structure is a map of ::maker-fee and ::taker-fee"
  [{::keys [maker-fee taker-fee] ::market/keys [market] :as opts}]
  (merge opts
         {::order-lb/book (order-lb/book)
          ::order/index {}
          ::taker-buys (queue)
          ::taker-sells (queue)}))

(defn clean-update
  "Removes the current order updates of the simulator"
  [sim]
  (dissoc sim ::order/updates))

(defn- valid-maker-price?
  [o lt]
  (let [{oprice ::tx/price oside ::tx/side} (::order/parameters o)
        {ltprice ::tx/price ltside ::tx/side} lt]
    (condp = [oside ltside]
      [::tx/buy ::tx/buy] (< oprice ltprice)
      [::tx/buy ::tx/sell] (<= oprice ltprice)
      [::tx/sell ::tx/buy] (>= oprice ltprice)
      [::tx/sell ::tx/sell] (> oprice ltprice))))

(defn- add-order-update
  ([{ts ::time/stamp :as sim} o]
   (update sim ::order/updates (fnil conj []) (assoc o ::time/stamp ts)))
  ([{ts ::time/stamp :as sim} o t]
   (update sim ::order/updates (fnil conj []) (assoc o ::time/stamp ts ::trade/trade t))))

(defn- add-order
  [sim o]
  (update sim ::order/index assoc (::order/id o) o))

(defn- remove-order
  [sim o]
  (update sim ::order/index dissoc (::order/id o)))

(defn- invalid-open-order
  [sim o anom]
  (add-order-update sim (merge o {::anomalies/category anom})))

(defn- reject-order
  ([sim o]
   (reject-order sim o anomalies/fault))
  ([sim o anom]
   (add-order-update sim (merge o {::anomalies/category anom
                                   ::order/execution {::order/status ::order/rejected}}))))

(defn- add-maker-order
  [sim o]
  (let [newo (order/transition (order/init-execution o) ::order/created)]
    (-> sim
        (add-order newo)
        (update ::order-lb/book order-lb/add-order newo)
        (add-order-update newo))))

(def ^:private taker-side->queue
  {::tx/buy ::taker-buys
   ::tx/sell ::taker-sells})

(defn- add-taker-order
  [state {::tx/keys [side id] :as o}]
  (let [newo (order/transition (order/init-execution o) ::order/created)]
    (-> state
        (update (taker-side->queue side) conj id)
        (add-order newo)
        (add-order-update newo))))

(defn- add-fee
  [t rate]
  (assoc t ::fee/fee {::fee/rate rate
                      ::balance/change (fee/calculate-balance-change (::tx/value t) rate)}))

(defn- fill-maker-orders
  [{::keys [latest-trade maker-fee] ::market/keys [market] :as sim}]
  (let [{os ::order/orders book ::order-lb/book}
        (order-lb/touch (::order-lb/book sim) latest-trade true)]
    (reduce
     (fn [{ts ::time/stamp :as sim} o]
       (let [otx (tx/add-value (select-keys (::order/parameters o) [::tx/price ::tx/size ::tx/side])
                               market)
             exec-trade (-> (merge
                             otx
                             {::tx/actor ::tx/maker
                              ::market/symbol (::market/symbol market)
                              ::time/stamp ts
                              ::trade/id (str (random-uuid))})
                            (add-fee maker-fee))

             newo (order/ingest-trade o exec-trade market)]
         (-> sim
             (remove-order newo)
             (add-order-update newo exec-trade))))
     (assoc sim ::order-lb/book book)
     os)))

(defn- fill-taker-orders
  [{::market/keys [market] ::keys [latest-trade taker-fee] :as sim}]
  (let [qk (taker-side->queue (::tx/side latest-trade))]
    (loop [sim sim
           remt latest-trade]
      (let [q  (get sim qk)
            orders (::order/index sim)]
        (cond
          (not remt) sim
          (empty? q) sim
          (let [oid (peek q)
                {::order/keys [parameters execution] :as o} (get orders (::order/id oid))
                ofilled (::tx/size execution)
                orem    (- (::tx/size parameters) ofilled)
                [otrade remt]  (if (< (::tx/size remt) orem)
                                 [remt nil]
                                 (tx/split remt orem))
                otrade (add-fee otrade taker-fee)
                newo (order/ingest-trade o otrade market)
                finished? (order/finished? newo)
                new-state (cond-> (add-order-update newo otrade)
                            finished?       (-> (remove-order newo)
                                                (update qk pop))
                            (not finished?) (add-order newo))]
            (recur new-state remt)))))))

(defn open-orders
  "Open the given orders in the simulator.
   
   Creates order updates, with order/created or order/rejected, depending if parameters are valid
   for current market. order/updates might contain anomalies for "
  [sim os]
  (reduce
   (fn [{::keys [latest-trade] orders ::order/index :as sim} {::order/keys [id parameters] :as o}]
     (let [{::tx/keys [actor]} parameters
           existing (get orders id)]
       (if existing
         (invalid-open-order sim o anomalies/invalid-params)
         (case actor
           ::tx/maker
           (if (valid-maker-price? o latest-trade)
             (add-maker-order sim o)
             (reject-order sim o anomalies/invalid-params))

           ::tx/taker
           (add-taker-order sim o)

           nil
           (if (valid-maker-price? o latest-trade)
             (add-maker-order sim o)
             (add-taker-order sim o))))))
   (clean-update sim)
   os))

(defn cancel-orders
  [sim os]
  (reduce
   (fn [{orders ::order/index :as sim} {::order/keys [id] :as o}]
     (if-let [curro (get orders id)]
       (cond-> sim
         (not (#{::tx/taker} (-> curro ::order/parameters ::tx/actor)))
         (-> (update ::order-lb/book order-lb/remove-order curro)
             (remove-order curro)
             (add-order-update (-> curro
                                   (order/transition ::order/cancelled)
                                   (assoc ::order/cancellation ::order/created)))))
       (add-order-update sim (merge o {::anomalies/category ::anomalies/not-found
                                       ::order/cancellation ::order/rejected}))))
   (clean-update sim)
   os))

(defn ingest-trades
  [sim ts]
  (reduce
   (fn [sim t]
     (-> sim
         (assoc ::latest-trade t)
         (assoc ::time/stamp (::time/stamp t))
         (fill-maker-orders)
         (fill-taker-orders)))
   (clean-update sim)
   ts))
