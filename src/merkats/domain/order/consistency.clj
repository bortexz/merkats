(ns merkats.domain.order.consistency
  "Usually order updates are received in realtime, where disconnects or other network
   failures can occur. This namespace deals with computing the execution from trades and updates received, 
   potentially out of order/date.
   
   The main entity of this namespace is en extended version of order that contains ::ingested-trades
   (set of trade/ids) and ::remote (most leading execution received as part of updates), whose execution
   is computed from updates received.
   
   Only when all trades for a given size are ingested, the order will move (if needed) to remove status.
   E.g If an order update has an execution of ::tx/size 50M and ::order/status ::order/cancelled, 
   this execution will be stored on ::remote. Only when trades have been ingested accounting for the
   50M tx/size on the ::remote, then the cancelled status will be propagated to order/execution."
  (:require [merkats.extensions.spec :as es]
            [clojure.alpha.spec :as s]
            [merkats.domain.order :as order]
            [merkats.domain.trade :as trade]
            [merkats.domain.transaction :as tx]))

(es/def ::ingested-trades (s/coll-of ::trade/id :kind set?))
(es/def ::remote ::order/execution)

(es/def ::order (s/union ::order/order [::ingested-trades ::remote]))

(es/def ::update (s/union ::order [::trade/trade]))

(defn missing-trades?
  "Returns logical true iff the remote.tx/size execution has more tx/size than order.execution,
   meaning that remote has accounted for trades that have not been ingested."
  [{::keys [remote] ::order/keys [execution]}]
  (when (and remote execution)
    (> (::tx/size remote) (::tx/size execution))))

(defn remote-outdated?
  "Returns true iff o.remote execution is behind o execution"
  [o]
  (order/forward-execution? {::order/execution (::remote o)} o))

(defn- propagate-remote-status?
  [o]
  (and (order/valid-status-transition? (-> o ::order/execution ::order/status)
                                       (-> o ::remote ::order/status))
       (= (::order/size o) (-> o ::remote ::order/size))))

(defn- maybe-propagate-remote-status
  [{::keys [remote] :as o}]
  (if (propagate-remote-status? o)
    (order/transition o (::order/status remote))
    o))

(defn- apply-missing-trade
  [o t mkt]
  (-> o
      (order/ingest-trade t mkt)
      (assoc ::trade/trade t)
      (update ::ingested-trades (fnil conj #{}) (::trade/id t))
      (maybe-propagate-remote-status)))

(defn- apply-forward-execution
  [o exec]
  (maybe-propagate-remote-status (assoc o ::remote exec)))

(defn ingest-update
  "Returns an ::update that will include ::trade/trade iff `u` contains a trade that was not
   yet ingested into the order.
   
   The resulting update will have an execution that is at same step or beyond current execution `o`."
  [{::keys [ingested-trades] :or {ingested-trades #{}} :as o} 
   {::order/keys [execution cancellation] ::trade/keys [trade] :as u}
   mkt]
  (let [missing-trade? (and trade (not (ingested-trades (::trade/id trade))))
        forward-execution? (and execution (order/forward-execution? o u))
        forward-cancellation? (and cancellation (order/forward-cancellation? o u))]
    (cond-> o
      missing-trade? (apply-missing-trade trade mkt)
      (not missing-trade?) (dissoc ::trade/trade)
      forward-execution? (apply-forward-execution execution)
      forward-cancellation? (assoc ::order/cancellation cancellation))))
