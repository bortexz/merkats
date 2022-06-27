(ns merkats.domain.order
  (:require [clojure.alpha.spec :as s]
            [merkats.extensions.spec :as es]
            [merkats.domain.transaction :as tx]
            [merkats.domain.market :as market]
            [merkats.domain.trade :as trade]
            [merkats.domain.fee :as fee]))

;; Attribute specs
;; ===============

(es/def ::id
  "Unique id of order. This is the user-defined order/id, not the one that comes from exchanges."
  string?)

(es/def ::time-in-force
  "- fill-or-kill: Either fill order completely at given price or better, or cancel altogether.
   - immediate-or-cancel: Fill as much as possible at given price, cancel after that."
  #{::fill-or-kill ::immediate-or-cancel ::good-til-cancel})

(es/def ::parameters
  "Order parameters
   
   - If tx/actor is tx/taker, price will only be used when specifying immediate-or-cancel or 
   fill-or-kill as time-in-force.
   
   - If tx/actor is tx/maker,  price is mandatory and is used to post the order in the orderbook.
   Whenever it's supported on the platform, tx/maker orders should prevent execution on the other side 
   (become tx/taker orders).

   - If tx/actor is not specified, it is considered like a tx/maker order with mandatory limit book 
   price, but in this case the order is executed at market if the price is on the other side of tx/side
   specified."
  (s/schema [::time-in-force

             ::tx/size
             ::tx/side
             ::tx/actor
             ::tx/price]))

(es/def ::status
  "Execution status:
   
   - in-flight: order has been sent but hasn't been yet acknowledged.
   - created: order is in the matching engine, either placed on the orderbook or pending to be
     filled as taker order (A taker order might skip this status and go directly towards partially-filled
     or filled)
   - partially-filled: a partial size of this order has been filled.
   - filled: order has been completely filled.
   - cancelled: the order has been cancelled. It can potentially be cancelled after being partially filled.
   - rejected: order has been rejected when trying to create it."
  #{::in-flight
    ::created
    ::partially-filled
    ::filled
    ::cancelled
    ::rejected})

(es/def ::execution
  "Current execution of this order, "
  (s/schema [::status
             
             ::tx/price
             ::tx/side
             ::tx/size
             ::tx/value

             ::fee/fee]))

(es/def ::cancellation
  "A cancellation aims to cancel the order, and can be in these statuses."
  #{::in-flight
    ::created
    ::rejected})

(es/def ::order
  "Order entity."
  (s/schema [::market/symbol
             ::id
             ::parameters
             ::execution
             ::cancellation]))

(es/def ::update
  "Order update, it can also contain a trade/trade for this order"
  (s/union ::order
           [::trade/trade]))

(es/def ::orders (s/coll-of ::order))
(es/def ::updates (s/coll-of ::update))

(es/def ::index (s/map-of ::id ::order))

;; FNs
;; ===

(def status-transitions
  {::in-flight        #{::created
                        ::partially-filled
                        ::filled
                        ::rejected}
   ::created          #{::partially-filled
                        ::filled
                        ::cancelled}
   ::partially-filled #{::partially-filled
                        ::filled
                        ::cancelled}
   ::filled           #{}
   ::cancelled        #{}
   ::rejected         #{}})

(defn valid-status-transition?
  [old-s new-s]
  ((status-transitions old-s) new-s))

(defn valid-execution-size?
  "Returns logical true if
   - execution tx/size is zero and status #{::in-flight ::rejected ::created ::cancelled}
   - execution.tx/size = parameters.tx/size and status #{::filled}
   - (> 0M execution.tx/size parameters.tx/size) and status #{::partially-filled ::cancelled}

   else nil"
  [{::keys [parameters execution]}]
  (let [psize (::tx/size parameters)
        exsize (::tx/size execution)
        exstatus (::status execution)]
    (or
     (and (zero? exsize) (#{::in-flight ::rejected ::created ::cancelled} exstatus))
     (and (= psize exsize) (#{::filled} exstatus))
     (and (> 0M exsize psize) (#{::partially-filled ::cancelled} exstatus)))))

(defn valid-execution-side?
  "Returns logical true iff the tx/side of execution is the same one as parameters"
  [{::keys [parameters execution]}]
  (= (::tx/side parameters) (::tx/side execution)))

(defn valid-execution?
  "Returns logical true iff the execution properties of order `o` are valid in contrast to order
   parameters"
  [o]
  (and (valid-execution-size? o)
       (valid-execution-side? o)))

(defn finished-status?
  "Returns logical true if order-status is in a finished state, nil otherwise"
  [order-status]
  (#{::cancelled ::rejected ::filled} order-status))

(defn finished?
  "Returns logical true if order is in a finished state (filled, cancelled, rejected), nil otherwise"
  [o]
  (finished-status? (-> o ::execution ::status)))

(defn forward-execution?
  "Returns true whenever o2 execution properties make progress from o1 execution, else false
   E.g o1.execution.status = ::in-flight and o2.execution.status = ::created
   
   Only uses ::tx/size and ::status."
  [{exec1 ::execution} {exec2 ::execution}]
  (let [{status1 ::status size1 ::tx/size} exec1
        {status2 ::status size2 ::tx/size} exec2
        valid-st? (valid-status-transition? status1 status2)
        cancelled? (#{::cancelled} status2)
        both-partf? (and (#{::partially-filled} status1)
                         (#{::partially-filled} status2))]
    (cond
      (not valid-st?) false
      (and cancelled? (>= size1 size2)) true
      (and (not both-partf?) (not cancelled?)) true
      (> size1 size2) true
      :else false)))

(comment
  (forward-execution? {::execution {::tx/size 25M
                                    ::status ::partially-filled}}
                      {::execution {::tx/size 0M
                                    ::status ::created}}))

(defn diverged-execution?
  "Returns logical true whenever o2 has execution properties that diverge from the ones in o1
   (i.e they are not a forward or backward state from one another, are inconsistent)
   E.g o1.execution.status = rejected and o2.execution.status = filled."
  [{exec1 ::execution :as o1} {exec2 ::execution :as o2}]
  (and (not (forward-execution? o1 o2))
       (not (forward-execution? o2 o1))
       (not (= (select-keys exec1 [::tx/size ::status])
               (select-keys exec2 [::tx/size ::status])))))

(defn empty-execution
  [{::keys [parameters]}]
  {::tx/size 0M
   ::tx/value 0M
   ::tx/side (::tx/side parameters)
   ::status ::in-flight})

(defn init-execution
  "Given order `o`, initializes an execution with ::in-flight status and empty transaction properties"
  [o]
  (assoc o ::execution (empty-execution o)))

(defn transition
  "Transition to a new status, except partially-filled. Use ingest-trade for transitions when
   new trades occur."
  [o new-status]
  (update o ::execution assoc ::status new-status))

(comment
  (diverged-execution? {::execution {::tx/size 0M
                                     ::status ::rejected}}
                       {::execution {::tx/size 0M
                                     ::status ::created}}))

(defn valid-trade?
  "Returns logical true iff trade `t` is a valid trade from order `o`
   Checks that:
   - size is not bigger than remaining size to fill (o.parameters.tx/size - o.execution.tx/size)
   - t.tx/side = o.parameters.tx/side"
  [{::keys [parameters execution]} t]
  (and (= (::tx/side parameters) (::tx/side t))
       (<= (::tx/size t) (- (::tx/size parameters) (::tx/size execution)))))

(defn ingest-trade
  "Ingests given trade, updating execution properties to include those of given trade
   Calculations 
   Returns updated order"
  [{::keys [execution parameters] :as o} t mkt]
  (update
   o
   ::execution
   (fn [exec]
     (let [{exsize ::tx/size exval ::tx/value exfee ::fee/fee} execution
           {psize ::tx/size} parameters
           {tsize ::tx/size tval ::tx/value tfee ::fee/fee} t

           new-size (+ exsize tsize)
           new-val (+ exval tval)
           new-price (tx/calculate-avg-price [exec t] mkt)
           new-fee (cond
                     (and exfee tfee) (fee/accumulate-fees [exfee tfee])
                     (not tfee) exfee
                     :else tfee)
           filled? (= psize new-size)
           new-status (if filled? ::filled ::partially-filled)]
       (merge exec
              {::tx/size new-size
               ::tx/value new-val
               ::tx/price new-price
               ::status new-status}
              (when new-fee {::fee/fee new-fee}))))))

(def cancellation-transitions
  {::in-flight #{::created ::rejected}
   ::created #{}
   ::rejected #{}})

(defn valid-cancellation-transition?
  [old-c new-c]
  ((cancellation-transitions old-c) new-c))

(defn forward-cancellation?
  "Returns true iff order2 has a cancellation that is"
  [{c1 ::cancellation} {c2 ::cancellation}]
  (cond
    (and (not c1) c2) true
    (and c1 c2 (valid-cancellation-transition? c1 c2)) true
    :else false))

(defn init-cancellation
  [o]
  (assoc o ::cancellation ::in-flight))

(defn remaining-tx
  [{::keys [parameters execution] :as o}]
  (tx/towards (or execution (empty-execution o))
              parameters))