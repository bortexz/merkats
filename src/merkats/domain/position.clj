(ns merkats.domain.position
  (:require [clojure.alpha.spec :as s]
            [merkats.domain.market :as market]
            [merkats.domain.trade :as trade]
            [merkats.domain.transaction :as tx]
            [merkats.domain.balance :as balance]
            [merkats.extensions.spec :as es]))

(es/def ::id any?)

(es/def ::entry
  "Transaction representing the entry to this position"
  (s/schema [::tx/side
             ::tx/size
             ::tx/value
             ::tx/price]))

(es/def ::profit-loss
  "cash difference between what you'd get by getting out of this position and the cost of
   entering the position. Positive is profit, negative is loss."
  decimal?)

(es/def ::profit-loss-rate
  "Percentage representation of profit-loss in contrast to cash value of the position"
  decimal?)

(es/def ::equity
  "Amount of cash that you'd get by getting out of this position.
   equity = entry.tx/value + profit-loss"
  decimal?)

(es/def ::performance
  "Performance of this position (pnl, equity) if the position would be exited at tx/price."
  (s/schema [::profit-loss
             ::profit-loss-rate
             ::equity
             ::tx/price]))

(es/def ::position
  (s/schema [::market/symbol
             ::id
             ::entry
             ::performance]))

(es/def ::update
  (s/union ::position [::balance/change]))

(defn has-entry? 
  "Checks if the given position has an entry"
  [p]
  (some? (::entry p)))

(defmulti profit-loss 
  "Calculates profit-loss of position `p` in context of market `mkt` at given price `at-price`"
  (fn [pos at-price mkt] [(::market/direction mkt) (-> pos ::entry ::tx/side)]))

(defmethod profit-loss [::market/linear ::tx/buy]
  [{{::tx/keys [size price]} ::entry} at-price mkt]
  (- (tx/calculate-value {::tx/size size ::tx/price at-price} mkt)
     (tx/calculate-value {::tx/size size ::tx/price price} mkt)))

(defmethod profit-loss [::market/linear ::tx/sell]
  [{{::tx/keys [size price]} ::entry} at-price mkt]
  (- (tx/calculate-value {::tx/size size ::tx/price price} mkt)
     (tx/calculate-value {::tx/size size ::tx/price at-price} mkt)))

(defmethod profit-loss [::market/inverse ::tx/buy]
  [{{::tx/keys [size price]} ::entry} at-price mkt]
  (- (tx/calculate-value {::tx/size size ::tx/price price} mkt)
     (tx/calculate-value {::tx/size size ::tx/price at-price} mkt)))

(defmethod profit-loss [::market/inverse ::tx/sell]
  [{{::tx/keys [size price]} ::entry} at-price mkt]
  (- (tx/calculate-value {::tx/size size ::tx/price at-price} mkt)
     (tx/calculate-value {::tx/size size ::tx/price price} mkt)))

(defn add-performance
  "Adds to position it's performance at a given price. Position must have an entry."
  [pos at-price mkt]
  (let [entry-val (::tx/value (::entry pos))
        pl        (profit-loss pos at-price mkt)
        equity    (+ entry-val pl)
        plr       (/ pl entry-val)]
    (assoc pos ::performance {::profit-loss pl
                              ::profit-loss-rate plr
                              ::equity equity
                              ::tx/price at-price})))

(defn clean-update [pos] (dissoc pos ::balance/change))

(defmulti ingest-trade
  "Ingests the given trade into the position, returning a map of ::position with the updated position,
   and ::balance/change for the balance change in the cash asset for this position. 
   Also updates the position performance to the price of the given trade."
  (fn [{::keys [entry]} t mkt]
    (let [{p-size ::tx/size p-side ::tx/side} entry
          {tx-size ::tx/size tx-side ::tx/side} t]
      (cond
        (not p-size) :open
        (= p-side tx-side) :increase
        :else (case (.compareTo p-size tx-size) -1 :flip 0 :close 1 :decrease)))))

(defmethod ingest-trade :increase ingest-trade-increase-position
  [{::keys [entry] :as p} t mkt]
  (let [{psize ::tx/size} entry
        {tsize ::tx/size tval ::tx/value tprice ::tx/price} t
        new-size (+ psize tsize)
        new-price (tx/calculate-avg-price [entry t] mkt)
        new-entry (tx/add-value (merge entry {::tx/size new-size ::tx/price new-price}) mkt)]
    (-> p
        (assoc ::entry new-entry)
        (add-performance tprice mkt)
        (assoc ::balance/change (- tval)))))

(defmethod ingest-trade :decrease ingest-trade-decrease-position
  [{::keys [entry] :as p} t mkt]
  (let [{tsize ::tx/size tprice ::tx/price} t
        new-entry      (tx/add-value (update entry ::tx/size - tsize) mkt)
        removed-entry  (tx/add-value (assoc entry ::tx/size tsize) mkt)
        balance-change (+ (::tx/value removed-entry)
                          (profit-loss {::entry removed-entry} tprice mkt))]
    (-> p
        (assoc ::entry new-entry)
        (add-performance tprice mkt)
        (assoc ::balance/change balance-change))))

(defmethod ingest-trade :close ingest-trade-close-position
  [p t mkt]
  (let [bch (-> (add-performance p (::tx/price t) mkt) 
                ::performance 
                ::equity)]
    (-> p
        (dissoc ::entry ::performance)
        (assoc ::balance/change bch))))

(defmethod ingest-trade :open ingest-trade-open-position
  [p {::tx/keys [price value] :as t} mkt]
  (let [entry (select-keys t [::tx/value ::tx/side ::tx/size ::tx/price])]
    (-> p
        (assoc ::entry entry)
        (add-performance price mkt)
        (assoc ::balance/change (- value)))))

(def ^:private close-pos-fn (get-method ingest-trade :close))
(def ^:private open-pos-fn (get-method ingest-trade :open))

(defmethod ingest-trade :flip ingest-trade-flip-position
  [{::keys [entry] :as p} t mkt]
  (let [{psize ::tx/size} entry
        [close-t open-t] (tx/split t psize)
        {close-bch ::balance/change :as close-p} (close-pos-fn p close-t mkt)
        {open-bch ::balance/change :as open-p} (open-pos-fn close-p open-t mkt)]
    (assoc open-p ::balance/change (+ close-bch open-bch))))

(comment
  (def lin-mkt {::market/direction ::market/linear})
  (def inv-mkt {::market/direction ::market/inverse})

  (def buy-lin-entry (tx/add-value
                      {::tx/size 100M
                       ::tx/side ::tx/buy
                       ::tx/price 10000M}
                      lin-mkt))

  (profit-loss {::entry buy-lin-entry}
               20000M
               lin-mkt)

  (def buy-inv-entry (tx/add-value
                      {::tx/size 100M
                       ::tx/side ::tx/buy
                       ::tx/price 10000M}
                      inv-mkt))

  buy-inv-entry

  (profit-loss {::entry buy-inv-entry}
               20000M
               inv-mkt)

  (require '[criterium.core :as c])
  (def p {::entry (tx/add-value
                   {::tx/size 100M
                    ::tx/side ::tx/buy
                    ::tx/price 10000M}
                   lin-mkt)})

  (def inc-trade (tx/add-value
                  {::tx/size 50M
                   ::tx/price 15000M
                   ::tx/side ::tx/buy}
                  lin-mkt))


  (with-precision 25 (ingest-trade p inc-trade lin-mkt))
  (c/quick-bench (with-precision 25 (ingest-trade p inc-trade lin-mkt)))

  (def dec-trade (tx/add-value               
                  {::tx/size 50M
                   ::tx/price 15000M
                   ::tx/side ::tx/sell}
                  lin-mkt))

  (with-precision 25 (ingest-trade p dec-trade lin-mkt))
  (c/quick-bench (with-precision 25 (ingest-trade p dec-trade lin-mkt)))

  (def close-trade (tx/add-value
                    {::tx/size 100M
                     ::tx/price 15000M
                     ::tx/side ::tx/sell}
                    lin-mkt))

  (with-precision 25 (ingest-trade p close-trade lin-mkt))
  (c/quick-bench (with-precision 25 (ingest-trade p close-trade lin-mkt)))

  (def flip-trade (tx/add-value
                   {::tx/size 150M
                    ::tx/price 15000M
                    ::tx/side ::tx/sell}
                   lin-mkt))

  (with-precision 25 (ingest-trade p flip-trade lin-mkt))
  (c/quick-bench (with-precision 25 (ingest-trade p flip-trade lin-mkt)))

  (def open-trade (tx/add-value
                   {::tx/size 50M
                    ::tx/price 15000M
                    ::tx/side ::tx/buy}
                   lin-mkt))

  (with-precision 25 (ingest-trade {} open-trade lin-mkt))
  (c/quick-bench (with-precision 50 (ingest-trade {} open-trade lin-mkt)))
  ,)
