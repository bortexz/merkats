(ns merkats.providers.bybit.inverse
  (:refer-clojure :exclude [when-let if-let cond])
  (:require [clojure.core.async :as a]
            [clojure.set :as set]
            [com.stuartsierra.component :as component]
            [merkats.clients.bybit.rest :as bybit.rest]
            [merkats.clients.bybit.public-data :as bybit.public-data]
            [merkats.clients.bybit.websocket :as bybit.websocket]
            [merkats.extensions.core :refer [queue]]
            [merkats.domain.market :as market]
            [merkats.domain.transaction :as tx]
            [merkats.domain.time :as time]
            [merkats.domain.trade :as trade]
            [merkats.domain.fee :as fee]
            [merkats.domain.order :as order]
            [merkats.domain.orderbook :as orderbook]
            [merkats.domain.candle :as candle]
            [merkats.domain.balance :as balance]
            [merkats.domain.time.interval :as interval]
            [merkats.stream :as stream]
            [cljc.java-time.instant :as t.i]
            [cljc.java-time.duration :as t.d]
            [cljc.java-time.local-date :as t.ld]
            [merkats.anomalies :as anomalies]
            [tick.core :as t]
            [better-cond.core :refer [when-let if-let cond]]
            [clojure.string :as str]
            [merkats.services :as services])
  (:import (java.time ZoneOffset LocalDate)))

(def ^:private ->tx-side
  {"Buy" ::tx/buy
   "Sell" ::tx/sell})

(def ^:private <-tx-side
  (set/map-invert ->tx-side))

(def ^:private ->tx-actor
  {"AddedLiquidity" ::tx/maker
   "RemovedLiquidity" ::tx/taker})

;; Historic trades

(def ^:private public-data-format-change-date
  "Bybit public data changed from reverse order (end day first) and seconds.micros timestamps to normal 
   order (start day first) with only seconds timestamps, starting on this date.
   This applies to the :trading index used in `clients.bybit.public-data`"
  (t.ld/parse "2021-12-07"))

(defn- public-data->trade
  "Transforms a bybit public data string to a domain trade. nanos-offset will be used to be added to timestamp
   as nanos offset, so trades in same seconds timestamp can be ordered in same order they appear on 
   bybit public files.
   Tested agains the :trading index on bybit public data.
   Market will be used to manually calculate the value of the trade from given price and size.
   (Preferred over using foreign-notional, gives flexibility to adapt trade to different markets)"
  [trade-str]
  (let [[timestamp
         instr
         side
         _size
         price
         _tick-direction
         trd-match-id
         _gross-value
         home-notional
         foreign-notional] (str/split trade-str #",")
        ; Not using micros because they removed it on [[bybit-format-change-date]]
        ; but before they were included as seconds.micros
        [seconds _]        (mapv parse-long (str/split timestamp #"\."))]
    {::time/stamp (t.i/of-epoch-second seconds)
     ::trade/id trd-match-id
     ::market/symbol instr
     ::tx/size (bigdec home-notional)
     ::tx/price (bigdec price)
     ::tx/value (bigdec foreign-notional)
     ::tx/actor ::tx/taker
     ::tx/side (->tx-side side)}))

;; Candles

(def ^:private timeframe-minutes [1 3 5 15 30 60 120 240 360 720])

(def ^:private <-timeframe
  "Map from ::time/frame  to bybit interval, supporting from 1m to 12hours"
  (zipmap (map t.d/of-minutes timeframe-minutes)
          (map str timeframe-minutes)))

(def ^:private ->timeframe
  "Mam from bybit intervals to ::time/frame, inverse of [[<-timeframe]]"
  (set/map-invert <-timeframe))

(defn- rest->candle
  [{:keys [symbol interval open_time open high low close volume]}]
  (let [tf (->timeframe interval)
        from (t.i/of-epoch-second open_time)
        to (t/>> from tf)]
    {::market/symbol symbol

     ::time/frame tf

     ::interval/from from
     ::interval/to to

     ::candle/open (bigdec open)
     ::candle/close (bigdec close)
     ::candle/high (bigdec high)
     ::candle/low (bigdec low)
     ::candle/volume (bigdec volume)}))

(defn ws->candle
  [instrument timeframe {:keys [start end open close high low volume]}]
  {::market/symbol instrument
   ::time/frame timeframe

   ::candle/open (bigdec open)
   ::candle/close (bigdec close)
   ::candle/high (bigdec high)
   ::candle/low (bigdec low)

   ::interval/from (t.i/of-epoch-second start)
   ::interval/to (t.i/of-epoch-second end)

   ::candle/volume (bigdec volume)})

(defn candles-topic
  [instrument timeframe]
  (format "klineV2.%s.%s" (<-timeframe timeframe) instrument))

(comment
  (def example-kline {:open "38289.5",
                      :open_time 1645747500,
                      :turnover "0.09106212",
                      :symbol "BTCUSD",
                      :close "38292",
                      :volume "3487",
                      :high "38312.5",
                      :low "38289.5",
                      :interval "1"})
  (rest->candle example-kline))

;; Trades

(defn ws->trade
  [{:keys [trade_time_ms symbol side size price trade_id]}]
  {::time/stamp (t.i/of-epoch-milli trade_time_ms)
   ::market/symbol symbol

   ::tx/side (->tx-side side)
   ::tx/price (bigdec price)
   ::tx/size (bigdec size)

   ::trade/id trade_id})

(defn trades-topic
  [instrument]
  (format "trade.%s" instrument))

;; Orderbook

(defn ->orderbook-row
  [row]
  ((juxt (comp ->tx-side :side) (comp bigdec :price) :size) row))

(defn ws->apply-orderbook-delta
  [book {:keys [delete update insert]}]
  (let [new-rows (->> (concat delete update insert)
                      (map ->orderbook-row))]
    (orderbook/update-book book new-rows)))

(defn ws->orderbook-xf
  "Builds orderbook from websocket messages"
  []
  (fn [rf]
    (let [book (volatile! (orderbook/book))]
      (fn
        ([] (rf))
        ([r] (rf r))
        ([r {:keys [type data]}]
         (case type
           "snapshot" (vreset! book (orderbook/book (map ->orderbook-row data)))
           "delta"    (vswap! book ws->apply-orderbook-delta data))
         (rf r @book))))))

(defn orderbook-topic
  [instrument]
  (format "orderBookL2_25.%s" instrument))

;; Orders and executions

(def ^:private <-time-in-force
  {::order/fill-or-kill "FillOrKill"
   ::order/immediate-or-cancel "ImmediateOrCancel"
   ::order/good-til-cancel "GoodTillCancel"})

(defn- <-order-type
  [{::tx/keys [actor]}]
  (if (#{::tx/taker} actor)
    "Market"
    "Limit"))

(defn- open-request<-order
  [{::order/keys [id parameters] :as o}]
  (let [side  (<-tx-side (::tx/side parameters))
        otype (<-order-type parameters)
        tif   (if (#{::tx/maker} (::tx/actor parameters))
                "PostOnly"
                (<-time-in-force (::order/time-in-force parameters)))]
    (cond-> {:order_link_id id
             :side          side
             :symbol        (::market/symbol o)
             :order_type    otype
             :qty           (::tx/size parameters)
             :time_in_force tif}

      (#{"Limit"} otype) (assoc :price (::tx/price parameters)))))

(def ^:private ->order-status
  {"Created" ::order/in-flight ;; Not yet in system, can still be rejected
   "New" ::order/created
   "Rejected" ::order/rejected
   "PartiallyFilled" ::order/partially-filled
   "Filled" ::order/filled
   "Cancelled" ::order/cancelled})

(def ^:private execution-status? (set (keys ->order-status)))
(def ^:private cancellation-status? #{"PendingCancel" "Cancelled"}) 1

(defn- get-raw-order
  [rest-client o]
  (let [rparams {:symbol (::market/symbol o)
                 :order_link_id (::order/id o)}
        res (bybit.rest/request rest-client {:method :get
                                             :path "/v2/private/order"
                                             :params rparams})]
    res))

(defn- add-order-execution
  "Adds order execution from `result` to `o` whenever the results order_status is a supported one,
   otherwise returns `o` unchanged."
  [o {:keys [order_status cum_exec_qty] :as result}]
  (cond-> o
    (execution-status? order_status)
    (assoc ::order/execution
           {::tx/size cum_exec_qty
            ::order/status (->order-status order_status)})))

(defn- add-order-cancellation
  "Adds cancellation whenever the result contains a cancellation result"
  [o {:keys [order_status] :as result}]
  (cond-> o
    (cancellation-status? order_status)
    (assoc ::order/cancellation ::order/created)))

(defn- open-response->order
  [o {anomaly ::anomalies/category result :result :as res}]
  (cond
    (or (some? anomaly) (not (zero? (:ret_code res))))
    (merge o {::anomalies/category anomaly
              ::order/execution {::order/status ::order/rejected}})

    (-> o
        (assoc ::time/stamp (t.i/parse (:updated_at result)))
        (add-order-execution result))))

(defn- cancel-response->order
  [o {anomaly ::anomalies/category result :result :as res}]
  (cond
    (or (some? anomaly) (not (zero? (:ret_code res))))
    (merge o {::anomalies/category anomaly
              ::order/cancellation ::order/rejected})

    (-> o (merge {::time/stamp (t.i/parse (:updated_at result))
                  ::order/cancellation ::order/created})
        (add-order-execution result))))

(defn- get-response->order
  [o {anomaly ::anomalies/category result :result :as res}]
  (cond
    (or (some? anomaly) (not (zero? (:ret_code res))))
    (merge o {::anomalies/category anomaly})

    (-> o
        (assoc ::time/stamp (t.i/parse (:updated_at result)))
        (add-order-execution result)
        (add-order-cancellation result))))

(defn- rest-user->trade
  [{:keys [exec_fee exec_id exec_price exec_qty trade_time_ms exec_value
           fee_rate order_link_id side symbol last_liquidity_ind] :as raw}]
  {::order/id order_link_id
   ::trade/trade {::trade/id exec_id

                  ::market/symbol symbol

                  ::time/stamp (t.i/of-epoch-second trade_time_ms)

                  ::tx/size (bigdec exec_qty)
                  ::tx/side (->tx-side side)
                  ::tx/value (bigdec exec_value)
                  ::tx/actor (->tx-actor last_liquidity_ind)
                  ::tx/price (bigdec exec_price)

                  ::fee/fee {::fee/rate (- (bigdec fee_rate))
                             ::balance/change (- (bigdec exec_fee))}}})

(defn ws-order->update
  [{:keys [order_link_id timestamp] :as result}]
  (-> {::id order_link_id
       ::time/stamp timestamp}
      (add-order-execution result)
      (add-order-cancellation result)))

(defn ws-execution->update
  [{:keys [side order_link_id exec_id price exec_qty exec_fee is_maker trade_time symbol]}]
  {::order/id order_link_id
   ::trade/trade
   (-> {::market/symbol symbol

        ::time/stamp (t.i/parse trade_time)

        ::trade/id exec_id

        ::fee/fee {::balance/change (- (bigdec exec_fee))}

        ::tx/side (->tx-side side)
        ::tx/size (bigdec exec_qty)
        ::tx/price (bigdec price)
        ::tx/actor (if is_maker ::tx/maker ::tx/taker)}
       (tx/add-value {::market/direction ::market/inverse}))})

;; Historical data provider

(defn stream-historical-trades
  "Returns Closeable Object that puts trades into ch from given instrument fromdate todate using
   the historic data repository in bybit."
  [_provider instrument from to out close?]
  (let [from-date  (t/date from)
        to-date    (t/date to)
        dates-seq  (take-while #(or (t.ld/is-before % to-date) (t.ld/is-equal % to-date))
                               (iterate #(t.ld/plus-days % 1) from-date))

        stopped?_ (atom false)
        stop-ch (a/chan)]
    (a/thread
      (loop [days dates-seq]
        (when-let [_continue? (not @stopped?_)
                   next-day (first days)]
          (let [should-reverse? (t.ld/is-before next-day public-data-format-change-date)

                file-lines      (cond->
                                 (bybit.public-data/download-as-lines!
                                  {:index    :trading
                                   :date-str (t.ld/to-string next-day)
                                   :market   instrument})

                                  should-reverse? (reverse))

                trades-queue    (->> file-lines
                                     (map public-data->trade)
                                     (filter (fn [t] (t/< from (::time/stamp t) to)))
                                     (partition-by (juxt ::time/stamp ::tx/price))
                                     (into (queue)))]
            (loop [q trades-queue]
              (when (seq q)
                (a/alt!!
                  [[out (vec (peek q))]] (recur (pop q))
                  stop-ch (do (reset! stopped?_ true)
                              (when close? (a/close! out))))))
            (recur (rest days))))))
    stop-ch))

(defrecord HistoricalProvider []
  services/StreamHistoricalTrades
  (-stream-historical-trades [this instrument from to out close?]
    (stream-historical-trades this instrument from to out close?)))

(defn historical-provider [] (->HistoricalProvider))

(comment
  (def out-ch (a/chan 1))
  (def stop (stream-historical-trades
             nil
             "BTCUSD"
             (t/instant "2022-01-01T00:00:00.00Z")
             (t/instant "2022-01-02T00:00:00.00Z")
             out-ch
             true))

  (a/<!! out-ch)
  (a/close! stop)
  (a/<!! out-ch))

;; REST Provider

(defn get-candles
  [{:keys [rest-client req-timeout]} instrument timeframe from to]
  (let [res (doall
             (iteration
              (fn [begin]
                (let [req {:method :get
                           :path   "/v2/public/kline/list"
                           :params {:symbol   instrument
                                    :limit    200
                                    :from     (t.i/get-epoch-second begin)
                                    :interval (<-timeframe timeframe)
                                    :timeout  req-timeout}}
                      res (bybit.rest/request rest-client req)]
                  (if (some-> (:ret_code res) zero?)
                    {::candle/candles (mapv rest->candle (:result res))}
                    {::anomalies/category (or (::anomalies/category res)
                                              ::anomalies/fault)
                     :res                res})))
              {:initk from
               :somef (fn [r] (::candle/candles r))
               :kf    (fn [r] (let [cs      (::candle/candles r)
                                    latest  (::interval/to (peek cs))]
                                (when (t/<= latest to)
                                  latest)))}))]
    (if (every? ::candle/candles res)
      (->> res
           (mapcat ::candle/candles)
           (take-while (fn [c] (t/<= (::interval/from c) to)))
           (into []))
      (last res))))

(comment
  (get-candles {:rest-client (bybit.rest/client {:env :testnet})}
               "BTCUSD"
               (t/new-duration 5 :minutes)
               (t/instant "2022-01-01T00:00:00Z")
               (t/instant "2022-01-01T06:00:00Z")))

(defn get-orderbook
  [{:keys [rest-client req-timeout]} instrument]
  (let [req {:method :get
             :path "/v2/public/orderBook/L2"
             :params {:symbol instrument}
             :timeout req-timeout}
        res (bybit.rest/request rest-client req)]
    (if-let [ret-code (:ret_code res)
             _ok? (zero? ret-code)]
      (orderbook/book (map ->orderbook-row (:result res)))
      (merge {::anomalies/category ::anomalies/fault}
             res))))

(comment
  (get-orderbook {:rest-client (bybit.rest/client {})} "BTCUSD"))

(defn open-order
  [{:keys [rest-client req-timeout]} o]
  (let [rparams (open-request<-order o)
        res (bybit.rest/request rest-client {:method :post
                                             :path "/v2/private/order/create"
                                             :params rparams
                                             :timeout req-timeout})]
    (open-response->order o res)))

(defn cancel-order [{:keys [rest-client req-timeout]} o]
  (let [rparams {:symbol (::market/symbol o)
                 :order_link_id (::order/id o)}
        res (bybit.rest/request rest-client {:method :post
                                             :path "/v2/private/order/cancel"
                                             :params rparams
                                             :timeout req-timeout})]
    (cancel-response->order o res)))

(defn get-order
  [{:keys [rest-client]} o]
  (get-response->order o (get-raw-order rest-client o)))

(defn get-order-trades
  [{:keys [rest-client]} o]
  (let [raw-order (get-raw-order rest-client o)
        oid (-> raw-order :result :order_id)]
    (if oid
      (let [rparams {:order_id oid
                     :symbol (::market/symbol o)}
            res (bybit.rest/request rest-client {:method :get
                                                 :path "/v2/private/execution/list"
                                                 :params rparams})]
        (cond
          :let [ret-code   (:ret_code res)
                trade-list (-> res :result :trade_list)]
          (zero? ret-code) (map rest-user->trade trade-list)
          (merge {::anomalies/category anomalies/fault}
                 res)))
      (merge {::anomalies/caterogy anomalies/fault}
             raw-order))))

(defrecord RestProvider [rest-client
                         req-timeout]
  services/GetCandles
  (-get-candles [this instrument timeframe from to] (get-candles this instrument timeframe from to))

  services/GetOrderbook
  (-get-orderbook [this instrument] (get-orderbook this instrument))

  services/OrderExecution
  (-open-order [this o] (open-order this o))
  (-cancel-order [this o] (cancel-order this o))
  (-get-order [this o] (get-order this o))
  (-get-order-trades [this o] (get-order-trades this o)))

(defn rest-provider [{:keys [auth env req-timeout] :or {req-timeout 10000}}]
  (->RestProvider (bybit.rest/client {:auth auth :env env}) req-timeout))

(defn rest-provider-component
  "Accepts same opts as rest-provider"
  [opts]
  (with-meta {}
    {`com.stuartsierra.component/start
     (fn [_]
       (with-meta (rest-provider opts)
         {`com.stuartsierra.component/stop
          (fn [_]
            (rest-provider-component opts))}))}))

;; WEBSOCKET provider

(defn- on-stream-connected
  [topics->setup connection data-pub topics create-ch out]
  (if-let [existing (get topics->setup topics)]
    (do (a/tap (:mult existing) out)
        (update-in topics->setup [topics :taps] conj out))
    (let [ch (create-ch)
          setup {:ch ch :mult (a/mult ch) :taps #{out}}]
      (a/tap (:mult setup) out false)
      (doseq [t topics] (a/sub data-pub t ch))
      (bybit.websocket/subscribe connection topics)
      (assoc topics->setup topics setup))))

(defn- on-stream-closed
  [topics->setup connection data-pub topics out close?]
  (let [setup (get topics->setup topics)
        new-setup (update setup :taps disj out)
        cleanup? (empty? (:taps new-setup))]
    (a/untap (:mult setup) out)
    (when close? (a/close! out))
    (if cleanup?
      (do (a/close! (:ch setup))
          (doseq [t topics] (a/unsub data-pub t (:ch setup)))
          (bybit.websocket/unsubscribe connection topics)
          (dissoc topics->setup topics))
      (assoc topics->setup topics new-setup))))

(defn- create-stream
  [{:keys [data-pub topics->setup_ connection closed?_]} topics create-ch out close?]
  (when @closed?_ (throw (ex-info "Provider is already closed" {})))
  (let [stop-ch (a/chan)]
    (a/go (a/<! stop-ch)
          (when-not @closed?_
            (send topics->setup_ on-stream-closed connection data-pub topics out close?)))
    (send topics->setup_ on-stream-connected connection data-pub topics create-ch out)
    stop-ch))

(defn stream-candles
  [provider instrument timeframe out close?]
  (let [topics #{(candles-topic instrument timeframe)}
        ws->candle (partial ws->candle instrument timeframe)
        create-ch #(a/chan 16 (map (fn [evt] (mapv ws->candle (:data evt)))))]
    (create-stream provider topics create-ch out close?)))

(defn stream-orderbook
  [provider instrument out close?]
  (let [topics #{(orderbook-topic instrument)}
        create-ch #(a/chan (a/sliding-buffer 1) (ws->orderbook-xf))]
    (create-stream provider topics create-ch out close?)))

(defn stream-trades
  [provider instrument out close?]
  (let [topics #{(trades-topic instrument)}
        create-ch #(a/chan 16 (map (fn [evt] (mapv ws->trade (:data evt)))))]
    (create-stream provider topics create-ch out close?)))

(defn stream-order-updates
  [provider out close?]
  (let [xf (map (fn [{:keys [topic data]}]
                  (case topic
                    "execution" (map ws-execution->update data)
                    "order" (map ws-order->update data))))
        topics #{"order" "execution"}
        create-ch #(a/chan 16 xf)]
    (create-stream provider topics create-ch out close?)))

(defrecord WebsocketProvider [connection
                              data-ch
                              data-pub

                              topics->setup_ ; connection is {:ch ... :mult ... :taps #{<ch>}}
                              closed?_]
  java.lang.AutoCloseable
  (close [_]
    (reset! closed?_ true)
    (.close connection)
    (a/close! data-ch))

  services/StreamCandles
  (-stream-candles [this instrument timeframe out close?] (stream-candles this instrument timeframe out close?))

  services/StreamOrderbook
  (-stream-orderbook [this instrument out close?] (stream-orderbook this instrument out close?))

  services/StreamTrades
  (-stream-trades [this instrument out close?] (stream-trades this instrument out close?))

  services/StreamOrderUpdates
  (-stream-order-updates [this out close?] (stream-order-updates this out close?)))

(defn websocket-provider
  [{:keys [resocket-opts env auth data-buf]
    :or {data-buf 1024}}]
  (let [data-ch (a/chan data-buf)
        data-pub (a/pub data-ch :topic)
        connection (bybit.websocket/inverse-connection
                    {:resocket-opts resocket-opts
                     :env env
                     :auth auth
                     :on-data (fn [m] (a/>!! data-ch m))})]

    (->WebsocketProvider connection
                         data-ch
                         data-pub
                         (agent {})
                         (atom false))))

(defn websocket-provider-component
  [opts]
  (with-meta {}
    {`com.stuartsierra.component/start
     (fn [_]
       (with-meta
         (websocket-provider opts)
         {`com.stuartsierra.component/stop
          (fn [this]
            (.close this)
            (websocket-provider-component opts))}))}))

(comment
  (set! *math-context* (java.math.MathContext. 25 java.math.RoundingMode/HALF_EVEN))

  ;; REST provider
  (def rp (rest-provider {:auth {:key-id (System/getenv "bybit_testnet_key_id")
                                 :key-secret (System/getenv "bybit_testnet_key_secret")}
                          :env :testnet
                          :req-timeout 6000}))

  (services/get-candles rp "BTCUSD" (t/new-duration 1 :minutes) (t/<< (t/now) (t/new-duration 300 :minutes)))
  (services/get-orderbook rp "BTCUSD")

  (def taker-order
    {::order/id (str (random-uuid))
     ::market/symbol "BTCUSD"
     ::order/parameters {::order/time-in-force ::order/good-til-cancel
                         ::tx/size 25
                         ::tx/side ::tx/sell
                         ::tx/actor ::tx/taker}})


  (services/open-order rp taker-order)
  (services/get-order rp taker-order)
  (services/get-order-trades rp taker-order)

  (def maker-order
    {::order/id (str (random-uuid))
     ::market/symbol "BTCUSD"
     ::order/parameters {::order/time-in-force ::order/good-til-cancel
                         ::tx/size 25
                         ::tx/side ::tx/buy
                         ::tx/price 20000M
                         ::tx/actor ::tx/maker}})

  (services/open-order rp maker-order)
  (services/cancel-order rp maker-order)

  ;; Websocket Provider
  (def wsr (websocket-provider
            {:resocket-opts {:on-connection-error (fn [e] (println e))}
             :env :testnet
             :auth {:key-id (System/getenv "bybit_testnet_key_id")
                    :key-secret (System/getenv "bybit_testnet_key_secret")}}))
  (.close wsr)

  (def out (a/chan 1))
  (def candles-stream (services/stream-candles wsr "BTCUSD" (t/new-duration 5 :minutes) out))
  (a/close! candles-stream)

  (def out (a/chan 1))
  (def orderbook-stream (services/stream-orderbook wsr "BTCUSD" out)) 
  (a/poll! out)
  (a/close! orderbook-stream))
