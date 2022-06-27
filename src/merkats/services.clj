(ns merkats.services
  (:require [tick.core :as t]
            ))

;; OrderExecution

(defprotocol OrderExecution :extend-via-metadata true 
  (-open-order [this o] "Returns order, errors will contain ::anomaly/category") 
  (-cancel-order [this o] "Returns order, errors will contain ::anomaly/category") 
  (-get-order [this o] "Returns order, errors will contain ::anomaly/category") 
  (-get-order-trades [this o] "Returns vector of order updates containing ::order/trade, or ::anomaly"))

;; Market data

(defprotocol GetCandles :extend-via-metadata true
             (-get-candles [this instrument timeframe from to]))

(defprotocol GetOrderbook :extend-via-metadata true
             (-get-orderbook [this instrument]))

;; Streaming

(defprotocol CloseStream :extend-via-metadata true
  "Streams are core.async channels that implement this protocol via metadata.
   Calling `-close-stream` must cleanup any resource on the producer's side (e.g.unsub from websocket, etc...), 
   and then core.async/close! the chan." 
  (-close-stream! [this]))

(defprotocol StreamCandles :extend-via-metadata true
  (-stream-candles [this instrument timeframe buf-or-n]))

(defprotocol StreamOrderbook :extend-via-metadata true 
  (-stream-orderbook [this instrument buf-or-n]))

(defprotocol StreamTrades :extend-via-metadata true 
  (-stream-trades [this instrument buf-or-n]))

(defprotocol StreamOrderUpdates :extend-via-metadata true
  (-stream-order-updates [this buf-or-n]))

(defprotocol StreamPositions :extend-via-metadata true
  (-stream-positions [this buf-or-n]))

(defprotocol StreamBalances :extend-via-metadata true
  (-stream-balances [this buf-or-n]))

(defprotocol StreamHistoricalTrades :extend-via-metadata true
  (-stream-historical-trades [this instrument from to buf-or-n]))

;; FNs

(defn open-order [this o] (-open-order this o))
(defn cancel-order [this o] (-cancel-order this o))
(defn get-order [this o] (-get-order this o))
(defn get-order-trades [this o] (-get-order-trades this o))

(defn get-candles
  ([provider instrument timeframe from]
   (-get-candles provider instrument timeframe from (t/now)))
  ([provider instrument timeframe from to]
   (-get-candles provider instrument timeframe from to)))

(defn get-orderbook [provider instrument] (-get-orderbook provider instrument))

(defn close-stream! [stream] (-close-stream! stream))

;; Realtime

(defn stream-candles 
  [provider instrument timeframe buf-or-n]
  (-stream-candles provider instrument timeframe buf-or-n))

(defn stream-orderbook
  [provider instrument buf-or-n]
  (-stream-orderbook provider instrument buf-or-n))

(defn stream-trades
  [provider instrument buf-or-n]
  (-stream-trades provider instrument buf-or-n))

(defn stream-order-updates 
  [provider buf-or-n]
  (-stream-order-updates provider buf-or-n))

(defn stream-positions
  [provider buf-or-n]
  (-stream-positions provider buf-or-n))

(defn stream-balances
  [provider buf-or-n]
  (-stream-balances provider buf-or-n))

;; Historical

(defn stream-historical-trades
  [provider instrument from to buf-or-n]
  (-stream-historical-trades provider instrument from to buf-or-n))