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

(defprotocol StreamCandles :extend-via-metadata true
  (-stream-candles [this instrument timeframe out close?]))

(defprotocol StreamOrderbook :extend-via-metadata true 
  (-stream-orderbook [this instrument out close?]))

(defprotocol StreamTrades :extend-via-metadata true 
  (-stream-trades [this instrument out close?]))

(defprotocol StreamOrderUpdates :extend-via-metadata true
  (-stream-order-updates [this out close?]))

(defprotocol StreamPositions :extend-via-metadata true
  (-stream-positions [this out close?]))

(defprotocol StreamBalances :extend-via-metadata true
  (-stream-balances [this out close?]))

(defprotocol StreamHistoricalTrades :extend-via-metadata true
  (-stream-historical-trades [this instrument from to out close?]))

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

;; Realtime

(defn stream-candles 
  ([provider instrument timeframe out]
   (stream-candles provider instrument timeframe out true))
  ([provider instrument timeframe out close?]
   (-stream-candles provider instrument timeframe out close?)))

(defn stream-orderbook
  ([provider instrument out]
   (stream-orderbook provider instrument out true))
  ([provider instrument out close?]
   (-stream-orderbook provider instrument out close?)))

(defn stream-trades
  ([provider instrument out]
   (stream-trades provider instrument out true))
  ([provider instrument out close?]
   (-stream-trades provider instrument out close?)))

(defn stream-order-updates
  ([provider out]
   (stream-order-updates provider out true))
  ([provider out close?]
   (-stream-order-updates provider out close?)))

(defn stream-positions
  ([provider out]
   (stream-positions provider out true))
  ([provider out close?]
   (-stream-positions provider out close?)))

(defn stream-balances
  ([provider out]
   (stream-balances provider out true))
  ([provider out close?]
   (-stream-balances provider out close?)))

;; Historical

(defn stream-historical-trades
  ([provider instrument from to out]
   (stream-historical-trades provider instrument from to out true))
  ([provider instrument from to out close?]
   (-stream-historical-trades provider instrument from to out close?)))