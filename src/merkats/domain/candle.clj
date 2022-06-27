(ns merkats.domain.candle
  (:require [merkats.domain.trade :as trade]
            [merkats.domain.transaction :as tx]
            [clojure.alpha.spec :as s]
            [merkats.domain.time.interval :as interval]
            [merkats.extensions.spec :as es]))

; --------------------------------------------------------------------------------------------------
; Attributes

(es/def ::open
  "First candle value"
  number?)

(es/def ::close
  "Last candle value"
  number?)

(es/def ::high
  "Highest candle value"
  number?)

(es/def ::low
  "Lowest candle value"
  number?)

(es/def ::volume
  "Volume of this candle"
  number?)

(es/def ::trades
  "Numbero of trades of this candle"
  nat-int?)

(es/def ::candle
  "See https://www.investopedia.com/terms/c/candlestick.asp"
  (s/union ::interval/interval
           [::open
            ::close
            ::high
            ::low
            ::volume
            ::trades]))

(es/def ::candles (s/coll-of ::candles))

; Attributes
; --------------------------------------------------------------------------------------------------

; --------------------------------------------------------------------------------------------------
; Fns

(defn ingest-values
  "Ingests the given values, properly updating ohlc properties on candle"
  [candle values]
  (if-not (empty? values)
    (let [values (vec values)
          highest-value (reduce max values)
          lowest-value (reduce min values)
          close (peek values)
          open (or (::open candle) (first values))
          high (if-let [curr-high (::high candle)]
                 (max curr-high highest-value)
                 highest-value)
          low (if-let [curr-low (::low candle)]
                (min curr-low lowest-value)
                lowest-value)]
      (merge candle
             {::open open
              ::close close
              ::high high
              ::low low}))
    candle))

(defn ingest-trades
  "Ingests the given `trades`, updating ohlc and trading properties accordingly."
  [candle trades]
  (if-not (empty? trades)
    (let [trades (vec trades)
          prices (mapv ::tx/price trades)
          sizes (mapv ::tx/size trades)
          new-volume ((fnil + 0M) (::volume candle) (apply + sizes))
          new-trades ((fnil + 0) (::trades candle) (count trades))]
      (merge (ingest-values candle prices)
             {::volume new-volume
              ::trades new-trades}))
    candle))

(defn join
  "Joins c2 into c1, using the following logic:
   - open from c1
   - close from c2
   - high/low as highest/lowest between c1 and c2
   - volume and trades c1 + c2"
  [{o1 ::open c1 ::close h1 ::high l1 ::low t1 ::trades v1 ::volume :as c1}
   {o2 ::open c2 ::close h2 ::high l2 ::low t2 ::trades v2 ::volume}]
  (merge c1
         {::open o1
          ::close c2
          ::high (max h1 h2)
          ::low (min l1 l2)
          ::volume (+ v1 v2)
          ::trades (+ t1 t2)}))

(defn empty-at-price
  "Creates an empty candle (volume and trades to zero), where all OHLC properties are price p.
   These candles are sometimes used as 'fillers' for intervals with no trades."
  [itv p]
  (merge itv
         {::open   p
          ::close  p
          ::high   p
          ::low    p
          ::trades 0
          ::volume 0M}))

(comment
  ;; Building candles

  (ingest-values {} [1 2 3 4])
  (ingest-values {::high 5 ::low 1 ::open 1 ::close 3} [0 2 3 6])


  (ingest-trades {}
                 [{::trade/price 0 ::trade/size 1}
                  {::trade/price 2 ::trade/size 1}
                  {::trade/price 3 ::trade/size 1}
                  {::trade/price 6 ::trade/size 1}])

  (ingest-trades {::high 5 ::low 1 ::open 1 ::close 3
                  ::trades 5 ::volume 5M}
                 [{::trade/price 0 ::trade/size 1}
                  {::trade/price 2 ::trade/size 1}
                  {::trade/price 3 ::trade/size 1}
                  {::trade/price 6 ::trade/size 1}]))

; Fns
; --------------------------------------------------------------------------------------------------