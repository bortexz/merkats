(ns merkats.indicators
  "By defualt indicators are supported for double numbers, unless specified otherwise"
  (:refer-clojure :exclude [when-let if-let])
  (:require [merkats.graph :as g]
            [clojure.math :as math]
            [clojure.alpha.spec :as s]
            [better-cond.core :refer [when-let if-let]]
            [merkats.extensions.core :refer [ascending-comparator]]
            [merkats.domain.candle :as candle]
            [merkats.domain.candle.chart :as candlechart]
            [merkats.domain.time.interval :as interval]
            [merkats.extensions.sorted :as sort]))

; --------------------------------------------------------------------------------------------------
; Timeseries

(defprotocol UpdateTimeline
  (update-timeline [this value sources compute-ts]))

(defn- keep-latest
  "Micro-optimization: When the difference between current count and desired is small, 
   a dissoc of the first keys goes faster than a subrange of keep, but there's a threshold when it's 
   faster to do a sort/tail-sorted."
  [v keep]
  (let [diff (- (count v) keep)]
    (if (> diff 10)
      (sort/tail-sorted v keep)
      (reduce dissoc v (map #(key (nth v %)) (range diff))))))

(defrecord BaseTimeline [delta keep]
  UpdateTimeline
  (update-timeline [_ value sources compute-ts]
    (let [updated-val (reduce (fn [acc k]
                                (assoc acc k (compute-ts acc sources k)))
                              (or value (sort/sorted-map-by ascending-comparator))
                              delta)]
      (cond-> updated-val
        (> (count updated-val) keep) (keep-latest keep)))))

(defn base-timeline
  "Creates the value of a timeline indicator, given:
   `delta` sorted set of timestamps/keys to be updated
   `keep` maximum number of values to keep in the timeseries.
   This timeline does not allow to remove timeline points, and only old ones will be removed when
   there are more than `keep` values."
  [delta keep]
  (->BaseTimeline (mapv ::interval/from delta) keep))

(defn- timeseries-handler
  [compute-ts]
  (fn timeseries-handler* [current-value {::keys [timeline] :as sources}]
    (update-timeline timeline current-value sources compute-ts)))

(defn timeseries
  [tline sources compute-ts]
  (g/compute-node (merge sources {::timeline tline}) (timeseries-handler compute-ts)))

; Timeseries 
; --------------------------------------------------------------------------------------------------

; --------------------------------------------------------------------------------------------------
; Formulas

(defn -mean
  [values]
  (/ (reduce + 0 values)
     (count values)))

(defn -standard-deviation
  [values]
  (let [mean_ (-mean values)]
    (->> values
         (map #(math/pow (- % mean_) (double 2)))
         (-mean)
         (math/sqrt))))

; Formulas
; --------------------------------------------------------------------------------------------------

(defn candlechart
  "Given an initial chart, input-trades and input-candles (input nodes), creates a graph node whose
   value is a candle/chart. 
   
   Combine it with [[chart->timeline]] and [[chart->candleseries]]"
  [init-chart input-trades input-candles]
  (g/compute-node
   {::input-trades input-trades
    ::input-candles input-candles}
   (fn [val {::keys [input-trades input-candles]}]
     (cond-> (or val init-chart)
       input-trades (candlechart/ingest-trades input-trades)
       input-candles (candlechart/ingest-delta input-candles)))))

(defn candlechart->timeline
  "Graph node that creates a timeline indicator from a chart value, using it's delta."
  [chart-indicator]
  (g/compute-node
   {:charti chart-indicator}
   (fn [_ {:keys [charti]}]
     (base-timeline (::candlechart/delta charti)
                    (::candlechart/max-candles charti)))))

(defn candlechart->series
  "Given a candlechart indicator, returns it's candleseries, suitable to be used as the basis for other
   indicators."
  [chart-indicator]
  (g/compute-node
   {:charti chart-indicator}
   (fn [_ {:keys [charti]}]
     (::candlechart/series charti))))

; --------------------------------------------------------------------------------------------------
; Timeseries Indicators

; Utilities indicators

(defn map-val
  "(f x)"
  [tl series f]
  (timeseries
   tl
   {:series series}
   (fn map-val* [_ {:keys [series]} k]
     (when-let [val (get series k)]
       (f val)))))

(defn multiply-by
  "x * multiplier"
  [tl series multiplier]
  (map-val tl series (fn multiplier* [x] (* x multiplier))))

(defn value-change
  "x - previous-x"
  [tl series]
  (timeseries
   tl
   {:series series}
   (fn value-change* [_ {:keys [series]} k]
     (when-let [current (get series k)
                previous (sort/offset-val series k -1)]
       (- current previous)))))

(defn vertical-offset
  "x + (x * multiplier)"
  [tl series multiplier]
  (map-val tl series (fn vertical-offset* [x] (+ x (* x multiplier)))))

(defn difference
  "x - y"
  [tl x y]
  (timeseries
   tl
   {:x x :y y}
   (fn difference* [_ {:keys [x y]} k]
     (when-let [x* (get x k)
                y* (get y k)]
       (- x* y*)))))

(defn gain-indicator
  "if (pos? x) then x else 0"
  [tl series]
  (map-val tl (value-change tl series) (fn [x] (if (pos? x) x 0))))

(defn loss-indicator
  "if (neg? x) then -x else 0"
  [tl series]
  (map-val tl (value-change tl series) (fn [x] (if (neg? x) (- x) 0))))

(defn sources-mean
  "Indicator that returns the average value of the sources specified as ks."
  [tl sources]
  (let [ks (keys sources)]
    (timeseries
     tl
     sources
     (fn sources-mean-value* [_ sources* k]
       (when-let [vals (->> (select-keys sources* ks)
                            (mapv (fn [[_ sv]] (get sv k))))
                  _all? (every? some? vals)]
         (-mean vals))))))

(defn map-ks-mean
  "Indicator that returns the mean value of values specified by ks from the map at sources.series"
  [tl series ks]
  (timeseries
   tl 
   {:series series}
   (fn map-ks-mean* [_ {:keys [mapseries]} k]
     (when-let [m (get mapseries k)
                vals ((apply juxt ks) m)
                _all? (every? some? vals)]
       (-mean vals)))))

; Candles indicators

(defn high
  [tl candleseries]
  (map-val tl candleseries ::candle/high))

(defn low
  [tl candleseries]
  (map-val tl candleseries ::candle/low))

(defn open
  [tl candleseries]
  (map-val tl candleseries ::candle/open))

(defn close
  [tl candleseries]
  (map-val tl candleseries ::candle/close))

(defn hl2
  "mean([high low])"
  [tl candleseries]
  (map-ks-mean tl candleseries [::candle/high ::candle/low]))

(defn hlc3
  "mean([high low close])"
  [tl candleseries]
  (map-ks-mean tl candleseries [::candle/high ::candle/low ::candle/close]))

(defn ohlc4
  "mean([open high low close])"
  [tl candleseries]
  (map-ks-mean tl candleseries [::candle/open ::candle/high ::candle/low ::candle/close]))

(defn heikin-ashi
  "Heikin ashi"
  [tl candleseries]
  (timeseries
   tl
   candleseries
   (fn heikin-ashi* [ha {:keys [candleseries]} k]
     (when-let [[open close high low :as vals] ((juxt ::candle/open ::candle/close ::candle/high ::candle/low) (get candleseries k))
                _all? (every? some? vals)]
       (let [previous-ha (sort/offset-val ha k -1)
             ha-close (-mean [open close high low])
             ha-open (if previous-ha
                       (-mean [(::candle/open previous-ha) (::candle/close previous-ha)])
                       (-mean [open close]))]
         {::candle/open ha-open
          ::candle/close ha-close
          ::candle/high (max ha-open ha-close high)
          ::candle/low (min ha-open ha-close low)})))))


; Moving averages

(defn simple-moving-average
  [tl series period]
  (timeseries
   tl
   {:series series}
   (fn simple-moving-average* [_ {:keys [series]} k]
     (when-let [tail (sort/full-tail-vals series k period)]
       (-mean tail)))))

(defn weighted-moving-average
  [tl series period]
  (timeseries
   tl
   {:series series}
   (fn weighted-moving-average* [_ {:keys [series]} k]
     (let [tail (sort/full-tail-vals series k period)]
       (/ (->> tail
               (map * (range 1 (inc period)))
               (reduce + 0))
          (/ (* period (inc period)) 2))))))

(defn exponential-moving-average
  [tl series period]
  (let [internal-sma (simple-moving-average tl series period)
        multiplier   (/ 2 (inc period))]
    (timeseries
     tl
     {:series series
      :internal-sma internal-sma}
     (fn exponential-moving-average* [current-val {:keys [internal-sma series]} k]
       (let [previous-ema (sort/offset-val current-val k -1)]
         (when-let [previous-sma (sort/offset-val internal-sma k -1)
                    previous     (or previous-ema previous-sma)
                    current      (get series k)]
           (+ (* (- current previous) multiplier)
              previous)))))))

(defn relative-moving-average
  "https://www.tradingview.com/pine-script-reference/#fun_rma"
  [tl series period]
  (timeseries
   tl
   {:series series
    :internal-sma (simple-moving-average tl series period)}
   (fn relative-moving-average* [curr {:keys [series internal-sma]} k]
     (when-let [src (get series k)]
       (let [prev (sort/offset-val curr k -1)]
         (if-not prev
           (get internal-sma k)
           (/ (+ src (* (dec period) prev)) period)))))))

(defn hull-moving-average
  "HMA"
  [tl series period]
  (let [base-wma     (weighted-moving-average tl series period)
        n2-wma       (weighted-moving-average tl series (int (math/floor (double (/ period 2)))))
        n2-mult      (multiply-by tl n2-wma 2)
        base-n2-diff (difference tl n2-mult base-wma)
        wma-wrap     (weighted-moving-average tl base-n2-diff (int (math/floor (math/sqrt period))))]
    (timeseries
     tl
     {:wma-wrap wma-wrap}
     (fn hull-moving-average* [_ {:keys [wma-wrap]} k]
       (get wma-wrap k)))))

(defn volume-weighted-moving-average
  [tl price-series volume-series period]
  (timeseries
   tl 
   {:price price-series
    :volume volume-series}
   (fn volume-weighted-moving-average* [_ {:keys [price volume]} k]
     (when-let [volume-tail (sort/full-tail-vals volume k period)
                price-tail (sort/full-tail-vals price k period)]
       (/ (->> (map * volume-tail price-tail)
               (reduce + 0))
          (reduce + 0 volume-tail))))))

; Stats indicators

(defn mean-deviation
  [tl series period]
  (timeseries
   tl
   {:series series
    :internal-sma (simple-moving-average tl series period)}
   (fn mean-deviation* [_ {:keys [series internal-sma]} k]
     (when-let [current-sma (get internal-sma k)
                vals        (sort/full-tail-vals series k period)]
       (/ (reduce (fn [acc curr] (+ acc (abs (- curr current-sma)))) 0 vals)
          period)))))

(defn standard-deviation
  [tl series period]
  (timeseries
   tl
   {:series series}
   (fn standard-deviation* [_ {:keys [series]} k]
     (when-let [vals (sort/full-tail-vals series k period)]
       (-standard-deviation vals)))))

; SAR

(defn- internal-parabolic-sar
  "Implementation inspired by ta4j"
  [tl candle-series start increment max-value]
  (timeseries
   tl
   {:series candle-series}
   (fn internal-parabolic-sar* [current {:keys [series]} k]
     (when-let [previous-sar    (sort/offset-val current k -1)
                previous-candle (sort/offset-val series k -1)
                previous-close  (::candle/close previous-candle)
                current-candle  (get series k)
                current-close   (::candle/close current-candle)
                current-high    (::candle/high current-candle)
                current-low     (::candle/low current-candle)]
       (cond
         (and (not previous-sar) (not previous-close))
         nil

         (and (not previous-sar) previous-close)
         (let [current-trend (> current-close previous-close)
               sar           (if current-trend
                               current-low
                               current-high)]

           {:current-trend       current-trend
            :sar                 sar
            :acceleration-factor start
            :extreme-point       sar})

         :else
         (let [{:keys [current-trend extreme-point acceleration-factor sar]} previous-sar]
           (if current-trend
              ;; uptrend
             (let [current-extreme-point       (max extreme-point current-high)
                   current-acceleration-factor (cond-> acceleration-factor
                                                 (> current-extreme-point extreme-point) (-> (+ increment)
                                                                                             (min max-value)))
                   new-sar                     (+ sar
                                                  (* current-acceleration-factor (- current-extreme-point sar)))

                   uptrend                     (<= new-sar current-low)]

               (if (not uptrend)

                 {:current-trend       false
                  :sar                 extreme-point
                  :acceleration-factor start
                  :extreme-point       current-low}

                  ;; Continue the trend
                 {:current-trend       true
                  :sar                 new-sar
                  :extreme-point       current-extreme-point
                  :acceleration-factor current-acceleration-factor}))


              ;; downtrend
             (let [current-extreme-point       (min extreme-point current-low)
                   current-acceleration-factor (cond-> acceleration-factor
                                                 (< current-extreme-point extreme-point) (-> (+ increment)
                                                                                             (min max-value)))
                   new-sar                     (- sar
                                                  (* current-acceleration-factor (- sar current-extreme-point)))

                   downtrend                   (>= new-sar current-high)]

               (if (not downtrend)
                 {:current-trend       true
                  :sar                 extreme-point
                  :acceleration-factor start
                  :extreme-point       current-high}

                  ;; Continue the trend
                 {:current-trend       false
                  :sar                 new-sar
                  :extreme-point       current-extreme-point
                  :acceleration-factor current-acceleration-factor})))))))))

(defn parabolic-sar
  [tl candleseries start increment max-value]
  (map-val tl (internal-parabolic-sar tl candleseries start increment max-value) :sar))

; Oscillators
(defn momentum
  [tl series period]
  (timeseries
   tl
   {:series series}
   (fn momentum* [_ {:keys [series]} k]
     (when-let [curr (get series k)
                prev (sort/offset-val series k (- period))]
       (- curr prev)))))

(defn rate-of-change
  [tl series period]
  (timeseries
   tl
   {:series series}
   (fn rate-of-change* [_ {:keys [series]} k]
     (when-let [curr (get series k)
                prev (sort/offset-val series k (- period))]
       (* 100 (/ (- curr prev) prev))))))

(defn- stochastic-kline
  [tl candleseries period]
  (timeseries
   tl
   {:candleseries candleseries}
   (fn stochastic-kline* [_ {:keys [candleseries]} k]
     (when-let [tail-candles (sort/full-tail-vals candleseries k period)
                tail-high    (mapv ::candle/high tail-candles)
                tail-low     (mapv ::candle/low tail-candles)
                curr-candle  (get candleseries k)
                curr-close   (::candle/close curr-candle)
                lowest       (reduce min tail-low)
                highest      (reduce max tail-high)]
       (* 100
          (/ (- curr-close lowest)
             (- highest lowest)))))))

(defn stochastic-oscillator
  "https://www.tradingview.com/support/solutions/43000502332-stochastic-stoch/
   Each timeline value contains keys :k-line and :d-line"
  [tl candleseries period d smooth-k]
  (let [k-line-base (stochastic-kline tl candleseries period)
        k-line      (simple-moving-average tl k-line-base smooth-k)
        d-line      (simple-moving-average tl k-line d)]
    (timeseries
     tl
     {:k-line k-line
      :d-line d-line}
     (fn stochastic-oscillator* [_ {:keys [k-line d-line]} k]
       (when-let [k-line* (get k-line k)
                  d-line* (get d-line k)]
         {:k-line k-line*
          :d-line d-line*})))))

(defn relative-strength-index
  [tl series period]
  (let [gains-avg (relative-moving-average tl (gain-indicator tl series) period)
        losses-avg (relative-moving-average tl (loss-indicator tl series) period)]
    (timeseries
     tl
     {:gains-avg gains-avg
      :losses-avg losses-avg}
     (fn [_ {:keys [gains-avg losses-avg]} k]
       (when-let [current-gains-avg  (get gains-avg k)
                  current-losses-avg (get losses-avg k)
                  rs                 (/ current-gains-avg current-losses-avg)]
         (- 100 (/ 100 (+ 1 rs))))))))

(def rsi "Alias for relative-strength-index" relative-strength-index)

(defn commodity-channel-index
  "Although typical price is specified as (high + low + close)/3, then TradingView
  lets you choose which value to use (close, high, hlc3, ...). Replicating this behaviour,
  it accepts a source of numbers instead of candles. To use the typical price as specified
  by the original formula, use hlc3 as series."
  [tl series period]
  (let [sma-price (simple-moving-average tl series period)
        mean-dev  (mean-deviation tl series period)]
    (timeseries
     tl
     {:series series
      :sma-price sma-price
      :mean-dev mean-dev}
     (fn commodity-channel-index* [curr {:keys [mean-dev sma-price series]} k]
       (when-let [price    (get series k)
                  sma      (get sma-price k)
                  mean-dev (get mean-dev k)
                  cci-val  (/ (- price sma)
                              (* 0.015 mean-dev))]
         (if (Double/isNaN cci-val)
           (sort/offset-val curr k -1)
           cci-val))))))

(defn- relative-volatility-u-s
  [tl series period]
  (timeseries
   tl
   {:series series
    :stdev (standard-deviation tl series period)}
   (fn relative-volatility-u-s* [_ {:keys [series stdev]} k]
     (when-let [curr-stdev (get stdev k)
                prev-series (sort/offset-val series k -1)
                curr-series (get series k)]
       (if (> curr-series prev-series)
         {:u curr-stdev :s 0}
         {:u 0 :s curr-stdev})))))

(defn relative-volatility-index
  "EMA period is 14 in the original formula, but can be changed with ema-period"
  [tl series period ema-period]
  (let [base  (relative-volatility-u-s tl series period)
        u     (map-val tl base :u)
        s     (map-val tl base :s)
        ema-u (exponential-moving-average tl u ema-period)
        ema-s (exponential-moving-average tl s ema-period)]
    (timeseries
     tl
     {:ema-u ema-u
      :ema-s ema-s}
     (fn relative-volatility-index* [_ {:keys [ema-u ema-s]} k]
       (when-let [-ema-u (get ema-u k)
                  -ema-s (get ema-s k)]
         (* 100 (/ -ema-u (+ -ema-s -ema-u))))))))

(defn refined-relative-volatility-index
  "EMA period is 14 in the original formula, but can be changed with ema-period"
  [tl candleseries period ema-period]
  (let [low      (low tl candleseries)
        high     (high tl candleseries)
        high-rvi (relative-volatility-index tl high period ema-period)
        low-rvi  (relative-volatility-index tl low period ema-period)]
    (timeseries
     tl
     {:high-rvi high-rvi
      :low-rvi low-rvi}
     (fn refined-relative-volatility-index* [_ {:keys [high-rvi low-rvi]} k]
       (when-let [high (get high-rvi k)
                  low  (get low-rvi k)]
         (/ (+ high low) 2))))))

; Range indicators

(defn range-indicator
  [tl candleseries]
  (timeseries
   tl
   {:candleseries candleseries}
   (fn range-indicator* [_ {:keys [candleseries]} k]
     (when-let [candle (get candleseries k)
                high   (::candle/high candle)
                low    (:low candle)]
       (- high low)))))

(defn true-range
  [tl candleseries]
  (timeseries
   tl
   {:candleseries candleseries}
   (fn [_ {:keys [candleseries]} k]
     (when-let [current       (get candleseries k)
                previous      (sort/offset-val candleseries k -1)
                curr-high     (::candle/high current)
                curr-low      (::candle/low current)
                prev-val      (get previous ::candle/close)
                high-low-diff (- curr-high curr-low)
                high-close    (abs (- curr-high prev-val))
                low-close     (abs (- curr-low prev-val))]
       (max high-low-diff high-close low-close)))))

(defn average-range
  "Smoothing method any moving average indicator"
  [tl candleseries period smoothing-indicator]
  (smoothing-indicator tl (range-indicator tl candleseries) period))

(defn average-true-range
  "Smoothing indicator any average indicator"
  [tl candleseries period smoothing-indicator]
  (smoothing-indicator tl (true-range tl candleseries) period))

; Price channels

(defn bollinger-bands
  "Bollinger bands, returns timeline with values as maps of :upper, :lower, :middle"
  [tl series period multiplier]
  (timeseries
   tl
   {:series series 
    :internal-sma (simple-moving-average tl series period)}
   (fn bollinger-bands* [_ {:keys [series internal-sma]} k]
     (when-let [vals (sort/full-tail-vals series k period)
                middle-band (get internal-sma k)
                stdev (-standard-deviation vals)
                offset (* stdev multiplier)]
       {:upper (+ middle-band offset)
        :lower (+ middle-band offset)
        :middle middle-band}))))

(defn keltner-channels
  "TradingView's Keltner channels ATR use EMA for its calculation.
   
   Returns indicator with timeline vals as maps of :middle, :upper, :lower"
  [tl ma-indicator range-indicator multiplier]
  (timeseries 
   tl
   {:ma ma-indicator
    :-range range-indicator}
   (fn keltner-channels* [_ {:keys [ma -range]} k]
     (when-let [curr-ma    (get ma k)
                curr-rng   (get -range k)]
       {:middle curr-ma
        :upper  (+ curr-ma (* multiplier curr-rng))
        :lower  (- curr-ma (* multiplier curr-rng))}))))
