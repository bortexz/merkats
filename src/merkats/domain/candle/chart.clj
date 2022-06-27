(ns merkats.domain.candle.chart
  (:require [merkats.extensions.spec :as es]
            [clojure.alpha.spec :as s]
            [merkats.domain.market :as market]
            [merkats.domain.time.interval :as interval]
            [merkats.domain.candle :as candle]
            [merkats.extensions.sorted :as sort]
            [merkats.extensions.core :refer [ascending-comparator]]
            [merkats.domain.time :as time]))

(es/def ::series
  "Sorted map of candles indexed on interval/from, representing a timeline of candles."
  (s/map-of ::interval/from ::candle/candle :kind? sorted?))

(es/def ::delta
  "When there are updates to the chart, delta represents only the new added candles as a vector."
  ::candle/candles)

(es/def ::max-candles
  "Maximum number of candles to keep in memory on chart"
  pos-int?)

(es/def ::chart (s/schema [::market/symbol
                           ::time/frame
                           ::max-candles
                           ::series]))

(es/def ::update (s/union ::chart [::delta]))

(defn series
  "Returns empty ::chart/series, or from given coll of candles"
  ([]
   (sort/sorted-map-by ascending-comparator))
  ([candles]
   (sort/index-by ::interval/from candles)))

(defn join-series
  "Joins series `chs2` into `chs1`, merging on interval/from using `candle/join`."
  [chs1 chs2]
  (merge-with candle/join chs1 chs2))

(defn apply-max-candles
  "Removes previous candles to max-candles, if needed"
  [{::keys [max-candles] :as chart}]
  (update chart ::series (fn [sc]
                           (cond-> sc
                             (> (count sc) max-candles) (sort/tail-sorted max-candles)))))

(defn ingest-delta
  "Ingests given delta into chart, returns a new ::update."
  [ch d]
  (-> ch
      (update ::series merge (series d))
      (apply-max-candles)
      (assoc ::delta d)))

(defn- ordered-candle-trades
  "Returns delta from the given trades. Trades must be sorted by time/stamp.
   Optimized for the case when all trades are in the same candle (by only checking candle of 
   first and last trade)."
  [ts tf]
  (let [tsv (vec ts)
        first-itv (interval/interval {::time/frame tf ::time/stamp (::time/stamp (first tsv))})
        last-itv (interval/interval {::time/frame tf ::time/stamp (::time/stamp (peek tsv))})
        all-same-interval? (= first-itv last-itv)]
    (if all-same-interval?
      [[first-itv ts]]
      (let [xf (comp
                (map (fn [t] [(interval/interval {::time/frame tf ::time/stamp (::time/stamp t)}) t]))
                (partition-by first)
                (map (fn [itv-ts] [(ffirst itv-ts) (map second itv-ts)])))]
        (into [] xf ts)))))

(defn ingest-trades
  "Returns ::update with the given trades `ts` ingested into the chart."
  [{tf ::time/frame :as chart} ts]
  (apply-max-candles
   (reduce (fn [{chs ::series :as ch} [itv ts]]
             (let [from (::interval/from itv)
                   uc (candle/ingest-trades (get chs from itv) ts)]
               (-> ch
                   (update ::series assoc from uc)
                   (update ::delta conj uc))))
           (assoc chart ::delta [])
           (ordered-candle-trades ts tf))))

(defn delta-xf
  "Transducer that takes collection of trades as items, and propagates delta of candles updated.
   Trades must be sorted by timestamp. 
   Only candle from latest trades is kept in memory."
  [{tf ::time/frame}]
  (fn [rf]
    (let [chart_ (atom {::time/frame tf
                        ::series (series)})]
      (fn
        ([] (rf))
        ([r] (rf r))
        ([r ts]
         (let [{d ::delta} (ingest-trades @chart_ ts)]
           (vswap! chart_ assoc ::series (series d))
           (rf r d)))))))

(defn ->candles
  "Returns vector of candles from given chart series"
  [chart]
  (mapv val (::series chart)))

(defn ingest-series
  [chart s]
  (ingest-delta chart (mapv val s)))
