(ns merkats.domain.time.interval
  (:require [merkats.extensions.spec :as es]
            [clojure.alpha.spec :as s]
            [merkats.domain.time :as time]
            [cljc.java-time.instant :as t.i]
            [cljc.java-time.duration :as t.d]
            [tick.core :as t]))

(es/def ::from
  "beggining of time interval, inclusive."
  t/instant?)

(es/def ::to
  "Ending instant of candle, exclusive."
  t/instant?)

(es/def ::interval
  (s/schema [::from ::to ::time/frame]))

;; Use here, somehow, a open-fn?

(defn add-from
  "Adds ::from to interval having ::time/frame and ::to"
  [{::keys [to] tf ::time/fame :as candle}]
  (assoc candle ::from (t.i/minus to tf)))

(defn add-to
  "Adds ::to to interval having ::time/frame and ::from"
  [{::keys [from] tf ::time/frame :as candle}]
  (assoc candle ::to (t.i/plus from tf)))

(defn interval
  "Returns interval that contains given ::time/stamp with ::time/frame"
  [{::time/keys [stamp frame]}]
  (let [inst-millis (t.i/to-epoch-milli stamp)
        tf-millis (t.d/to-millis frame)
        remaining (rem inst-millis tf-millis)
        from (t.i/of-epoch-milli (- inst-millis remaining))]
    (add-to {::from from
             ::time/frame frame})))

(defn enclosing
  "Returns a new interval enclosing `interval` within a multiple ::time/frame of `interval`s time/frame
   
   E.g return enclosing 5m interval containing 1m interval"
  [interval new-timeframe]
  (interval {::time/frame new-timeframe ::time/stamp (::from interval)}))

(defn shift
  "Moves interval n time/frames away. (shift interval 0) returns same interval."
  [{_from ::from _to ::to timeframe ::time/frame :as interval} n]
  (let [move-by (t.d/multiplied-by timeframe n)]
    (-> interval
        (update ::from t.i/plus move-by)
        (update ::to t.i/plus move-by))))

(defn after?
  "Returns true when `ts` is after `interval`"
  [{to ::to} ts]
  (>= (compare to ts) 0))

(defn before?
  "Returns true when `ts` is before `interval`"
  [{from ::from} ts]
  (< (compare from ts) 0))

(defn inside?
  "Returns true when `ts` is inside `interval`"
  [{::keys [from to]} ts]
  (and (>= (compare ts from) 0)
       (< (compare ts to) 0)))

(defn shifts-between
  "Returns how many shifts there is between c1 and c2. 
   c1 and c2 must have same timeframe and c2 be after c1. 
   Doesn't work with timeframes smaller than millis."
  [{from1 ::from tf ::time/frame} {from2 ::from}]
  (let [d (t.d/between from1 from2)]
    (/ (t.d/to-millis d) (t.d/to-millis tf))))

(defn missing
  "Returns an ordered collection of intervals that are missing to form a complete chain of intervals.
   All candles should have the same time/frame and be ordered."
  [is]
  (->> (partition 2 1 is)
       (mapcat (fn [[i1 i2]]
                 (->> (range 1 (shifts-between i1 i2))
                      (map (partial shift i1)))))))

(comment

  ;; Intervals

  (def i (t.i/parse "2022-01-01T00:00:00.000Z"))
  (def i2 (t.i/parse "2022-01-02T00:00:00.000Z"))
  (def d (t.d/of-minutes 5))

  (add-from {::time/frame d ::to i})
  (add-to {::time/frame d ::from i})
  (interval {::time/frame d ::time/stamp i})

  (def itv (interval  {::time/frame d ::time/stamp i}))
  (def itv2 (interval {::time/frame d ::time/stamp i2}))

  (before? itv i)
  (after? itv i)
  (inside? itv i)

  (shifts-between itv itv2)

  (missing [itv itv2])
  )