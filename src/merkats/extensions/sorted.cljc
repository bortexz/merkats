(ns merkats.extensions.sorted
  "Extensions for sorted collections using data.avl and tailored for the performance of candleseries indicators.
   Many avl functions are aliased here to depend only on this namespace, and not use avl directly."
  (:refer-clojure :exclude [sorted-map sorted-map-by sorted-set sorted-set-by first last when-let if-let])
  (:require [clojure.data.avl :as avl]
            [better-cond.core :refer [when-let if-let]]
            [clojure.core.protocols :as p]))

(defn- nil-f
  [f]
  (fn [x] (some-> x f)))

(def ^:private some-key (nil-f key))
(def ^:private some-val (nil-f val))

(defn sorted-map
  "Create data.avl sorted map"
  [& args]
  (apply avl/sorted-map args))

(defn sorted-map-by
  "Create data.avl sorted map with comparator `f`"
  [f & args]
  (apply avl/sorted-map-by f args))

(defn index-by
  "Indexes a collection using function `f`, and put the results into a sorted-map"
  [f coll]
  (into (sorted-map) (map (juxt f identity)) coll))

(defn nth-key
  "nil-safe (key (nth sc idx))."
  [sc idx]
  (key (nth sc idx)))

(defn nth-val
  "nil-safe (val (nth sc idx))"
  [sc idx]
  (val (nth sc idx)))

(defn first
  "Like clojure.core/first, but faster for sorted collections."
  [sc]
  (nth sc 0 nil))

(defn first-key
  "nil-safe (key (first sc))"
  [sc]
  (some-key (first sc)))

(defn first-val
  "nil-safe (val (first sc))"
  [sc]
  (some-val (first sc)))

(defn last
  "Like clojure.core/last, but faster"
  [sc]
  (when (not (zero? (count sc)))
    (nth sc (dec (count sc)) nil)))

(defn last-key
  "nil-safe (key (last sc))"
  [sc]
  (some-key (last sc)))

(defn last-val
  "nil-safe (val (last sc))"
  [sc]
  (some-val (last sc)))

(defn rank-of
  "like avl/rank-of, but returns nil instead of -1 when item not found"
  [sc k]
  (when-let [r (avl/rank-of sc k)
             _exists? (>= r 0)]
    r))

(def nearest avl/nearest)

(defn nearest-key
  "nil-safe (key (nearest ...))"
  [sc test k]
  (some-key (nearest sc test k)))

(defn nearest-val
  "nil-safe (val (nearest ...))"
  [sc test k]
  (some-val (nearest sc test k)))

(defn nearest-rank
  "Like (rank-of sc (nearest-key sc test k))"
  [sc test k]
  (when-let [near-key (nearest-key sc test k)]
    (avl/rank-of sc near-key)))

(defn offset
  "Takes the item at `n` items from key `k`.
   e.g. (offset sc k -1) takes the previous item to `k`
   e.g. (offset sc k 1) takes the next item to `k`.
   `k` must exist in `sc`, otherwise returns nil"
  [sc k n]
  (when-let [k-rank (avl/rank-of sc k)]
    (nth sc (+ k-rank n) nil)))

(defn offset-key
  "nil-safe (key (offset sc k n))"
  [sc k n]
  (some-key (offset sc k n)))

(defn offset-val
  "nil-safe (val (offset sc k n))"
  [sc k n]
  (some-val (offset sc k n)))

(defn -tail-vec-fn
  "Returns a [[tail]] function where items are mapped using `f` when building the resulting vector.
   Resulting fn is faster than (mapv f (tail sc n)).
   See [[tail]]."
  [f]
  (fn tail-vec
    ([sc n]
     (when-let [l (last-key sc)]
       (tail-vec sc l n)))
    ([sc ending-k n]
     (when-let [end (rank-of sc ending-k)
                start (max 0 (- end (dec n)))]
       (persistent!
        (reduce (fn [v k] (conj! v (f (nth sc k))))
                (transient [])
                (range start (inc end))))))))

(def tail
  "Returns a vector of the last up to `n` items of `sc` until `ending-k`.
   If `ending-k` is not specified, last-key will be used.
   `ending-k` must exist if it's specified, otherwise returns nil."
  (-tail-vec-fn identity))

(def tail-keys
  "faster (mapv key (tail ...))"
  (-tail-vec-fn key))

(def tail-vals
  "faster (mapv val (tail ...))"
  (-tail-vec-fn val))

(defn tail-sorted
  "Like [[tail]] but returns an avl sorted-map. 
   Usually faster than (into (sorted-map) (tail ...)),
   except when the resulting collection is very small ~(< 10 items)"
  ([sc n]
   (when (not (zero? (count sc)))
     (tail-sorted sc (last-key sc) n)))
  ([sc ending-k n]
   (when-let [ending-rank (rank-of sc ending-k)
              starting-rank (max (- ending-rank (dec n)) 0)
              starting-k (nth-key sc starting-rank)]
     (avl/subrange sc >= starting-k <= ending-k))))

(defn full-tail-vals
  "Like `sorted/tail-vals` but only return non-nil when the resulting vec has n non-nil items, nil otherwise."
  [sc ending-k n]
  (when-let [end   (avl/rank-of sc ending-k)
             start (- end (dec n))
             _     (nat-int? start)]
    (some-> (reduce (fn [v k]
                      (if-let [x (val (nth sc k))]
                        (conj! v x)
                        (reduced nil)))
                    (transient [])
                    (range start (inc end)))
            (persistent!))))

(extend-type clojure.data.avl.AVLMap
  p/Datafiable
  (datafy [this] (into (clojure.core/sorted-map-by (.comparator this)) this)))
