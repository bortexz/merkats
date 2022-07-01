(ns merkats.extensions.core
  "Extensions to clojure core"
  (:refer-clojure :exclude [when-let if-let get-in])
  (:require [better-cond.core :refer [when-let if-let]]))

(defmacro while-let
  [bindings & body]
  `(loop []
     (when-let ~bindings
       ~@body
       (recur))))

(defmacro assert-val
  [exp & [msg]]
  `(let [v# ~exp]
     (assert v# ~msg)
     v#))

(def ascending-comparator  compare)
(def descending-comparator (fn [a b] (compare b a)))

(defn get-in [m ks] (reduce get m ks))

(defn move
  [m from to]
  (if-some [v (get m from)]
    (assoc m to v)
    m))

(defn move-in
  [m from-path to-path]
  (if-some [v (get-in m from-path)]
    (assoc-in m to-path v)
    m))

(defn filter-vals
  [m pred]
  (persistent!
   (reduce-kv (fn [acc k v]
                (cond-> acc
                  (not (pred v)) (dissoc! k)))
              (transient m)
              m)))

(defn queue
  "Create a queue, empty or from given `coll`."
  ([]
   ;#?(:clj clojure.lang.PersistentQueue/EMPTY
   ;   :cljs cljs.core/PersistentQueue.EMPTY))
   clojure.lang.PersistentQueue/EMPTY)
  ([coll]
   (reduce conj (queue) coll)))

(defn queue?
  [x]
  (instance? clojure.lang.PersistentQueue x))

(defn sorted-by?
  "Returns true if `coll` of maps is sorted by (f item) using `comparator`.
   If `comparator` not specified, clojure's default one will be used."
  ([f coll]
   (sorted-by? compare f coll))
  ([comparator f coll]
   (empty? (->> coll
                (map f)
                (partition 2 1)
                (map (fn [[a b]] (comparator a b)))
                (filter #(pos? %))))))

(defn index-by
  [f coll]
  (into {} (map (juxt f identity)) coll))

(defn pull!
  ([a_]
   (first (swap-vals! a_ empty)))
  ([a_ & path]
   (let [[old _] (swap-vals! a_ (fn [v] (update-in v path empty)))]
     (get-in old path))))

(defn merge-meta
  [obj m]
  (with-meta obj ((fnil merge {}) (meta obj) m)))

(defn current-millis [] (System/currentTimeMillis))

;; Events

(defn map-tuple
  "Given arbitrary number of args, returns a n-ary maping fn that maps to a tuple as if (into args vs).
   
   E.g Mapping to transform data into events like [topic data]: (map-tuple :my-topic)"
  [& args]
  (map (fn map*
         ([v1] (conj args v1))
         ([v1 v2] (conj args v1 v2))
         ([v1 v2 v3] (conj args v1 v2 v3))
         ([v1 v2 v3 & v] (into (map* v1 v2 v3) v)))))
