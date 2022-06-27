(ns merkats.extensions.core
  "Extensions to clojure core"
  (:refer-clojure :exclude [when-let if-let get-in])
  (:require [better-cond.core :refer [when-let if-let]])
  (:import (java.io InputStream)
           (java.util.zip GZIPInputStream)))

; --------------------------------------------------------------------------------------------------
; Macro

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

; Macro
; --------------------------------------------------------------------------------------------------

; --------------------------------------------------------------------------------------------------
; Java IO

(defn gzip-input-stream
  "Create java.util.zip.GZIPInputStream from input-stream `is`"
  [^InputStream i]
  (GZIPInputStream. i))

; Java IO
; --------------------------------------------------------------------------------------------------

; --------------------------------------------------------------------------------------------------
; Comparators

(def ascending-comparator  compare)
(def descending-comparator (fn [a b] (compare b a)))

; Comparators
; --------------------------------------------------------------------------------------------------

; --------------------------------------------------------------------------------------------------
; Maps

(defn get-in [m ks] (reduce get m ks))

(defn move
  [m from to]
  (if-some [v (get-in m from)]
    (assoc-in m to v)
    m))

(defn filter-vals
  [m pred]
  (persistent!
   (reduce-kv (fn [acc k v]
                (cond-> acc
                  (not (pred v)) (dissoc! k)))
              (transient m)
              m)))

; Maps
; --------------------------------------------------------------------------------------------------

; --------------------------------------------------------------------------------------------------
; Collections

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

; Collections
; --------------------------------------------------------------------------------------------------

; --------------------------------------------------------------------------------------------------
; Atoms

(defn pull!
  ([a_]
   (first (swap-vals! a_ empty)))
  ([a_ path]
   (let [[old _] (swap-vals! a_ (fn [v] (update-in v path empty)))]
     (get-in old path))))

; Atoms
; --------------------------------------------------------------------------------------------------

; --------------------------------------------------------------------------------------------------
; Misc

(defn add-meta
  [obj m]
  (with-meta obj ((fnil merge {}) (meta obj) m)))

(defn random-str-uuid
  []
  (str (random-uuid)))

(defn current-millis [] (System/currentTimeMillis))

; Misc
; --------------------------------------------------------------------------------------------------
