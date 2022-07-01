(ns merkats.extensions.async
  (:require [clojure.core.async :as a]
            [clojure.set :as set]
            [merkats.extensions.core :refer [while-let]])
  (:import (clojure.core.async.impl.channels ManyToManyChannel)))

(defn chan? [x] (instance? ManyToManyChannel x))

(defn chs-map
  "`m` is a map of keywords to async channels, and returns a map with:
   - `ch->key` returns key given a ch
   - `key->ch` returns ch given a key
   - `as-vec` vector of all chs (for use in alts!, etc...)
   - `as-map` the original map
   
   Saves a bit of boilerplate when dealing with `alts!` when having maps {<key> <ch>}"
  [m]
  (let [im (set/map-invert m)
        chs-vec (mapv second m)]
    {:->key (fn [ch] (get im ch))
     :->ch (fn [k] (get m k))
     :as-vec chs-vec
     :as-map m
     :close (fn [] (run! a/close! chs-vec))}))

(defn interval
  "Given a number of milliseconds `ms` and output chan `out`, starts a process that will put a logical
   true value every x `ms` on out chan. Returns a ch that can be closed to stop the process.
   If close? is true, the out chan will be closed when the process is stopped."
  ([ms out] (interval ms out true))
  ([ms out close?]
   (let [stop-ch (a/chan)]
     (a/go-loop [timer (a/timeout ms)]
       (a/alt!
         [stop-ch timer]
         ([_ port]
          (condp = port
            timer (do (a/>! out true) (recur (a/timeout ms)))
            stop-ch (when close? (a/close! out))))))
     stop-ch)))

(defn sample
  "Takes an input-ch and a sampler-ch, and creates a process that keeps taking values from input-ch,
   and puts the latest one received on output ch whenever a value is taken from sampler-ch.
   Doesn't put anything into output ch if a value is taken from sampler-ch without any new values from input-ch."
  ([input sampler out]
   (sample input sampler out true))
  ([input sampler out close?]
   (let [stop-ch (a/chan)
         latest_ (atom nil)]
     (a/go-loop []
       (a/alt!
         [input sampler stop-ch]
         ([val port]
          (condp = port
            input (do (reset! latest_ val)
                      (recur))
            sampler (let [[o _] (reset! latest_ nil)]
                      (when o (a/>! out o))
                      (recur))
            stop-ch (when close? (a/close! out))))))
     stop-ch)))

(defn sample-interval
  ([ms inp out] (sample-interval ms inp out true))
  ([ms inp out close?]
   (let [sampler-ch (a/chan)
         stop-sampler (interval ms sampler-ch)
         stop-sample (sample inp sampler-ch out close?)]
     (a/pipe stop-sample stop-sampler)
     stop-sample)))

(defn window
  ([input sampler out] (window input sampler out))
  ([input sampler out close?]
   (let [stop-ch (a/chan)
         vals_ (atom [])]
     (a/go-loop []
       (a/alt!
         [input sampler stop-ch]
         ([val port]
          (condp = port
            input (do (swap! vals_ conj val)
                      (recur))
            sampler (let [[o _] (swap! vals_ empty)]
                      (when (seq o) (a/>! out o))
                      (recur))
            stop-ch (when close? (a/close! out))))))
     stop-ch)))

(defn window-n
  ([n input out] (window-n n input out true))
  ([n input out close?]
   (let [stop-ch (a/chan)
         vals_ (atom [])]
     (a/go-loop []
       (a/alt!
         [input stop-ch]
         ([val port]
          (condp = port
            input (let [xs (swap! vals_ conj val)]
                    (when (>= (count xs) n) 
                      (a/>! out xs)
                      (swap! vals_ empty))
                    (recur))
            stop-ch (when close? (a/close! out))))))
     stop-ch)))

(defn window-interval
  ([ms inp out] (window-interval ms inp out true))
  ([ms inp out close?]
   (let [sampler-ch (a/chan)
         stop-sampler (interval ms sampler-ch)
         stop-window (window inp sampler-ch out close?)]
     (a/pipe stop-window stop-sampler)
     stop-window)))

