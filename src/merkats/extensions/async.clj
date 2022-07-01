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
   true value every x `ms` on out chan.
   Returns a ch that stops the process when closed. Can specify close? to close the out ch when stopped."
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
  "Given an input chan, sampler chan and out chan, creates a process that keeps taking values from input,
   and puts the latest one received on out whenever a value is taken from sampler.
   Returns a ch that stops the process when closed. Can specify close? to close the out ch when stopped."
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
  "Like [[sample]], using as sampler an inverval of given ms. 
   Returns a ch that stops the process when closed. Can specify close? to close the out ch when stopped."
  ([ms input out] (sample-interval ms input out true))
  ([ms input out close?]
   (let [sampler-ch (a/chan)
         stop-sampler (interval ms sampler-ch)
         stop-sample (sample input sampler-ch out close?)]
     (a/pipe stop-sample stop-sampler)
     stop-sample)))

(defn accumulate
  "Given an input chan, signal chan and output chan, creates a process that will accumulate values 
   taken from input in a vector, and whenever a value is taken from signal, it will put the vector
   of accumulated values into out, and restart the window vector. 
   Returns a ch that stops the process when closed. Can specify close? to close the out ch when stopped."
  ([input signal out] (accumulate input signal out))
  ([input signal out close?]
   (let [stop-ch (a/chan)
         vals_ (atom [])]
     (a/go-loop []
       (a/alt!
         [input signal stop-ch]
         ([val port]
          (condp = port
            input (do (swap! vals_ conj val)
                      (recur))
            signal (let [[o _] (swap! vals_ empty)]
                      (when (seq o) (a/>! out o))
                      (recur))
            stop-ch (when close? (a/close! out))))))
     stop-ch)))

(defn accumulate-interval
  "Like [[window]], using an interval of `ms` milliseconds as signal.
   Returns a ch that stops the process when closed. Can specify close? to close the out ch when stopped."
  ([ms inp out] (accumulate-interval ms inp out true))
  ([ms inp out close?]
   (let [sampler-ch (a/chan)
         stop-signal (interval ms sampler-ch)
         stop-window (accumulate inp sampler-ch out close?)]
     (a/pipe stop-window stop-signal)
     stop-window)))

