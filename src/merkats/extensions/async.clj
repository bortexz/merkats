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

(defn sample-time
  "Similar to Rx sampleTime. Accepts input ch, ms and (optionally) buf-or-n for returning ch.
   Returns a ch that will contain the latest value received on ch for the latest `ms`.
   The sampling starts on the next value on the input ch after the previous send to the output chan,
   not when sending to output chan."
  ([ch ms]
   (sample-time ch ms nil))
  ([ch ms buf-or-n]
   (let [out-ch (a/chan buf-or-n)
         state_ (atom {:val nil
                       :timer nil})
         create-timer (fn []
                        (delay
                         (a/go
                           (when (a/<! (a/timeout ms))
                             (let [{:keys [val]} (swap! state_ assoc :timer nil)]
                               (when val (a/>! out-ch val)))))))]
     (a/go
       (while-let [v (a/<! ch)]
         (let [{:keys [timer]}
               (swap! state_ (fn [{:keys [timer] :as state}]
                               (let [timer (or timer (create-timer))]
                                 (-> state
                                     (assoc :val v)
                                     (assoc :timer timer)))))]
           @timer)))
     out-ch)))
