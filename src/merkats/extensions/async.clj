(ns merkats.extensions.async
  (:require [clojure.core.async :as a :refer [<!! >!!]]
            [clojure.core.async.impl.protocols :as ap]
            [clojure.core.async.impl.buffers :as ab]
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
   
   Saves a bit of boilerplate when dealing with `alts!` where each port is assigned to a key in a map."
  [m]
  (let [im (set/map-invert m)
        chs-vec (mapv second m)]
    {:->key (fn [ch] (get im ch))
     :->ch (fn [k] (get m k))
     :as-vec chs-vec
     :as-map m
     :close (fn [] (run! a/close! chs-vec))}))
