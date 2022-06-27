(ns merkats.eventflow.nodes
  (:require [merkats.eventflow :as ef]
            [merkats.eventflow.async :as efa]
            [clojure.core.async :as a]))

(defn transducer
  ([xf] (transducer xf 16))
  ([xf buf-or-n]
   (let [newxf (comp xf (map (fn [v] [:output v])))]
     (reify
       ef/Node
       (process [_ _ event]
         (transduce newxf conj [event]))

       efa/Node
       (initialize [_]
         (let [ch (a/chan buf-or-n xf)]
           (efa/new-process {:input ch} {:output ch} (fn [] (a/close! ch)))))))))
