(ns merkats.stream
  (:require [clojure.core.async.impl.protocols :as p]))

(defprotocol IStream
  (close! [this] "Closes given stream")
  (ch [this] "Returns channel for this stream"))

(defrecord Stream [ch
                   on-close
                   closed?_]
  IStream
  (ch [_] ch)
  (close! [_]
    (let [[prev _] (reset-vals! closed?_ true)]
      (when-not prev
        (try
          (on-close)
          (finally (p/close! ch))))))
  
  p/ReadPort
  (take! [_ fnh] (p/take! ch fnh)))

(defn create
  "Creates a stream given a ch and nullary fn on-close, that will be called
   once when the Stream is closed to stop producing to the internal ch. The ch will be closed
   automatically after on-close executed.
   Experimental: The resulting Stream also implements core.async/ReadPort so you can
   consume the stream directly as you would a chan (take!/<!/<!!/...)."
  [ch on-close]
  (->Stream ch on-close (atom false)))
 