(ns merkats.eventflow.flushable
  (:refer-clojure :exclude [flush])
  (:require [merkats.eventflow :as ef]))

(defprotocol Pipeline
  "A flushable pipeline holds output events until -flush is run."
  (flush [this]
    "Flushes accumulated outputs into linked inputs and runs -process on nodes.
          Not recursive, will flush data accumulated up until -flush is called.
          Returns pipeline.")
  (pending [this]
    "Returns collection of [id output event] of pending outputs to be flushed."))

(defn pipeline
  "Creates a flushable pipeline.

   Thread-safety:
   - The recommendation is to use this pipeline from a single thread! If you still want to 
     explore multi-threading, there are more notes about it below.
   
   - add/remove node/links can be used from different threads.

   - ingest reads the node from an atom first, then calls `process` on the node outside atom,
     then swap!'s the outputs into the atom, verifying that the node still exists on the pipeline.
     If node gets removed/replaced during the process execution, then no outputs of this node will be 
     added to pending outputs from the result of `process`.
   
   - flush will retrieve and empty the data inside the atom (using swap-vals!). A transformation from
     pending outputs to inputs to be ingested will be computed from the state of the atom when retrieving 
     the data, meaning links added/removed after data has been retrieved will not be used.
     Will internally call `ingest` on each computed pending inputs. If a node gets removed
     after data is retrieved, see previous note.
   "
  []
  (let [state_   (atom {:nodes {} ; {<id> <node>}
                        :links {} ; {[<from-id> <from-output>] #{[<to-id> <to-input>]}}
                        :pending [] ; coll of [id output event]
                        })
        get-node (fn [m id] (get (:nodes m) id))

        get-link (fn [m from-id from-output to-input to-id]
                   (-> (:links m)
                       (get [from-id from-output])
                       (get [to-id to-input])))]
    (reify
      ef/Pipeline
      (ingest [this id inp event]
        (let [node (get-node @state_ id)
              os   (some-> node (ef/process inp event))]
          (assert node "A node for given `id` does not exist")
          (swap! state_ (fn [state]
                          (cond-> state
                            (and (identical? (get-node state id) node) (seq os))
                            (update :pending into (map (fn [[out ev]] [id out ev])) os))))
          this))

      (add-node [this id node]
        (let [[o _] (swap-vals!
                     state_
                     (fn [state]
                       (if (get-node state id)
                         state
                         (update state :nodes assoc id node))))]
          (assert (not (get-node o id)) "A node with `id` already exist")
          this))

      (remove-node [this id]
        (let [[o _] (swap-vals!
                     state_
                     (fn [state]
                       (if-not (get-node state id)
                         state
                         (-> state
                             (update :nodes dissoc id)
                             (update :links
                                     (fn [links]
                                       (->> links
                                            (remove (fn [[[-id _output] _id-input-set]] (= -id id)))
                                            (map (fn [[id-op set-id-in]]
                                                   [id-op
                                                    (set
                                                     (remove
                                                      (fn [[-id _]] (= id -id))
                                                      set-id-in))]))
                                            (remove (comp empty? second))
                                            (into {}))))
                             (update :pending (fn [xs] (into [] (remove (comp #{id} first)) xs)))))))]
          (assert (get-node o id) "Node with given `id` does not exist")
          this))

      (add-link [this fid fout tinp tid]
        (let [[o _] (swap-vals!
                     state_
                     (fn [state]
                       (if (and (get-node state fid)
                                (get-node state tid)
                                (not (get-link state fid fout tinp tid)))
                         (update state
                                 :links
                                 update
                                 [fid fout]
                                 (fnil conj #{})
                                 [tid tinp])
                         state)))]
          (assert (get-node o fid) "From-id node does not exist")
          (assert (get-node o tid) "To-id node does not exist")
          (assert (not (get-link o fid fout tinp tid)) "Given link already exist")
          this))

      (remove-link [this fid fout tinp tid]
        (let [[o _] (swap-vals!
                     state_
                     (fn [state]
                       (update state :links (fn [links]
                                              (let [from [fid fout]
                                                    -links (update links from disj [tid tinp])]
                                                (cond-> -links
                                                  (empty? (get -links from))
                                                  (dissoc from)))))))]
          (assert (get-link o fid fout tinp tid) "Given link does not exist")
          this))

      (nodes [_]
        (:nodes @state_))

      (links [_]
        (->> (:links @state_)
             (mapcat (fn [[from to-set]]
                       (map (fn [[to-id to-input]]
                              (into from [to-input to-id]))
                            to-set)))))

      Pipeline
      (flush [this]
        (let [[o _] (swap-vals! state_ update :pending empty)
              to-ingest (mapcat
                         (fn [[id out event]]
                           (map (fn [[tid tinp]] [tid tinp event])
                                (get (:links o) [id out])))
                         (:pending o))]
          (run! (fn [[tid tinp event]] (ef/ingest this tid tinp event))
                to-ingest)
          this))

      (pending [_] (:pending @state_)))))

(defn drain
  "Flushes the given `pipeline` until there is no more pending data"
  [pipeline]
  (while (seq (pending pipeline)) (flush pipeline)))