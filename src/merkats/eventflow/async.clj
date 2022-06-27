(ns merkats.eventflow.async
  (:require [merkats.eventflow :as ef]
            [clojure.core.async :as a]
            [merkats.extensions.async :as ea]
            [merkats.extensions.core :refer [while-let]]))

(defprotocol IProcess :extend-via-metadata true
  (-input-chs [this] "Returns a map of {<input> <ch>}")
  (-output-chs [this] "Returns a map of {<output> <ch>}")
  (-shutdown [this] "Closes all internal chs and processes. Blocking."))

(defprotocol Node :extend-via-metadata true
  "Protocol to specify async nodes where inputs and outputs are core.async chs, and a setup is created
   to connect inputs to outputs.
   When working with async/Node, inputs and outputs need to be known in advance, to create channels for them."
  (initialize [this]
    "Returns an instance of `IProcess`.
     
     Async Pipelines only need nodes to implement eventflow.async/Node, but not eventflow/Node,
     although making nodes implement eventflow/Node allows for better testability and to reuse nodes
     both in eventflow.async/Pipeline and other pipelines. In some scenarios, connecting inputs to outputs
     directly without calling process might be desired (e.g a node that transduces values 
     from 1 input to 1 output might have an async ch that applies the xf directly on the input-ch and 
     pipes to output-ch without calling process, althoguh can still implement process to reuse the node
     on non-async pipelines)."))

(defprotocol Pipeline
  "Marker for pipelines that run asynchornously. Async pipelines accept nodes implementing AsyncNode")

(defn pipeline
  "Async pipeline
   - Links are implemented as core.async mult/tap, where each node's output has a mult and linked node 
   inputs tap into it.
   - Ingest uses >!! in Clojure and put! in cljs.
   - (Clojure) Thread-safety: 
     - nodes and links can be added and removed anytime from any thread.
     - ingest is susceptible to put to a closed ch if node was removed from another thread at the same time."
  []
  (let [state_      (atom {:nodes {} ; {<id> (delay {::process ... ::node ... ::output-mults ...})}
                           :links #{} ; #{[<from-id> <from-output> <to-input> <to-id>]}
                           :fx nil ; Delay of effects to run for setting up async connections
                           })

        get-node-ps (fn [state id] (get (:nodes state) id))
        get-link    (fn [state fid fout tinp tid] (get (:links state) [fid fout tinp tid]))
        get-ich     (fn [state id input]
                      (-> (get-node-ps state id)
                          (force)
                          ::process
                          (-input-chs)
                          (get input)))
        get-omult   (fn [state id output]
                      (-> (get-node-ps state id)
                          (force)
                          ::output-mults
                          (get output)))
        run-fx      (fn [state] (force (:fx state)))]
    (reify
      ef/Pipeline
      (ingest [this id input event]
        (let [state @state_
              node (get-node-ps state id)
              ich (some-> node force ::input-chs (get input))]
          (assert node "Specified node does not exist")
          (assert ich "Specified input ch does not exist")
          (a/>!! ich event)
          this))

      (add-node [this id node]
        (let [[o n]
              (swap-vals!
               state_
               (fn [state]
                 (if (get-node-ps state id)
                   state
                   (let [node-state
                         (delay
                          (run-fx state)
                          (let [ps (initialize node)
                                output-mults (update-vals (-output-chs ps) (fn [och] (a/mult och)))]
                            {::node node
                             ::output-mults output-mults
                             ::process ps}))]
                     (-> state
                         (update :nodes assoc id node-state)
                         (assoc :fx node-state))))))]
          (assert (not (get-node-ps o id)) "A node already exists under given `id`")
          (run-fx n)
          this))

      (remove-node [this id]
        (let [[o n]
              (swap-vals!
               state_
               (fn [{:keys [nodes links] :as state}]
                 (if-let [existing (get nodes id)]
                   (let [new-links (->> links
                                        (remove (fn [[fid _ _ tid]]
                                                  (or (= fid id) (= tid id))))
                                        (into #{}))
                         new-nodes (dissoc nodes id)
                         new-fx (delay
                                 (run-fx state)
                                 (-shutdown (::setup existing)))]
                     (assoc state
                            :nodes new-nodes
                            :links new-links
                            :fx new-fx))
                   state)))]
          (assert (get-node-ps o id) "A node with given `id` does not exist")
          (run-fx n)
          this))

      (add-link [this from-id from-output to-input to-id]
        (let [[o n]
              (swap-vals!
               state_
               (fn [{:keys [links] :as state}]
                 (if (or (get-link state from-id from-output to-input to-id)
                         (not (get-node-ps state from-id))
                         (not (get-node-ps state to-id)))
                   state
                   (let [new-links (conj links [from-id from-output to-input to-id])
                         new-fx (delay
                                 (run-fx state)
                                 (let [omult (get-omult state from-id from-output)
                                       ich (get-ich state to-id to-input)]
                                   (a/tap omult ich false)))]
                     (assoc state
                            :links new-links
                            :fx new-fx)))))]
          (assert (not (get-link o from-id from-output to-input to-id)) "Given link already exists")
          (assert (get-node-ps o from-id) "From-id node does not exist")
          (assert (get-node-ps o to-id) "to-id node does not exist")
          (run-fx n)
          this))

      (remove-link [this from-id from-output to-input to-id]
        (let [[o n]
              (swap-vals!
               state_
               (fn [{:keys [links] :as state}]
                 (if-let [link (get-link state from-id from-output to-input to-id)]
                   (let [new-links (disj links link)
                         new-fx (delay
                                 (run-fx state)
                                 (let [omult (get-omult state from-id from-output)
                                       ich (get-ich state to-id to-input)]
                                   (a/untap omult ich)))]
                     (assoc state
                            :links new-links
                            :fx new-fx))
                   state)))]
          (assert (get-link o from-id from-output to-input to-id)
                  "Given link does not exist")
          (run-fx n)
          this))

      (nodes [_] (update-vals (:nodes @state_) (comp :node force)))
      (links [_] (:links @state_)))))

(defn new-process
  "Creates a new `IProcess` (return type of `initialize`) from given input-chs, output-chs and shutdownf (nullary f)"
  [input-chs output-chs shutdown-f]
  (reify
    IProcess
    (-input-chs [_] input-chs)
    (-output-chs [_] output-chs)
    (-shutdown [_] (shutdown-f))))

(defn alts-process
  ([node input-chs output-chs]
   (alts-process node input-chs output-chs {}))
  ([node input-chs output-chs {:keys [thread?] :or {thread? false}}]
   (let [{ichk :->ch ichv :as-vec ichm :as-map iclose :close} (ea/chs-map input-chs)
         {och :->key ochm :as-map oclose :close} (ea/chs-map output-chs)
         ps (if thread?
              (a/thread
               (while-let [[event port] (a/alts!! ichv)]
                 (doseq [[o ev] (ef/process node (ichk port) event)]
                   (a/>!! (och o) ev))))
              
              (a/go
                (while-let [[event port] (a/alts! ichv)]
                  (doseq [[o ev] (ef/process node (ichk port) event)]
                    (a/>! (och o) ev)))))
         
         td (fn [] (iclose) (a/<!! ps) (oclose))]
     (new-process ichm ochm td))))

(defn parallel-process
  ([node input-chs output-chs]
   (parallel-process node input-chs output-chs {}))
  ([node input-chs output-chs {:keys [thread?-f] :or {thread?-f (constantly false)}}]
   (let [pschs (mapv 
                (fn [[i ch]]
                  (if (thread?-f i)
                    (a/thread
                      (while-let [v (a/<!! ch)]
                        (doseq [[o ev] (ef/process node i v)] (a/>!! (output-chs o) ev))))
                    
                    (a/go
                      (while-let [v (a/<! ch)]
                        (doseq [[o ev] (ef/process node i v)] (a/>! (output-chs o) ev))))))
                input-chs)
         shutdown (fn []
                    (run! a/close! (mapv second input-chs))
                    (run! a/<!! pschs)
                    (run! a/close! (mapv second output-chs)))]
     (new-process input-chs output-chs shutdown))))