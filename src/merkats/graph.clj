(ns merkats.graph
  (:require [clojure.set :as set]
            [merkats.extensions.core :refer [queue]]))

;; TODO: Small refactor, check these when in need for input or not input
;; Allow other libs to create input/compute nodes, and only need to implement that
(defprotocol Node)
(defprotocol Input)

(defrecord InputNode [id] Node Input)

(defrecord ComputeNode [id sources handler] Node)

(defrecord Graph [labels nodes adjacency-map])

(defrecord Context [graph values compilations processor])

(defprotocol Processor
  "Compiles and processes a graph context for the given inputs."
  (-compile [this graph inputs]
    "Creates a compilation to traverse the `graph` for the given `input` set.
     The result will be passed to -process as `compilation`.")
  (-process [this graph compilation values inputs-map]
    "Processes the `graph` traversing the given `compilation`, 
     using the current `values` of the context and the given 
     `inputs-map` as {input-id value}. Returns the new values
     of the context."))

(defn input?
  "Checks if x is an input"
  [x]
  (extends? Input (class x)))

(defn node?
  "Checks if x is any type of graph node"
  [x]
  (extends? Node (class x)))

(defn- cycles?
  "Checks if there are cycles on `adjacency-map` starting from start-id.
  Returns true if there are cycles, false otherwise."
  ([adj-m start-id]
   (cycles? #{} start-id adj-m))
  ([path adj-m start-id]
   (if (contains? path start-id)
     true
     (boolean (some (partial cycles? (conj path start-id) adj-m) (get adj-m start-id))))))

(defn- reverse-adjacency-map
  "Given an adjacency map, returns a reversed adjacency map (dependencies to dependants or viceversa)"
  [adjacency-map]
  (->> adjacency-map
       (map key)
       (map (fn [id]
              [id (->> adjacency-map
                       (filter (fn [[_ s]] (contains? s id)))
                       (map key)
                       (into #{}))]))
       (into {})))

(defmulti -node-sources-ids type)

(defmethod -node-sources-ids ComputeNode
  [x]
  (into #{} (map (comp :id second)) (:sources x)))

(defmethod -node-sources-ids InputNode
  [_]
  #{})

(defn- add-recursive
  [{:keys [nodes] :as graph} {:keys [id sources] :as node}]
  (if (get nodes id)
    graph
    (let [node-sources-ids (-node-sources-ids node)
          graph-with-sources (reduce add-recursive graph (map val sources))
          new-graph (-> graph-with-sources
                        (update :nodes assoc id node)
                        (update :adjacency-map assoc id node-sources-ids))]
      (assert (not (cycles? (:adjacency-map new-graph) id)))
      new-graph)))

(defn add
  "Adds `node` to `graph` under the given `label`.
   Recursively adds all sources of `node` that do not exist yet on `graph`, without label.
   Throws if another node already exists with the same label or the resulting graph has cycles."
  [graph label node]
  (assert (node? node) "Node must be either an input node or a compute node.")
  (assert (not (get (:labels graph) label)) "A node already exists with this label.")
  (-> graph
      (update :labels assoc label (:id node))
      (add-recursive node)))

(defn input-node
  "Returns a graph node that can be used to input values when processing the graph context.
   input-nodes do not hold their values through processes of the context, and only have a value
   when they are specified as inputs to `process`."
  []
  (->InputNode (random-uuid)))

(defn compute-node
  "Returns a graph node that computes a new value from the values of its `sources` using `handler`.
   `sources` is a map as {<source-label> <soure-node>}.
   `handler` is a 2-arity function accepting the current node value and a map of {<source-label> <source-value>}."
  [sources handler]
  (assert (seq sources) "Sources of a compute-node cannot be empty.")
  (->ComputeNode (random-uuid) sources handler))

(defn graph
  "Returns a graph, empty or from a map of {label node}."
  ([]
   (->Graph {} {} {}))
  ([nodes-map]
   (reduce-kv (fn [g label node]
                (add g label node))
              (graph)
              nodes-map)))

(defn- -build-topology-depths
  "Returns a map of node-ids and a set of depths as the values."
  [depths-map depth next-ids adjacency-map]
  (if (not (seq next-ids))
    depths-map
    (let [updated-depths (reduce (fn [acc id]
                                   (update acc id (fnil conj #{}) depth))
                                 depths-map
                                 next-ids)
          next-ids (->> (select-keys adjacency-map next-ids)
                        (mapcat second))]
      (-build-topology-depths updated-depths (inc depth) next-ids adjacency-map))))

(defn topological-sort
  "Returns a parallel topological sort of the graph, as a collection of steps,
   where each step is a collection of node ids that can be run in parallel.
   Useful as a base for certain processor compilations."
  [graph input-ids]
  (let [depths-map (-build-topology-depths {} 0 input-ids (reverse-adjacency-map (:adjacency-map graph)))]
    (->> (update-vals depths-map (fn [depths] (apply max depths)))
         (group-by second)
         (sort)
         (map (comp #(map first %) second)))))

;; Graph inputs

(defn- input-ids
  "Returns ids of input nodes of g"
  [g]
  (->> (:adjacency-map g)
       (filter (comp empty? second))
       (map key)
       (into #{})))

(defn input-labels
  "Returns the labels of input-nodes of g"
  [g]
  (let [ids (input-ids g)]
    (into #{} (select-keys (set/map-invert (:labels g)) ids))))

;; Process helpers

(defn -base-compilation
  "Base compilation used by sequential and parallel processors. 
   When parallel? is false, the topology is flattened to be 1-dimensional.
   When no inputs specified, it will create a compilation for all input nodes of the graph.
   Throws when any input does not exist as input of the graph."
  ([graph parallel?]
   (-base-compilation graph (input-ids graph) parallel?))
  ([graph inputs parallel?]
   (let [inputs (set inputs)]
     (assert (set/subset? inputs (input-ids graph)) "Inputs must be input-nodes inside the graph.")
     (cond-> (rest (topological-sort graph inputs))
       (not parallel?) (flatten)))))

(defn -sources-values
  "Returns a map of {source-label source-state}, given a `node`, current process accumulated `values`
   and currently processing `inputs`.
   Useful for implementing graph-processors."
  [node values inputs]
  (update-vals (:sources node)
               (fn [{:keys [id]}]
                 (or (get values id) (get inputs id)))))

(defn values
  "Returns current `context` values as {label value}."
  [context]
  (let [g (:graph context)
        vals (:values context)]
    (update-vals (:labels g) (fn [id] (get vals id)))))

(defn value
  "Returns the value of node identified by `label` in `context`.
   Faster than `(get (values context) label)`"
  [context label]
  (let [id (get-in context [:graph :labels label])]
    (get (:values context) id)))

(defn precompile
  "Creates a compilation for the given `inputs-labels`, and stores the result into `context` to
   be used when processing the graph with the same `input-labels` in future calls to `process`.
   Labels corresponding to non-input nodes will be ignored."
  [{:keys [graph processor] :as context} input-labels]
  (let [iids (set/intersection (set (vals (select-keys (:labels graph) input-labels)))
                               (input-ids graph))
        compilation (-compile processor graph iids)]
    (update context :compilations assoc iids compilation)))

(defn- translate-labelled-inputs
  [labels-mapping labelled-inputs]
  (persistent!
   (reduce-kv (fn [acc label v]
                (let [id (get labels-mapping label)]
                  (if id
                    (assoc! acc id v)
                    acc)))
              (transient {})
              labelled-inputs)))

(defn process
  "Processes the given `context` using `labelled-inputs` as {input-label value}.
   If a compilation didn't exist for the current inputs, it will create one and store it in `context`.
   Returns updated `context`."
  [{:keys [processor graph compilations values] :as context} labelled-inputs]
  (let [labels-mapping (:labels graph)
        input-map (translate-labelled-inputs labels-mapping labelled-inputs)
        input-ids-set (set (keys input-map))
        existing-compilation (get compilations input-ids-set)
        compilation (or existing-compilation (-compile processor graph input-ids-set))
        new-values (-process processor graph compilation values input-map)]
    (cond-> context
      (not existing-compilation) (update :compilations assoc input-ids-set compilation)
      :always (assoc :values new-values))))

(defn sequential-processor
  "Returns a sequential processor that processes the nodes sequentially."
  []
  (reify Processor
    (-compile [_ graph input-ids]
      (-base-compilation graph input-ids false))

    (-process [_ {:keys [nodes]} compilation values inputs]
      (loop [-values (transient values)
             remaining (queue compilation)]
        (if-not (seq remaining)
          (persistent! -values)
          (let [node-id (peek remaining)
                node (get nodes node-id)
                handler (:handler node)
                node-value (handler (get -values node-id)
                                    (-sources-values node -values inputs))]
            (recur (assoc! -values node-id node-value)
                   (pop remaining))))))))

(defn parallel-processor
  "Returns a parallel processor that will execute each parallel step of the topological sort using pmap."
  []
  (reify Processor
    (-compile [_ graph input-ids]
      (-base-compilation graph input-ids true))

    (-process [_ {:keys [nodes]} compilation values inputs]
      (loop [-values values
             remaining (queue compilation)]
        (if-not (seq remaining)
          -values
          (let [node-ids (peek remaining)
                node-values (into {} (pmap
                                      (fn [id]
                                        (let [node (get nodes id)
                                              handler (:handler node)
                                              node-value (handler (get -values id)
                                                                  (-sources-values node -values inputs))]
                                          [id node-value]))
                                      node-ids))]
            (recur (merge -values node-values)
                   (pop remaining))))))))

(defn context
  "Returns a context to execute the given `graph` with `processor`.
   If `processor` is not specified, it will use a `sequential-processor`."
  ([graph]
   (context graph (sequential-processor)))
  ([graph processor]
   (->Context graph {} {} processor)))
