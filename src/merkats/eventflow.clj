(ns merkats.eventflow)

;; Protocols

(defprotocol Node
  (process [this input data] 
           "Processes a new event. 
            Returning a collection of tuples [output data] or nil."))

(defprotocol Pipeline
  (ingest [this id input data]
          "Ingests a new `event` into `input` of node identified with `id`.")
  
  (add-node [this id node]
            "Adds given node to pipeline")
  
  (remove-node [this id]
               "Removes node under `id` from pipeline, also removing all links from an to this node.")
  
  (add-link [this from-id from-output to-input to-id]
            "Creates a link between output `from-output` of node id `from-id` 
              to input `to-input` of node id `to-id`.")
  (remove-link [this from-id from-output to-input to-id]
               "Removes link between output `from-output` of node id `from-id` 
                 to input `to-input` of node id `to-id`.")
  (nodes [this] 
         "Returns map of {id node}")
  (links [this] 
         "Returns coll of [from-id from-output to-input to-id]"))