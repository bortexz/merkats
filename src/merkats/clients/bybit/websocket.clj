(ns merkats.clients.bybit.websocket
  (:require [merkats.resocket :as rs]
            [hato.websocket :as hato.ws]
            [clojure.set :as set]
            [merkats.extensions.core :refer [current-millis]]
            [merkats.clients.bybit.common :as common]
            [jsonista.core :as json]))

(def ^:private env->base-url
  {:testnet "wss://stream-testnet.bybit.com"
   :mainnet "wss://stream.bybit.com"})

(def ^:private expire-millis 3000)

(def ^:private json-mapper (json/object-mapper {:bigdecimals   true
                                                :decode-key-fn true}))

(defn- auth->query-string
  [{:keys [key-id key-secret]}]
  (let [expires (+ (current-millis) expire-millis)
        to-sign (format "GET/realtime%d" expires)
        signed (common/sign to-sign key-secret)]
    (format "api_key=%s&expires=%d&signature=%s" key-id expires signed)))

(defn- inverse-sub-msg
  [topics]
  (json/write-value-as-string {:op "subscribe" :args (vec topics)}))

(defn- inverse-unsub-msg
  [topics]
  (json/write-value-as-string {:op "unsubscribe" :args (vec topics)}))

(defn subscribe
  "Subscribes client to given topics"
  [{:keys [state_]} topics]
  (let [topics (set topics)
        [o n] (swap-vals! state_ update :topics into topics)
        new-topics (set/difference (:topics n) (:topics o))
        conn  (:conn n)]
    (when (and (seq new-topics) (some? conn) (not (.isOutputClosed conn)))
      (hato.ws/send! conn (inverse-sub-msg new-topics)))))

(defn unsubscribe
  "Unsubscribe client from given topics"
  [{:keys [state_]} topics]
  (let [topics (set topics)
        [o n] (swap-vals! state_ update :topics set/difference topics)
        unsub-topics (set/intersection (:topics o) topics)
        conn  (:conn n)]
    (when (and (seq unsub-topics) (some? conn) (not (.isOutputClosed conn)))
      (hato.ws/send! conn (inverse-unsub-msg unsub-topics)))))

(defn- juxt-user-f
  [user-f client-f]
  (if user-f (juxt user-f client-f) client-f))

(defrecord Connection [state_
                       resocket-conn]
  java.lang.AutoCloseable
  (close [_] (.close resocket-conn)))

(defn inverse-connection
  "Suitable for inverse perpetual and inverse expiring contracts.
   
   Note: subscribe and unsubscribe will send subscribe/unsubscribe messages depending if given topic
   is subscribed or not locally, but does not wait on exchanges responses for confirmation.
   If subscribe/unsubscribe are used from multiple threads or on quick succession, some 
   requests might error. If unsure about this behaviour, specify a set of topics in advance.
   TODO: Implement better handling of sub/unsub topics on websocket using the incoming request responses.
   stateflow of subbing/sub/unsubbing/unsub (?)
   
   Returns object implementing java.lang.AutoCloseable (.close)"
  [{:keys [resocket-opts env auth on-data on-request-error on-request-success topics]
    :or   {env           :testnet
           resocket-opts {}
           topics        #{}
           on-request-error (constantly nil)
           on-request-success (constantly nil)}
    :as opts}]
  (let [state_        (atom {:conn   nil
                             :topics (set topics)})
        
        resocket-opts (-> resocket-opts
                          (assoc :url-fn
                                 (fn []
                                   (cond-> (str (env->base-url env) "/realtime")
                                     auth (str "?" (auth->query-string auth)))))
                          
                          (update :on-new-connection
                                  juxt-user-f
                                  (fn [ws]
                                    (let [{:keys [topics]} (swap! state_ assoc :conn ws)]
                                      (when (seq topics)
                                        (hato.ws/send! ws (inverse-sub-msg topics))))))
                          
                          (update :on-message
                                  juxt-user-f
                                  (fn [_ msg]
                                    (let [m (json/read-value msg json-mapper)]
                                      (cond
                                        (:topic m)
                                        (on-data m)

                                        (and (:request m) (not (:success m)))
                                        (on-request-error m)

                                        (and (:request m) (:succes m))
                                        (on-request-success m))))))
        
        resocket-conn (rs/connection resocket-opts)]
    (->Connection state_ resocket-conn)))

(defn component
  [opts]
  (with-meta opts
    {`com.stuartsierra.component/start
     (fn [opts]
       (with-meta
         (inverse-connection opts)
         {`com.stuartsierra.component/stop
          (fn [conn]
            (.close conn)
            (component opts))}))}))

(comment
  (def c (inverse-connection {:topics ["orderBookL2_25.BTCUSD"] 
                              :on-data (fn [msg] (tap> msg))}))
  (.close c)

  (def com (component {:topics ["orderBookL2_25.BTCUSD"]
                       :on-data (fn [msg] (tap> msg))}))
  
  (require '[com.stuartsierra.component])
  (def started (com.stuartsierra.component/start com))
  (def stopped (com.stuartsierra.component/stop started))
  (def restarted (com.stuartsierra.component/start stopped))
  (def restopped (com.stuartsierra.component/stop restarted))
  )