(ns merkats.clients.bybit.rest
  (:require [merkats.clients.bybit.common :as common]
            [merkats.extensions.core :refer [current-millis]]
            [hato.middleware :as http.middleware]
            [hato.client :as http.client]
            [jsonista.core :as json]
            [clojure.alpha.spec :as s]
            [merkats.anomalies :as anomalies])
  (:import (java.io IOException)
           (java.net.http HttpTimeoutException)))

(def ^:private env->base-url
  {:testnet "https://api-testnet.bybit.com"
   :mainnet "https://api.bybit.com"})

(defn- prepare-auth-request
  [{:keys [params request-method] :as req}
   {:keys [key-id key-secret recv-window]}]
  (if-not (seq params)
    req
    (let [with-auth-params (-> params
                               (assoc :api_key key-id)
                               (assoc :timestamp (current-millis))
                               (cond->
                                recv-window (assoc :recv_window recv-window)))
          sorted-params    (into (sorted-map) with-auth-params)
          query-string     (http.middleware/generate-query-string sorted-params)
          signed           (common/sign query-string key-secret)
          query-string     (str query-string "&sign=" signed)]
      (if (#{:get} request-method)
        (assoc req :query-string query-string)
        (assoc req :body (json/write-value-as-string
                          (assoc with-auth-params :sign signed)
                          common/json-mapper))))))

(defn- auth-request-middleware
  [auth]
  (fn [client]
    (fn
      ([req]               (client (prepare-auth-request req auth)))
      ([req respond raise] (client (prepare-auth-request req auth) respond raise)))))

(defn- prepare-public-request
  [{:keys [request-method params] :as req}]
  (if (and (#{:get} request-method) (seq params))
    (assoc req :query-string (http.middleware/generate-query-string params))
    req))

(defn- public-request-middleware
  []
  (fn [client]
    (fn
      ([req]               (client (prepare-public-request req)))
      ([req respond raise] (client (prepare-public-request req) respond raise)))))

(defn- prepare-response
  [resp]
  (cond-> resp
    (= 200 (:status resp)) (update :body json/read-value common/json-mapper)))

(defn- response-middleware
  []
  (fn [client]
    (fn
      ([req]               (prepare-response (client req)))
      ([req respond raise] (client req #(respond (prepare-response %)) raise)))))

(defn- auth-middleware-stack
  [auth]
  [http.middleware/wrap-request-timing
   (response-middleware)
   (auth-request-middleware auth)
   http.middleware/wrap-method
   http.middleware/wrap-content-type
   http.middleware/wrap-url])

(defn- public-middleware-stack
  []
  [http.middleware/wrap-request-timing
   (response-middleware)
   (public-request-middleware)
   http.middleware/wrap-method
   http.middleware/wrap-content-type
   http.middleware/wrap-url])

(def rate-limiter->paths
  "TODO: Add other instrument types, for now only inverse-perpetual"
  {;; https://bybit-exchange.github.io/docs/inverse/#t-ratelimits
   ;; 100/min
   :inverse-perp/order-ops        #{"/v2/private/order/cancel"
                                    "/v2/private/order/create"
                                    "/v2/private/order/cancelAll"
                                    "/v2/private/order/replace"
                                    "/v2/private/stop-order/create"
                                    "/v2/private/stop-order/cancel"
                                    "/v2/private/stop-order/replace"
                                    "/v2/private/stop-order/cancelAll"}
   ;; 600/min
   :inverse-perp/order-list       #{"/v2/private/order/list"
                                    "/v2/private/stop-order/list"
                                    "/v2/private/order"}
   ;; 120/min
   :inverse-perp/execution-list   #{"/v2/private/execution/list"}
   ;; 75/min
   :inverse-perp/position-margin  #{"/v2/private/position/leverage/save"
                                    "/v2/private/position/change-position-margin"
                                    "/v2/private/position/trading-stop"
                                    "/v2/private/tpsl/switch-mode"}
   ;; 120/min
   :inverse-perp/position-balance #{"/v2/private/position/list"
                                    "/v2/private/wallet/balance"}
   ;; 120/min
   :inverse-perp/funding-rate     #{"/v2/private/funding/prev-funding-rate"
                                    "/v2/private/funding/prev-funding"
                                    "/v2/private/funding/predicted-funding"}
   ;; 120/min
   :inverse-perp/wallet           #{"/v2/private/wallet/fund/records"
                                    "/v2/private/wallet/withdraw/list"}
   ;; 600/min
   :inverse-perp/api-key          #{"/v2/private/account/api-key"}})

(def ^:private path->rate-limiter
  (->> rate-limiter->paths
       (mapcat (fn [[c eps]] (zipmap eps (repeat c))))
       (into {})))

(defrecord Client [rate-limits_
                   auth-middleware
                   public-middleware
                   base-url])

(defn request
  "Options:
     - `:path` of the request, e.g `/v2/public/orderBook/L2`
     - `:method` e/o #{:get :post} http method, passed directly to http client
     - `:params` request params, automatically uses either query-params or form-params depending :method.
     - `:timeout` timeout before the request fails
   
   See [[client]] for more info.
   "
  [{:keys [public-middleware auth-middleware base-url rate-limits_]}
   {:keys [path method params timeout]}]
  (let [rate-limiter            (get path->rate-limiter path)
        private?                (some? rate-limiter) ; If has rate-limiter is private path
        [reset-ms limit-status] (get @rate-limits_ rate-limiter)
        current-ms              (current-millis)
        auth-allowed?           (or (and private? (some? auth-middleware)) (not private?))
        rl-allowed?             (or (not private?)
                                    (not reset-ms)
                                    (>= current-ms reset-ms)
                                    (> limit-status 0))
        middleware              (if private? auth-middleware public-middleware)]
    (cond
      (not auth-allowed?) {::anomalies/category anomalies/unauthorized}
      (not rl-allowed?)   {::anomalies/category anomalies/rate-limited}
      :else
      (try
        (let [res    (http.client/request {:url          (str base-url path)
                                           :content-type :json
                                           :method       method
                                           :params       params
                                           :middleware   middleware
                                           :timeout      timeout})
              status (:status res)
              result (case status
                       200 (:body res)
                       403 {::anomalies/category anomalies/rate-limited}
                       404 {::anomalies/category anomalies/not-found}
                       {::anomalies/category anomalies/fault ::response res})]
          (when (and rate-limiter (= 200 status))
            (swap! rate-limits_
                   (fn [rate-limits]
                     (let [rl-status   (:rate_limit_status result)
                           rl-reset-ms (:rate_limit_reset_ms result)
                           rl-info     (and rl-status rl-reset-ms [rl-reset-ms rl-status])
                           existing    (get rate-limits rate-limiter)]
                       (cond-> rate-limits

                         (and rl-info existing)
                         (update rate-limiter
                                 (fn [[curr-reset-ms curr-status :as curr-rl]]
                                   (cond
                                     (>= rl-reset-ms curr-reset-ms)
                                     rl-info

                                     (= rl-reset-ms curr-reset-ms)
                                     [rl-reset-ms (min curr-status rl-status)]

                                     :else curr-rl)))

                         (and rl-info (not existing))
                         (assoc rate-limiter rl-info))))))
          (with-meta result res))
        (catch HttpTimeoutException _ {::anomalies/category anomalies/timeout})
        (catch IOException e {::anomalies/category anomalies/connection ::error e})
        (catch Exception e {::anomalies/category anomalies/fault ::error e})))))

(defn client
  "Creates a client to call the bybit REST API.

   Features:
   - Automatically signs private requests when `:auth` opt is specified.

   - Rate-Limiter of private endpoints using response info from bybit (rate_limit_status and 
     rate_limit_reset_ms). Everytime that a request is done before the latest rate_limit_reset_ms
     for each paths grouping, the request automatically returns {::anomalies/category ::anomalies/busy}.
     Paths are grouped for rate-limiting. See https://bybit-exchange.github.io/docs/inverse/#t-understandingratelimits
     for inverse rate-limits.
   
   - If auth is not provided, private paths will automatically fail with 
     {::anomalies/category ::anomalies/forbidden}.
   
   - Automatically translates 403 and 404 to ::anomalies/busy and ::anomalies/not-found. 
     All 200 responses are left untouched, and the :body of the response is returned JSON parsed.
     Other exceptional error codes from bybit are translated to {::anomalies/category ::anomalies/fault}
   
   - The original response is always returned as metadata of the return value of request-fn, whenever
     an actual request was made (if not prevented by rate-limit or auth on client side).
   
   - Intended for use in multi-threaded environments. As rate-limit info comes in responses, it won't
     rate-limit until a response contains rate_limit_reset_ms in the future, and could happen that
     multiple in-flight requests are rate-limited on bybit's end.

   - All other http client exceptions are mapped to anomalies, and return an :error key with the exception.
     Translations:
     - IllegalArgumentException -> ::anomalies/incorrect
     - SecurityException        -> ::anomalies/forbidden
     - IOException              -> ::anomalies/unavailable
     - InterruptedException     -> ::anomalies/interrupted
     - Any other throwable      -> ::anomalies/fault 

   When calling request, it returns either a map with {::anomalies/category ...} or the body
   of a successful request.
     
   Note: Successful requests can still be 'errors', see https://bybit-exchange.github.io/docs/inverse/#t-errors
   `request` only handles errors when status is 403, 404.

   Example:
   ```clojure
   (def client (client {:env :testnet}))
   (request client
            {:path \"/v2/public/orderBook/L2\"
             :method :get
             :params {:symbol \"BTCUSD\"}
             :timeout 5000})
   ```

   TODO: Currently only working for inverse-perpetual endpoints, more to be added.
   TODO: Don't use anomalies, use normal non-ns-kw errors, document them. {:error :private-rate-limit} etc...?
   TODO: Also use limit-status or just remove it, currently held but not used. Also remove from docs
   "
  [{:keys [auth env] :or {env :testnet}}]
  (->Client (atom {})
            (when auth (auth-middleware-stack auth))
            (public-middleware-stack)
            (env->base-url env)))

(defn component
  "Returns a client that implements component/Lifecycle protocol"
  [opts]
  (with-meta opts
    {'com.stuartsierra.component/start
     (fn [_]
       (with-meta (client opts)
         {'com.stuartsierra.component/stop
          (component opts)}))}))

(comment
  (def c (client {:env :testnet}))
  
  (def res (request c {:method :get
                       :path "/v2/public/kline/list"
                       :params {:symbol "BTCUSD"
                                :interval "1"
                                :from 1645735722
                                :limit 10}}))
  res
  (meta res))
