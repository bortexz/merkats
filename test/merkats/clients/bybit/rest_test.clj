(ns merkats.clients.bybit.rest-test
  (:require [clojure.test :refer :all]
            [merkats.anomalies :as anomalies]
            [merkats.extensions.core :refer [current-millis]]
            [hato.client :as http.client]
            [merkats.clients.bybit.rest :as rest :refer [request]]))

(deftest request-fn
  (with-redefs [http.client/request (fn [_] {:status 200
                                             :body   {:result []}})]
    (testing "Public endpoint request returns the body"
      (let [c (rest/client {})]
        (is (= {:result []}
               (request c {:path "/v2/public/orderBook/L2"})))))
    (testing "Private endpoint request returns the body when auth specified"
      (let [c (rest/client {:auth {:key-id     "..."
                                   :key-secret "..."}})]
        (is (= {:result []}
               (request c {:path "/v2/private/order/list"})))))
    (testing "Private endpoint early return anomaly on missing auth"
      (let [c (rest/client {})]
        (is (= {::anomalies/category ::anomalies/unauthorized}
               (request c {:path "/v2/private/order/list"}))))))

  (testing "When receiving a reset-ms in the future, it correctly limits requests until given ms"
    (let [current-time (atom 10)]
      (with-redefs [http.client/request   (fn [_] {:status 200
                                                   :body   {:rate_limit_status   0
                                                            :rate_limit_reset_ms 12}})
                    current-millis (fn [] @current-time)]
        (let [c (rest/client {:auth {:key-id     "..."
                                     :key-secret "..."}})]
          (request c {:path   "/v2/private/order/list"
                      :method :get
                      :params {}})
          (is (= {::anomalies/category ::anomalies/rate-limited}
                 (request c {:path   "/v2/private/order/list"
                             :method :get
                             :params {}})))
          (testing "This only affects endpoints on the same category"
            (is (not (::anomalies/category (request c {:path   "/v2/private/position/list"
                                                       :method :get
                                                       :params {}})))))

          (testing "When time passes, requests are allowed again"
            (reset! current-time 15)
            (is (not (::anomalies/category (request c {:path   "/v2/private/order/list"
                                                       :method :get
                                                       :params {}}))))))))))