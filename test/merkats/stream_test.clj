(ns merkats.stream-test
  (:require [clojure.test :refer :all]
            [clojure.core.async :as a]
            [merkats.stream :as stream]))

(deftest base-test
  (let [ch (a/chan 1)
        closed_ (atom 0)
        stream (stream/create ch (fn [] (swap! closed_ inc)))]
    (testing "Returns ch"
      (is (= ch (stream/ch stream))))
    
    (testing "Can read from stream as if it was a ch"
      (a/>!! ch true)
      (is (= true (a/<!! stream))))
    
    (testing "Closing the stream also closes the ch"
      (stream/close! stream)
      (is (false? (a/>!! ch true))))
    
    (testing "Executes on-close fn"
      (is (= 1 @closed_)))
    
    (testing "Close is called only once"
      (stream/close! stream)
      (is (= 1 @closed_)))))