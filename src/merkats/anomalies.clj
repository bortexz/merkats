(ns merkats.anomalies
  (:require [clojure.alpha.spec :as s]))

(s/def ::category keyword?)

(def hierarchy (make-hierarchy))

;; Base errors

(def fault 
  "Unknown or uncategorized errors"
  ::fault)

(def incorrect
  "When the request is incorrect"
  ::incorrect)

(def unsupported
  "When verb is not supported"
  ::unsupported)

(def not-found
  "When resource is not found"
  ::not-found)

(def invalid-params 
  "Invalid request parameters"
  ::invalid-params)

(def unauthorized
  "Authentication error, API key, etc..."
  ::unauthorized)

(def connection 
  "Errors trying to establish the connection with the server. Retryable error."
  ::unreachable)

(def timeout
  "The request timed out without managing to establish a connection"
  ::timeout)

(def unavailable
  "When the platform is not available, it could be 500 errors, maintenance, etc..."
  ::unavailable)

(def busy
  "It is common for exchanges to have their API overloaded during high-volume times. Specific error
   for these situations."
  ::busy)

(def rate-limited
  "Error thrown when request is rate-limited by the server"
  ::rate-limited)

(def outdated
  "When a request has arrived at the platform too late, for platforms that have a 
   maximum received window."
  ::outdated)

;; Hierarchy

(derive hierarchy unsupported incorrect)
(derive hierarchy not-found incorrect)
(derive hierarchy invalid-params incorrect)
(derive hierarchy unauthorized incorrect)

(derive hierarchy timeout connection)
(derive hierarchy unavailable connection)
(derive hierarchy busy connection)
(derive hierarchy rate-limited connection)
(derive hierarchy outdated connection)
