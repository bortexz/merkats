(ns merkats.resocket
  "Re(connectable web)socket
   TODO: opts namespaced? hmm!"
  (:require [hato.websocket :as websocket]
            [clojure.core.async :as a])
  (:import (java.nio ByteBuffer)))

(defn- execute-in!
  [ms f & args]
  (a/go (a/<! (a/timeout ms))
        {:result (apply f args)}))

(defn- ->ping-data [x] (ByteBuffer/wrap (.getBytes x)))

(defrecord Connection [state_
                       closed?_
                       close-fn
                       opts]
  java.lang.AutoCloseable
  (close [_] (close-fn)))

(defn connection
  [{:keys [retry-ms-fn
           ping-pong?
           abort-ms
           ping-data
           ping-pong-ms
           pong-ack-ms
           url-fn
           client-opts
           on-new-connection
           on-message
           on-connection-error]
    :or   {retry-ms-fn         (constantly 1000)
           ping-pong?          true
           ping-pong-ms        5000
           pong-ack-ms         1000
           ping-data           "ping"
           abort-ms            1000
           client-opts         {}
           on-new-connection   (constantly nil)
           on-connection-error (constantly nil)}
    :as opts}]
  (let [get-connection (fn [handlers] @(websocket/websocket (url-fn) (merge client-opts handlers)))

        state_    (agent {:token         nil
                          :conn          nil
                          :closed?       false
                          :pending-pong? false
                          :retry-attempt 0})

        sync-state (fn [token f & args]
                     (send state_
                           (fn [state]
                             (if (= token (:token state))
                               (apply f state args)
                               state))))

        closed?_  (promise)]
    (letfn [; Creates connection and updates state accordingly.
            ; If already connected or used closed?, does nothing
            (connect+ [{:keys [conn token closed? retry-attempt]
                        :as   state}]
              (if (or (and conn (not (.isInputClosed conn)))
                      closed?)
                state ; Already connected or closed?, don't do anything
                (let [new-token (random-uuid)
                      new-conn  (try (get-connection (handlers new-token))
                                     (catch Throwable t
                                       (on-connection-error t)
                                       (sync-state new-token retry+)
                                       nil))]
                  {:conn          new-conn
                   :token         new-token
                   :closed?       false
                   :pending-pong? false
                   :retry-attempt retry-attempt})))

            ; Cleanup current connection and reset token
            (cleanup+ [{:keys [closed?]
                        :as   state}]
              ; When cleanup on closed?, complete closed-p for blocking close-fn
              (when closed? (deliver closed?_ true))
              (-> state
                  (assoc :conn nil)
                  (assoc :token (random-uuid))
                  (assoc :pending-pong? false)))

            (retry+ [{:keys [token retry-attempt closed?]
                      :as   state}]
              (if closed?
                state
                (let [retry-att (inc retry-attempt)
                      retry-ms  (retry-ms-fn retry-att)]
                  (execute-in! retry-ms sync-state token connect+)
                  (assoc state :retry-attempt retry-att))))

            ; terminate current connection and send connect+ if not closed?
            (terminate+ [{:keys [conn token closed?]
                          :as   state}]
              (if conn
                (do
                  (when (not (.isOutputClosed conn)) (websocket/close! conn))
                  (execute-in! abort-ms sync-state token (if closed?
                                                           (comp cleanup+ abort+)
                                                           (comp retry+ cleanup+ abort+))))
                (cleanup+ state))
              state)

            ; Set pending-pong? to false when conn
            (is-alive+ [state] (assoc state :pending-pong? false))

            ; If not alive, does terminate+ to force reconnection
            (check-alive+ [{:keys [pending-pong?]
                            :as   state}]
              (cond-> state
                pending-pong? (terminate+)))

            ; Sends ping, and setups check-alive+ to be executen in ping-pong-ms
            (keep-alive+ [{:keys [conn token]
                           :as   state}]
              (if (and conn (not (.isOutputClosed conn)))
                (try (websocket/ping! conn (->ping-data ping-data))
                     (execute-in! pong-ack-ms sync-state token check-alive+)
                     (assoc state :pending-pong? true)
                     (catch Exception e
                       (sync-state token terminate+)
                       state))
                ; If keep-alive finds closed conn, retries
                (retry+ state)))

            ; abort conn, usually after no on-close received when closing gracefully, always cleanups
            (abort+ [{:keys [conn]
                      :as   state}]
              (websocket/abort! conn))

            ; When confirmed connection working, reset retries
            (reset-retries+ [state]
              (assoc state :retry-attempt 0))

            ; connection handlers
            (handlers [token]
              {:on-message (let [sb  (StringBuilder.)
                                 ack (volatile! false)]
                             (fn [ws msg last?]
                               (.append sb msg)
                               (when last?
                                 (on-message ws (.toString sb))
                                 (.setLength sb 0)
                                 ; on first complete message, restart retries
                                 (when-not @ack
                                   (vreset! ack true)
                                   (sync-state token reset-retries+)))))

               :on-open    (fn [ws]
                             (on-new-connection ws)
                             (when ping-pong?
                               (execute-in! ping-pong-ms sync-state token keep-alive+))
                             nil)

               :on-close   (fn [_ _ _]
                             (sync-state token (comp retry+ cleanup+))
                             nil)

               :on-error   (fn [_ _]
                             (sync-state token (comp retry+ cleanup+))
                             nil)

               :on-pong    (fn [_ _]
                             (when ping-pong?
                               (sync-state token is-alive+)
                               (execute-in! ping-pong-ms sync-state token keep-alive+))
                             nil)})]
      (send state_ connect+)
      (->Connection state_
                    closed?_
                    (fn []
                      (when-not (realized? closed?_)
                        (send state_ (comp terminate+
                                           (fn [state] (assoc state :closed? true))))
                        (deref closed?_)))
                    opts))))

(defn component
  [opts]
  (with-meta opts
    {`com.stuartsierra.component/start
     (fn [opts]
       (with-meta (connection opts)
         {`com.stuartsierra.component/stop
          (fn [conn]
            (.close conn)
            (component opts))}))}))