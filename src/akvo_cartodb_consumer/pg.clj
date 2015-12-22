(ns akvo-cartodb-consumer.pg
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.core.async :as async]
            [cheshire.core :as json]
            [taoensso.timbre :as timbre]
            [corax.core :as sentry])
  (:import [java.util.concurrent Executors TimeUnit]))

(set! *warn-on-reflection* true)

(defn event-log-spec [config]
  (assert (not (empty? config)) "Config map is empty")
  {:subprotocol "postgresql"
   :subname (format "//%s:%s/%s"
                    (config :event-log-server)
                    (config :event-log-port)
                    (config :org-id))
   :user (config :event-log-user)
   :password (config :event-log-password)})

(defn get-from [offset]
  {:pre [(integer? offset)]}
  (format "SELECT id, payload::text FROM event_log WHERE id > %s ORDER BY id ASC"
          offset))

(defn event-chan
  [config offset]
  {:pre [(integer? offset)]}
  (let [chan (async/chan)
        close-chan (async/chan)
        db-spec (event-log-spec config)
        last-offset (atom offset)
        sentry-reporter (when-let [dsn (:sentry-dsn config)]
                          (sentry/new-error-reporter dsn))]
    (async/thread
      (loop []
        (try
          (with-open [conn (jdbc/get-connection db-spec)]
            (.setAutoCommit conn false)
            (with-open [stmt (.createStatement conn)]
              (.setFetchSize stmt 1000)
              (with-open [result-set (.executeQuery stmt (get-from @last-offset))]
                (while (.next result-set)
                  (let [offset (.getLong result-set 1)
                        payload (json/parse-string (.getString result-set 2))]
                    (async/>!! chan {:offset offset
                                     :payload payload})
                    (reset! last-offset offset))))))
          (catch Exception e
            (timbre/errorf e "Error while fething events for %s" (:org-id config))
            (when sentry-reporter
              (-> (sentry/message "Error while fetching events")
                  (sentry/exception e)
                  (sentry/culprit ::event-chan)
                  (sentry/extra {:org-id (:org-id config)})
                  (sentry/report sentry-reporter)))))
        (async/alt!!
          (async/timeout 1000) (recur)
          close-chan nil)))
    {:chan chan
     :close! (fn []
               (async/close! close-chan)
               (async/close! chan))}))
