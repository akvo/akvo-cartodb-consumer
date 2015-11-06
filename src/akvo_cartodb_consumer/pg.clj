(ns akvo-cartodb-consumer.pg
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.core.async :as async]
            [cheshire.core :as json]
            [taoensso.timbre :as timbre])
  (:import [java.util.concurrent Executors TimeUnit]))

(set! *warn-on-reflection* true)

;; TODO config is misleading
(defn event-log-spec [config org-id]
  (assert (not (empty? config)) "Config map is empty")
  {:subprotocol "postgresql"
   :subname (format "//%s:%s/%s"
                    (config :event-log-server)
                    (config :event-log-port)
                    org-id)
   :user (config :event-log-user)
   :password (config :event-log-password)})

(defn- org-id
  "Get the org-id out of an event-log-spec"
  [db-spec]
  (let [subname (:subname db-spec)]
    (subs subname (inc (.lastIndexOf subname "/")))))

(def get-by-id
  "SELECT payload::text FROM event_log WHERE id = %s")

(defn- get-payload [conn id]
  (let [id (Long/parseLong id)]
    (with-open [stmt (.createStatement conn)
                rs (.executeQuery stmt (format get-by-id id))]
      (.next rs)
      {:offset id
       :payload (json/parse-string (.getString rs 1))})))

(defn- poll [conn chan]
  (fn []
    (with-open [stmt (.createStatement conn)
                rs (.executeQuery stmt "SELECT 1")])
    (doseq [notification (.getNotifications conn)]
      (let [payload (get-payload conn (.getParameter notification))]
        (async/>!! chan payload)))))

(defn publication [db-spec]
  (let [conn (jdbc/get-connection db-spec)
        scheduler (Executors/newScheduledThreadPool 1)
        chan (async/chan)
        pub (async/pub chan #(get-in % [:event "eventType"]))]

    (with-open [stmt (.createStatement conn)]
      (.execute stmt "LISTEN event_log"))

    (let [task (.scheduleWithFixedDelay scheduler
                                        (poll conn chan)
                                        1 ;; Initial delay
                                        1 ;; Delay
                                        TimeUnit/SECONDS)]
      {:pub pub
       :close! (fn []
                 (try
                   (with-open [stmt (.createStatement conn)]
                     (.execute stmt "UNLISTEN event_log"))
                   (.close conn)
                   (async/close! chan)
                   ;; (async/unsub-all pub)
                   (.cancel task true)
                   (.shutdown scheduler)
                   (catch Exception e
                     (.printStackTrace e))))})))

(defn close! [{:keys [close!]}]
  (close!))

(defn subscribe [{:keys [pub]} event-types]
  (let [chan (async/chan)]
    (doseq [event-type event-types]
      (async/sub pub event-type chan))
    chan))

(defn unsubscribe [pub chan event-types]
  (doseq [event-type event-types]
    (async/unsub pub chan event-type)))

(defn get-from [offset]
  {:pre [(integer? offset)]}
  (format "SELECT id, payload::text FROM event_log WHERE id > %s ORDER BY id ASC"
          offset))

(defn event-chan* [db-spec offset]
  {:pre [(integer? offset)]}
  (let [chan (async/chan)
        listener-conn (jdbc/get-connection db-spec)
        scheduler (Executors/newScheduledThreadPool 1)]
    (with-open [stmt (.createStatement listener-conn)]
      (.execute stmt "LISTEN event_log"))
    (async/thread
      (with-open [conn (jdbc/get-connection db-spec)]
        (.setAutoCommit conn false)
        (with-open [stmt (.createStatement conn)]
          (.setFetchSize stmt 1000)
          (with-open [result-set (.executeQuery stmt (get-from offset))]
            (let [t (System/nanoTime)]
              (loop [c 0]
                (if (.next result-set)
                  (do
                    (async/>!! chan {:offset (.getLong result-set 1)
                                     :payload (json/parse-string (.getString result-set 2))})
                    (recur (inc c)))
                  (do
                    ;; Catch up done, start listening
                    (timbre/infof "Catch-up done for %s. Processed %d events in %d seconds"
                                  (org-id db-spec)
                                  c
                                  (long (/ (- (System/nanoTime) t)
                                           (* 1000000 1000))))
                    (timbre/infof "Start polling for new events for %s"
                                  (org-id db-spec))
                    (.scheduleWithFixedDelay scheduler
                                             (poll listener-conn chan)
                                             1 ;; Initial delay
                                             1 ;; Delay
                                             TimeUnit/SECONDS)))))))))
    {:chan chan
     :close! (fn []
               (async/close! chan)
               (.close listener-conn)
               (.shutdown scheduler))}))
