(ns akvo-cartodb-consumer.core
  (:gen-class)
  (:import [com.google.apphosting.utils.config
            AppEngineWebXml
            AppEngineWebXmlReader
            AppEngineConfigException])
  (:require [clojure.edn :as edn]
            [clojure.string :as str]
            [clojure.set :as set]
            [clojure.pprint :as pp]
            [clojure.java.io :as io]
            [clojure.java.shell :as shell]
            [compojure.core :refer (defroutes GET POST routes)]
            [ring.adapter.jetty :as jetty]
            [akvo-cartodb-consumer.consumer :as consumer]
            [akvo-cartodb-consumer.cartodb :as cartodb]
            [taoensso.timbre :as timbre]
            [clj-statsd :as statsd]))

(defn ensure-directory [dir]
  (.mkdirs (io/file dir)))

(defn pull [repo]
  (let [exit-code (:exit
                   (shell/with-sh-dir repo
                     (shell/sh "git" "pull")))]
    (when-not (zero? exit-code)
      (throw (ex-info "Failed to pull repo"
                      {:repo repo
                       :exit-code exit-code})))))

(defn clone [repos-dir repo]
  (let [clone-url (format "https://github.com/akvo/%s.git" repo)
        exit-code (:exit
                   (shell/with-sh-dir repos-dir
                     (shell/sh "git" "clone" clone-url)))]
    (when-not (zero? exit-code)
      (throw (ex-info "Failed to clone repo"
                      {:repos-dir repos-dir
                       :repo repo
                       :clone-url clone-url
                       :exit-code exit-code})))))

(defn clone-or-pull [repos-dir repo]
  (if (.exists (io/file repos-dir repo))
    (pull (str repos-dir "/" repo))
    (clone repos-dir repo)))

(defn get-cartodb-config [akvo-flow-server-config-path org-id]
  (let [appengine-web (-> (str akvo-flow-server-config-path "/" org-id "/appengine-web.xml")
                          io/file
                          .getAbsolutePath
                          (AppEngineWebXmlReader. "")
                          .readAppEngineWebXml
                          .getSystemProperties)]
    {:cartodb-api-key (get appengine-web "cartodbApiKey")
     :cartodb-host (get appengine-web "cartodbHost")}))


(defn get-config [akvo-config-path akvo-flow-server-config-path config-file-name]
  (let [config (-> (str akvo-config-path "/services/cartodb/" config-file-name)
                   slurp
                   edn/read-string
                   (assoc :akvo-config-path akvo-config-path
                          :akvo-flow-services-path akvo-flow-server-config-path
                          :config-file-name config-file-name))
        instances (reduce (fn [result org-id]
                            (update-in result
                                       [org-id]
                                       merge
                                       (get-cartodb-config akvo-flow-server-config-path org-id)
                                       (select-keys config [:event-log-port :event-log-server :event-log-password :event-log-user])))
                          {}
                          (keys (:instances config)))]
    (assoc config :instances instances)))

;; Returns a map org-id -> (started) IConsumer
(defn run-consumers
  [config]
  (into {}
        (for [[org-id consumer-config] (:instances config)]
          [org-id (let [consumer (cartodb/consumer org-id consumer-config)]
                    (consumer/start consumer)
                    consumer)])))


(defn reload-config [current-config running-consumers]
  (let [{:keys [akvo-config-path
                akvo-flow-services-path
                config-file-name]} current-config
        next-config (do (pull akvo-config-path)
                        (pull akvo-flow-services-path)
                        (get-config akvo-config-path
                                    akvo-flow-services-path
                                    config-file-name))
        current-instances (-> current-config :instances keys set)
        next-instances (-> next-config :instances keys set)
        new-instances (set/difference next-instances current-instances)
        removed-instances (set/difference current-instances next-instances)]
    (doseq [org-id new-instances]
      (swap! running-consumers
             assoc
             org-id
             (let [consumer (cartodb/consumer org-id (get-in next-config [:instances org-id]))]
               (consumer/start consumer)
               consumer)))
    (doseq [org-id removed-instances]
      (let [consumer (get @running-consumers org-id)]
        (consumer/stop consumer)
        (swap! running-consumers dissoc org-id)))))

;; Note: both config and running consumers are atoms. They can be redefined at runtime
(defn app [config running-consumers]
  (routes
   (GET "/" [] "ok")

   (GET "/config" []
        (format "<pre>%s</pre>"
                (str/escape (with-out-str (pp/pprint @config)) {\< "&lt;" \> "&gt;"})))

   (GET "/offset/:org-id" [org-id]
        (if-let [consumer (get @running-consumers org-id)]
          (format "Offset for consumer %s is currently %s" org-id (consumer/offset consumer))
          (format "Cartodb consumer %s is not currently running" org-id)))

   (POST "/restart/:org-id" [org-id]
         (let [current-consumers @running-consumers]
           (if-let [consumer (get current-consumers org-id)]
             (do (consumer/restart consumer)
                 (format "Cartodb consumer %s restarted" org-id))
             (format "Cartodb consumer %s is not currently running" org-id))))

   (POST "/reload-config" []
         (reload-config config running-consumers)
         "ok")))


(defn -main [repos-dir config-file-name]
  (ensure-directory repos-dir)
  (shell/with-sh-dir repos-dir
    (clone-or-pull repos-dir "akvo-flow-server-config")
    (clone-or-pull repos-dir "akvo-config"))
  (let [config (get-config (str repos-dir "/akvo-config")
                           (str repos-dir "/akvo-flow-server-config")
                           config-file-name)
        running-consumers (run-consumers config)]
    (timbre/set-level! (:log-level config :info))
    (statsd/setup (:statsd-host config)
                  (:statsd-port config)
                  :prefix (:statsd-prefix config))
    (let [port (Integer. (:port config 3030))]
      (jetty/run-jetty (app (atom config) (atom running-consumers))
                       {:port port
                        :join? false}))))
