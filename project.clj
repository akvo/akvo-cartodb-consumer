(defproject akvo-cartodb-consumer "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-environ "1.0.0"]]
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/java.jdbc "0.3.6"]
                 [org.clojure/core.async "0.1.346.0-17112a-alpha"]
                 [org.clojure/core.cache "0.6.4"]
                 [org.clojure/tools.cli "0.3.1"]
                 [org.akvo/commons "0.3.0-SNAPSHOT"]
                 [org.postgresql/postgresql "9.3-1102-jdbc41"]
                 [com.github.fge/json-schema-validator "2.2.6"]
                 [com.google.appengine/appengine-tools-sdk "1.9.9"]
                 [com.google.appengine/appengine-remote-api "1.9.9"]
                 [com.google.appengine/appengine-api-1.0-sdk "1.9.9"]
                 [com.mchange/c3p0 "0.9.5"]
                 [ring/ring-core "1.3.2"]
                 [ring/ring-json "0.3.1"]
                 [ring/ring-jetty-adapter "1.3.2"]
                 [com.taoensso/timbre "3.4.0"]
                 [me.raynes/fs "1.4.6"]
                 [listora/corax "0.3.0"]
                 [http-kit "2.1.18"]
                 [clj-http "2.0.0"]
                 [liberator "0.12.2"]
                 [compojure "1.3.1"]
                 [yesql "0.4.0"]
                 [cheshire "5.4.0"]
                 [environ "1.0.0"]
                 [clj-time "0.9.0"]
                 [clj-statsd "0.3.11"]
                 [clojurewerkz/elastisch "2.1.0"]
                 #_[OfflineReverseGeocode/OfflineReverseGeocode "1.0-SNAPSHOT"]]
  :aot [akvo-cartodb-consumer.core]
  :main akvo-cartodb-consumer.core

  ;; TODO figure out :profiles {:dev {:source-paths ["dev"]}}
  :source-paths ["dev" "src"])
