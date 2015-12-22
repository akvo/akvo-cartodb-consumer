(defproject akvo-cartodb-consumer "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-environ "1.0.0"]]
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/java.jdbc "0.4.2"]
                 [org.clojure/core.async "0.2.374"]
                 [org.clojure/core.cache "0.6.4"]
                 [org.clojure/tools.nrepl "0.2.12"]
                 [org.postgresql/postgresql "9.4-1205-jdbc41"]
                 [com.google.appengine/appengine-tools-sdk "1.9.28"]
                 [com.google.appengine/appengine-remote-api "1.9.28"]
                 [com.google.appengine/appengine-api-1.0-sdk "1.9.28"]
                 [ring/ring-core "1.4.0" :exclusions [org.clojure/tools.reader]]
                 [ring/ring-json "0.4.0"]
                 [ring/ring-jetty-adapter "1.4.0"]
                 [com.taoensso/timbre "4.1.4" :exclusions [org.clojure/tools.reader]]
                 [listora/corax "0.3.0"]
                 [clj-http "2.0.0"]
                 [compojure "1.4.0"]
                 [cheshire "5.5.0"]
                 [clj-statsd "0.3.11"]]
  :aot [akvo-cartodb-consumer.core]
  :main akvo-cartodb-consumer.core

  ;; TODO figure out :profiles {:dev {:source-paths ["dev"]}}
  :source-paths ["dev" "src"])
