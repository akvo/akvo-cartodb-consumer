(ns akvo-cartodb-consumer.cartodb
  (:refer-clojure :exclude (ensure))
  (:require [clojure.string :as string]
            [clojure.set :as set]
            [clojure.edn :as edn]
            [clojure.data :as data]
            [clojure.core.async :as async]
            [akvo-cartodb-consumer.pg :as pg]
            [akvo-cartodb-consumer.entity-store :as es]
            [akvo-cartodb-consumer.consumer :as consumer]
            [taoensso.timbre :as timbre]
            [clojure.pprint :refer (pprint)]
            [clojure.java.jdbc :as jdbc]
            [cheshire.core :refer (generate-string parse-string)]
            [environ.core :refer (env)]
            [org.httpkit.client :as http])
  (:import [org.postgresql.util PGobject]))

(defmacro ensure
  "Like clojure.core/assert, but throws an ExceptionInfo instead"
  ([expr] `(ensure ~expr "Ensure failed"))
  ([expr msg] `(ensure ~expr ~msg {}))
  ([expr msg map]
    `(when-not ~expr
       (throw (ex-info ~msg (merge {:expression '~expr}
                                   ~map))))))

(defn cartodb-spec [config org-id]
  (let [api-key (get-in config [org-id :cartodb-api-key])
        sql-api (get-in config [org-id :cartodb-sql-api])]
    (assert api-key "Cartodb api key is missing")
    (assert sql-api "Cartodb sql api url is missing")
    {:url sql-api
     :api-key api-key
     :org-id org-id}))

(def cartodbfy-data-points "SELECT cdb_cartodbfytable ('data_point');")

(def update-the-geom-function
  "CREATE OR REPLACE FUNCTION akvo_update_the_geom()
   RETURNS TRIGGER AS $$
   BEGIN
     NEW.the_geom := ST_SetSRID(ST_Point(NEW.lon, NEW.lat),4326);
     RETURN NEW;
   END;
   $$ language 'plpgsql';")

(declare queryf)

(defn setup-tables [cdb-spec]
  (let [offset-sql "CREATE TABLE IF NOT EXISTS event_offset (
                       org_id TEXT PRIMARY KEY,
                       event_offset BIGINT)"
        survey-sql "CREATE TABLE IF NOT EXISTS survey (
                       id BIGINT PRIMARY KEY,
                       name TEXT,
                       public BOOLEAN,
                       description TEXT);"
        form-sql "CREATE TABLE IF NOT EXISTS form (
                     id BIGINT PRIMARY KEY,
                     survey_id BIGINT,
                     name TEXT,
                     description TEXT);"
        question-sql "CREATE TABLE IF NOT EXISTS question (
                         id BIGINT PRIMARY KEY,
                         form_id BIGINT,
                         display_text TEXT,
                         identifier TEXT,
                         type TEXT);"
        data-point-sql "CREATE TABLE IF NOT EXISTS data_point (
                           id BIGINT PRIMARY KEY,
                           lat DOUBLE PRECISION,
                           lon DOUBLE PRECISION,
                           survey_id BIGINT,
                           name TEXT,
                           identifier TEXT);"
        entity-store-sql "CREATE TABLE IF NOT EXISTS entity_store (
                             entity_type TEXT NOT NULL,
                             id BIGINT NOT NULL,
                             entity TEXT NOT NULL,
                             PRIMARY KEY (entity_type, id));"]
    (queryf cdb-spec offset-sql)
    (queryf cdb-spec survey-sql)
    (queryf cdb-spec form-sql)
    (queryf cdb-spec question-sql)
    (queryf cdb-spec data-point-sql)
    ;; Is it safe to cartodbfytable multiple times?
    (queryf cdb-spec "SELECT cdb_cartodbfytable ('data_point');")
    (queryf cdb-spec entity-store-sql)))

(defn query [cdb-spec q]
  (timbre/trace q)
  @(http/get (:url cdb-spec)
             {:query-params {:q q
                             :api_key (:api-key cdb-spec)}}))

(defn escape-str [s]
  (if (string? s)
    (string/replace s "'" "''")
    ""))

(defn queryf [cdb-spec q & args]
  (let [body (-> (query cdb-spec (apply format q args))
                 :body
                 parse-string)]
    (if (contains? body "error")
      (timbre/warnf "Query error for %s. Query '%s' resulted in error message '%s'"
                    (:org-id cdb-spec) q (first (get body "error")))
      (get body "rows"))))

(defn question-type->db-type [question-type]
  (condp contains? question-type
    #{"FREE_TEXT" "OPTION" "NUMBER" "PHOTO" "GEO" "SCAN" "VIDEO" "GEOSHAPE"} "text"
    #{"DATE"} "date"
    #{"CASCADE"} "text[]"))

(defn answer-type->db-type [answer-type]
  (condp contains? answer-type
    #{"VALUE" "GEO" "IMAGE" "VIDEO" "OTHER"} "text"
    #{"DATE"} "date"
    #{"CASCADE"} "text[]"))

(defn raw-data-table-name [form-id]
  (ensure (integer? form-id) "Invalid form-id" {:form-id form-id})
  (str "raw_data_" form-id))

(defn munge-display-text [display-text]
  (ensure (string? display-text) "Invalid display-text" {:display-text display-text})
  (-> display-text
      (.replaceAll " " "_")
      (.replaceAll "[^A-Za-z0-9_]" "")))


(defn question-column-name
  ([cdb-spec question-id]
   (ensure (integer? question-id) "Invalid question-id" {:question-id question-id})
   (if-let [{:strs [display_text identifier]}
            ;; TODO cache lookup
            (-> (queryf cdb-spec
                        "SELECT display_text, identifier FROM question WHERE id=%s"
                        question-id)
                first)]
     (question-column-name question-id identifier display_text)
     (throw (ex-info "Could not find question" {:org-id (:org-id cdb-spec)
                                                :quesiton-id question-id}))))
  ([question-id identifier display-text]
   (ensure (and (integer? question-id)
                         (string? display-text)
                         (string? identifier))
           "Can not generate question-column-name"
           {:question-id question-id
            :display-text display-text
            :identifier identifier})
   (if (empty? identifier)
     (format "\"%s_%s\"" question-id (munge-display-text display-text))
     identifier)))

(defmulti handle-event
  (fn [cdb-spec entity-store event]
    (get-in event [:payload "eventType"])))

(defmethod handle-event :default [cdb-spec entity-store event]
  (timbre/tracef "Skipping %s at offset %s."
                 (get-in event [:payload "eventType"])
                 (get event :offset)))

(defmethod handle-event "surveyGroupCreated"
  [cdb-spec entity-store {:keys [payload offset]}]
  (let [entity (get payload "entity")]
    ;; Ignore folders for now.
    (when (= (get entity "surveyGroupType")
             "SURVEY")
      (queryf cdb-spec
              "INSERT INTO survey (id, name, public, description) VALUES (%s, '%s', %s, '%s')"
              (get entity "id")
              (escape-str (get entity "name"))
              (get entity "public")
              (escape-str (get entity "description"))))))

(defmethod handle-event "surveyGroupUpdated"
  [cdb-spec entity-store {:keys [payload offset]}]
  (let [entity (get payload "entity")]
    (when (= (get entity "surveyGroupType")
             "SURVEY")
      (queryf cdb-spec
              "UPDATE survey SET name='%s', public=%s, description='%s' WHERE id=%s"
              (escape-str (get entity "name"))
              (get entity "public")
              (escape-str (get entity "description"))
              (get entity "id")))))

(defmethod handle-event "formCreated"
  [cdb-spec entity-store {:keys [payload offset]}]
  (let [entity (get payload "entity")
        form-id (get entity "id")
        table-name (raw-data-table-name form-id)]
    (queryf cdb-spec
            "CREATE TABLE IF NOT EXISTS %s (
                id BIGINT UNIQUE NOT NULL,
                data_point_id BIGINT,
                lat DOUBLE PRECISION,
                lon DOUBLE PRECISION);"
            table-name)
    (queryf cdb-spec
            "INSERT INTO form (id, survey_id, name, description) VALUES (
                %s, %s, '%s', '%s')"
            form-id
            (get entity "surveyId")
            (escape-str (get entity "name" ""))
            (escape-str (get entity "description" "")))
    (queryf cdb-spec "SELECT cdb_cartodbfytable ('%s');" table-name)
    ;; TODO Figure out why why akvo_update_the_geom trigger doesn't work
    #_(queryf cdb-spec
              "CREATE TRIGGER \"akvo_update_the_geom_trigger\"
                  BEFORE UPDATE OR INSERT ON %s FOR EACH ROW
                  EXECUTE PROCEDURE akvo_update_the_geom();"
              table-name)))

(defmethod handle-event "formUpdated"
  [cdb-spec entity-store {:keys [payload offset]}]
  (let [form (get payload "entity")]
    (queryf cdb-spec
            "UPDATE form SET survey_id=%s, name='%s', description='%s' WHERE id=%s"
            (get form "surveyId")
            (escape-str (get form "name" ""))
            (escape-str (get form "description" ""))
            (get form "id"))))

(defmethod handle-event "questionCreated"
  [cdb-spec entity-store {:keys [payload offset]}]
  (let [question (get payload "entity")]
    (queryf cdb-spec
            "ALTER TABLE IF EXISTS %s ADD COLUMN %s %s"
            (raw-data-table-name (get question "formId"))
            (question-column-name (get question "id")
                                  (get question "identifier" "")
                                  (get question "displayText"))
            (question-type->db-type (get question "questionType")))
    (queryf cdb-spec
            "INSERT INTO question (id, form_id, display_text, identifier, type)
               VALUES ('%s','%s','%s','%s', '%s')"
            (get question "id")
            (get question "formId")
            (escape-str (get question "displayText"))
            (get question "identifier" "")
            (get question "questionType"))))

(defn get-question [cdb-spec id]
  (ensure (integer? id) "Invalid question id" {:org-id (:org-id cdb-spec)
                                               :id id})
  (first
   (queryf cdb-spec
           "SELECT display_text as \"displayText\",
                   identifier,
                   \"type\" as \"questionType\",
                   form_id as \"formId\"
            FROM question WHERE id='%s'"
           id)))

(defmethod handle-event "questionUpdated"
  [cdb-spec entity-store {:keys [payload offset] :as event}]
  (let [new-question (get payload "entity")
        id (get new-question "id")
        type (get new-question "questionType")
        display-text (get new-question "displayText")
        identifier (get new-question "identifier" "")
        existing-question (get-question cdb-spec id)
        existing-type (get existing-question "questionType")
        existing-display-text (get existing-question "displayText")
        existing-identifier (get existing-question "identifier" "")]
    (ensure existing-question "No such question" {:event event
                                                  :org-id (:org-id cdb-spec)})
    (queryf cdb-spec
            "UPDATE question SET display_text='%s', identifier='%s', type='%s' WHERE id='%s'"
            (escape-str display-text)
            identifier
            type
            id)
    (when (or (not= display-text existing-display-text)
              (not= identifier existing-identifier))
      (queryf cdb-spec
              "ALTER TABLE IF EXISTS %s RENAME COLUMN %s TO %s"
              (raw-data-table-name (get new-question "formId"))
              (question-column-name id existing-identifier existing-display-text)
              (question-column-name id identifier display-text)))
    (when (not= type existing-type)
      (queryf cdb-spec
              "ALTER TABLE IF EXISTS %s ALTER COLUMN %s TYPE %s USING NULL"
              (raw-data-table-name (get new-question "formId"))
              (question-column-name cdb-spec id)
              (question-type->db-type type)))))

(defmethod handle-event "questionDeleted"
  [cdb-spec entity-store {:keys [payload offset] :as event}]
  (let [id (get-in payload ["entity" "id"])
        question (get-question cdb-spec id)]
    (queryf cdb-spec
            "ALTER TABLE IF EXISTS %s DROP COLUMN IF EXISTS %s"
            (raw-data-table-name (get question "formId"))
            (question-column-name cdb-spec id))
    (queryf cdb-spec
            "DELETE FROM question WHERE id=%s"
            id)))

(defn get-location [cdb-spec data-point-id]
  (ensure (integer? data-point-id) "Invalid data-point-id" {:org-id (:org-id cdb-spec)
                                                            :data-point-id data-point-id})
  (first (queryf cdb-spec
                 "SELECT lat, lon FROM data_point WHERE id=%s"
                 data-point-id)))

(defmethod handle-event "formInstanceCreated"
  [cdb-spec entity-store {:keys [payload offset]}]
  (let [form-instance (get payload "entity")
        data-point-id (get form-instance "dataPointId")
        {:strs [lat lon]} (when data-point-id (get-location cdb-spec data-point-id))]
    ;; TODO Figure out why why akvo_update_the_geom trigger doesn't work
    (queryf cdb-spec
            "INSERT INTO %s (id, data_point_id, the_geom, lat, lon) VALUES (%s, %s, %s, %s, %s)"
            (raw-data-table-name (get form-instance "formId"))
            (get form-instance "id")
            (get form-instance "dataPointId" "NULL")
            (if (and lat lon)
              (format "ST_SetSRID(ST_Point(%s, %s),4326)" lon lat)
              "NULL")
            (or lat "NULL")
            (or lon "NULL"))
    (es/set-entity entity-store form-instance)
    #_(queryf cdb-spec
              "INSERT INTO %s (id, data_point_id, lat, lon) VALUES (%s, %s, %s, %s)"
              (raw-data-table-name (get form-instance "formId"))
              (get form-instance "id")
              (get form-instance "dataPointId" "NULL")
              (or lat "NULL")
              (or lon "NULL"))))

(defmethod handle-event "formInstanceUpdated"
  [cdb-spec entity-store {:keys [payload offset]}]
  (let [form-instance (get payload "entity")
        data-point-id (get form-instance "dataPointId")
        {:strs [lat lon]} (when data-point-id (get-location cdb-spec data-point-id))]
    ;; TODO Figure out why why akvo_update_the_geom trigger doesn't work
    (queryf cdb-spec
            "UPDATE %s SET data_point_id=%s, the_geom=%s, lat=%s, lon=%s WHERE id=%s"
            (raw-data-table-name (get form-instance "formId"))
            (get form-instance "dataPointId" "NULL")
            (if (and lat lon)
              (format "ST_SetSRID(ST_Point(%s, %s),4326)" lon lat)
              "NULL")
            (or lat "NULL")
            (or lon "NULL")
            (get form-instance "id"))
    (es/set-entity entity-store form-instance)
    #_(queryf cdb-spec
              "UPDATE %s SET data_point_id=%s, lat=%s, lon=%s WHERE id=%s"
              (raw-data-table-name (get form-instance "formId"))
              (get form-instance "dataPointId" "NULL")
              (or lat "NULL")
              (or lon "NULL")
              (get form-instance "id"))))

(defmethod handle-event "formInstanceDeleted"
  [cdb-spec entity-store {:keys [payload offset]}]
  (let [id (get-in payload ["entity" "id"])]
    (when-let [form-instance (es/get-entity entity-store "FORM_INSTANCE" id)]
      (let [form-id (get form-instance "formId")]
        (queryf cdb-spec
                "DELETE FROM %s WHERE id=%s"
                (raw-data-table-name form-id)
                id)
        (es/delete-entity entity-store "FORM_INSTANCE" id)))))

(defmethod handle-event "dataPointCreated"
  [cdb-spec entity-store {:keys [payload offset]}]
  (let [data-point (get payload "entity")
        {:strs [lat lon]} data-point]
    (queryf cdb-spec
            "INSERT INTO data_point (id, the_geom, lat, lon, survey_id, name, identifier) VALUES
                 (%s, %s, %s, %s, %s, '%s', '%s')"
            (get data-point "id")
            (if (and lat lon)
              (format "ST_SetSRID(ST_Point(%s, %s),4326)" lon lat)
              "NULL")
            lat
            lon
            (get data-point "surveyId")
            (get data-point "name")
            (get data-point "identifier"))))

(defmethod handle-event "dataPointUpdated"
  [cdb-spec entity-store {:keys [payload offset]}]
  (let [data-point (get payload "entity")
        {:strs [lat lon]} data-point]
    (queryf cdb-spec
            "UPDATE data_point SET the_geom=%s, lat=%s, lon=%s, survey_id=%s, name='%s', identifier='%s' WHERE id=%s"
            (if (and lat lon)
              (format "ST_SetSRID(ST_Point(%s, %s),4326)" lon lat)
              "NULL")
            lat
            lon
            (get data-point "surveyId")
            (get data-point "name")
            (get data-point "identifier")
            (get data-point "id"))))

(defmethod handle-event "dataPointDeleted"
  [cdb-spec entity-store {:keys [payload offset]}]
  (let [data-point-id (get-in payload ["entity" "id"])]
    (queryf cdb-spec
            "DELETE FROM data_point WHERE id=%s"
            data-point-id)))

(defn answer-upsert [cdb-spec entity-store {:keys [payload]}]
  (let [answer (get payload "entity")]
    (queryf cdb-spec
            "UPDATE %s SET %s=%s WHERE id=%s"
            (raw-data-table-name (get answer "formId"))
            (question-column-name cdb-spec (get answer "questionId"))
            (format "'%s'" (escape-str (get answer "value")))
            (get answer "formInstanceId"))
    (es/set-entity entity-store answer)))

(defmethod handle-event "answerCreated"
  [cdb-spec entity-store event]
  (answer-upsert cdb-spec entity-store event))

(defmethod handle-event "answerUpdated"
  [cdb-spec entity-store event]
  (answer-upsert cdb-spec entity-store event))

(defmethod handle-event "answerDeleted"
  [cdb-spec entity-store {:keys [payload]}]
  (let [id (get-in payload ["entity" "id"])]
    (when-let [answer (es/get-entity entity-store "ANSWER" id)]
      (queryf cdb-spec
              "UPDATE %s SET %s=NULL WHERE id=%s"
              (raw-data-table-name (get answer "formId"))
              (question-column-name cdb-spec (get answer "questionId"))
              (get answer "formInstanceId"))
      (es/delete-entity entity-store "ANSWER" id))))

(defn delete-all-raw-data-tables [cdb-spec]
  (when-let [tables (->> (queryf cdb-spec "SELECT tablename FROM pg_tables")
                         (map #(get % "tablename"))
                         (filter #(.startsWith % "raw_data_"))
                         seq)]
    (queryf cdb-spec "DROP TABLE IF EXISTS %s;" (string/join "," tables))))

(defn get-offset [cdb-spec org-id]
  (let [offset (-> (queryf cdb-spec
                       "SELECT event_offset FROM event_offset WHERE org_id='%s'"
                       org-id)
                   first
                   (get "event_offset"))]
    (if (nil? offset)
      (do (queryf cdb-spec
                  "INSERT INTO event_offset (org_id, event_offset) VALUES ('%s', 0)"
                  org-id)
          0)
      offset)))

(defn wrap-update-offset [cdb-spec org-id entity-store event-handler]
  (fn [event]
    (try
      (event-handler cdb-spec entity-store event)
      (queryf cdb-spec
              "UPDATE event_offset SET event_offset=%s WHERE org_id='%s'"
              (:offset event)
              (:org-id cdb-spec))
      (catch Exception e
        (timbre/error e
                      (format "Could not handle event for %s: %s"
                              (:org-id cdb-spec)
                              (pr-str event)))))))

(defn cartodb-entity-store [cdb-spec]
  ;; (queryf cdb-spec create-entity-store-sql)
  (reify es/IEntityStore
      (-get [_ entity-type id]
        (-> (queryf cdb-spec
                "SELECT entity FROM entity_store WHERE id=%s AND entity_type='%s'"
                id
                entity-type)
            first
            (get "entity")
            edn/read-string))
      (-set [_ entity-type id entity]
        ;; TODO postgresql 9.5 will have better upsert support
        (try (queryf cdb-spec
                     "INSERT INTO entity_store VALUES ('%s', %s, '%s');"
                     entity-type
                     id
                     (pr-str entity))
             (catch Exception e
               (queryf cdb-spec
                       "UPDATE entity_store SET entity='%s' WHERE id=%s AND entity_type='%s';"
                       (pr-str entity)
                       id
                       entity-type))))
      (-del [_ entity-type id]
        (queryf cdb-spec
                "DELETE FROM entity_store WHERE id=%s AND entity_type='%s'"
                id
                entity-type))))

(defn start [org-id config event-handler]
  (let [db-spec (pg/event-log-spec config org-id)
        cdb-spec (cartodb-spec config org-id)
        entity-store (es/cached-entity-store
                      (cartodb-entity-store cdb-spec)
                      1e5)
        offset (get-offset cdb-spec org-id)
        {:keys [chan close!] :as events} (pg/event-chan* db-spec offset)
        event-handler (wrap-update-offset cdb-spec
                                          org-id
                                          entity-store
                                          event-handler)]
    (async/thread
      (loop []
          (when-let [event (async/<!! chan)]
            (event-handler event)
            (recur))))
    close!))

(defn clear-tables [cdb-spec]
  (queryf cdb-spec "DELETE FROM event_offset")
  (queryf cdb-spec "DELETE FROM question")
  (queryf cdb-spec "DELETE FROM survey")
  (queryf cdb-spec "DELETE FROM form")
  (queryf cdb-spec "DELETE FROM data_point")
  (queryf cdb-spec "DELETE FROM entity_store")
  (delete-all-raw-data-tables cdb-spec))

(defn consumer [org-id config]
  (let [cdb-spec (cartodb-spec config org-id)]
    (setup-tables cdb-spec)
    (let [stop-fn (atom nil)]
      (reify consumer/IConsumer
        (-start [consumer]
          (timbre/infof "Starting cartodb consumer for %s with config %s" org-id config)
          (reset! stop-fn (start org-id config handle-event)))
        (-stop [consumer]
          (when-let [stop @stop-fn]
            (timbre/infof "Stopping cartodb consumer for %s with config %s" org-id config)
            (stop)))
        (-reset [consumer]
          ;; TODO the consumer should not be running, so perhaps
          ;; (-stop consumer)?
          (clear-tables cdb-spec))
        (-offset [consumer]
          (queryf cdb-spec "SELECT * FFOM event_offset"))))))