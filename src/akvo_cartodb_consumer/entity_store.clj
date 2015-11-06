(ns akvo-cartodb-consumer.entity-store
  (:require [clojure.core.cache :as cache]))

(defprotocol IEntityStore
  (-get [es entity-type id] "Get by entity-type/id pair")
  (-set [es entity-type id val] "Set val")
  (-del [es entity-type id] "Remove entity at entity-type/id"))

(defn get-entity [es kind id]
  {:pre [(integer? id)
         (string? kind)]}
  (-get es kind id))

(defn set-entity [es val]
  {:pre [(map? val)
         (string? (get val "type"))
         (integer? (get val "id"))]}
  (-set es (get val "type") (get val "id") val))

(defn delete-entity [es kind id]
  {:pre [(string? kind)
         (integer? id)]}
  (-del es kind id))

(defn cached-entity-store [backing-store threshold]
  (let [in-memory-cache (atom (cache/lru-cache-factory {} :threshold threshold))]
    (reify IEntityStore
      (-get [es entity-type id]
        (let [cache @in-memory-cache]
          (if (cache/has? cache [entity-type id])
            (do (swap! in-memory-cache cache/hit [entity-type id])
                (cache/lookup cache [entity-type id]))
            (let [entity (-get backing-store entity-type id)
                  {:strs [entityType id]} entity]
              (swap! in-memory-cache cache/miss [entityType id] entity)
              entity))))

      (-set [es entity-type id entity]
        (let [cache @in-memory-cache]
          (when (cache/has? cache [entity-type id])
            (swap! in-memory-cache cache/evict [entity-type id]))
          (-set backing-store entity-type id entity)
          (swap! in-memory-cache cache/miss [entity-type id] entity)))

      (-del [es entity-type id]
        (let [cache @in-memory-cache]
          (when (cache/has? cache [entity-type id])
            (swap! in-memory-cache cache/evict [entity-type id]))
          (-del backing-store entity-type id))))))
