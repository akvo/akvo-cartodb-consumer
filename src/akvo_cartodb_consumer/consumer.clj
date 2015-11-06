(ns akvo-cartodb-consumer.consumer)

(defprotocol IConsumer
  (-start [consumer])
  (-stop [consumer])
  (-reset [consumer])
  (-offset [consumer]))

(defn start
  "Start the consumer"
  [consumer]
  (-start consumer))


(defn stop
  "Stop the consumer"
  [consumer]
  (-stop consumer))

(defn offset [consumer]
  (-offset consumer))

(defn restart [consumer]
  (stop consumer)
  (-reset consumer)
  (start consumer))
