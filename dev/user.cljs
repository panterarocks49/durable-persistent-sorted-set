(ns user
  (:require [me.tonsky.persistent-sorted-set :as pss]))

;; In-memory storage implementation
(deftype MemoryStorage [storage]
  pss/IStorage
  (restore [_ address]
    (get @storage address))

  (accessed [_ _address]
    nil) ; No-op for memory storage

  (store [_ node]
    (let [address (random-uuid)]
      (swap! storage assoc address node)
      address)))

(defn memory-storage []
  (MemoryStorage. (atom {})))


;; Main function for shadow-cljs
(defn main [& args]
  (println "Persistent Sorted Set REPL started")
  (println "Try running (test-persistent-set) to test the storage protocol")
  )

;; Run this in the REPL
(comment
  (def storage (memory-storage))

  (def s (pss/sorted-set 1 2 3))

  (conj s 4)

  (doseq [x s]
    (prn x))

  )
