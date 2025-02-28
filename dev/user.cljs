(ns user
  (:require
   [me.tonsky.persistent-sorted-set.storage :refer [IStorage]]
   [me.tonsky.persistent-sorted-set :as pss]))

;; In-memory storage implementation
(deftype MemoryStorage [storage]
  IStorage
  (restore [_ address]
    (get @storage address))

  (accessed [_ _address]
    nil) ; No-op for memory storage

  (store [_ node]
    (prn "STORE" node)
    (prn (.-keys node))
    (let [address (str (random-uuid))]
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

  ;; TODO: in this case we can see that we are restoring nodes which already were stored
  ;; I think there is a case which recreates the same node and we overwrite the address
  (def s (pss/from-sequential
          #(compare %2 %1)
          (range 0 256)
          {:storage storage}))

  (pss/walk-addresses s (fn [address]
                          (prn address)
                          nil))

  (pss/store s)

  (prn)

  (let [x (reduce
           (fn [s x]
             (let [s  (disj s x)]
               ;; (prn (.-keys (pss/-root s)))
               s))
           s
           (range 190 220))]
    (pss/store x))

  (doseq [x s]
    (prn x))

  )
