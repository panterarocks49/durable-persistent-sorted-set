(ns user
  (:require
   [me.tonsky.persistent-sorted-set.storage :refer [IStorage]]
   [me.tonsky.persistent-sorted-set.arrays :as arrays]
   [me.tonsky.persistent-sorted-set :as pss]))

;; In-memory storage implementation
(deftype MemoryStorage [storage]
  IStorage
  (restore [_ address]
    (prn "RESTORE" address)
    (let [{:as data :keys [keys addresses]} (get @storage address)]
      (prn data)
      (if addresses
        (pss/Node. keys (arrays/make-array (arrays/alength addresses)) addresses)
        (pss/Leaf. keys))))

  (accessed [_ _address]
    nil) ; No-op for memory storage

  (store [_ node]
    (let [address (str (random-uuid))
          data    (cond-> {:keys (.-keys node)}
                    (instance? pss/Node node)
                    (assoc :addresses (.-_addresses node)))]
      (prn "STORE")
      (prn address)
      (prn data)
      (swap! storage assoc address data)
      address)))

(defn memory-storage []
  (MemoryStorage. (atom {})))


;; Main function for shadow-cljs
(defn main [& args]
  (println "Persistent Sorted Set REPL started")
  (println "Try running (test-persistent-set) to test the storage protocol")
  )

(def cmp #(compare %2 %1))
(def storage (memory-storage))

;; Run this in the REPL
(comment
  (def s (pss/from-sequential
          cmp
          (range 0 10)
          {:storage storage}))

  (.-keys (pss/-root s))

  (def s2 (reduce
           (fn [acc x]
             (conj acc x))
           s
           (range 0 1000)))

  (.-keys (pss/-root s2))

  (disj s 36)

  (reduce
   (fn [acc x]
     (disj acc x))
   s
   (range 0 1000))

  (pss/store s)

  (pss/store (disj s 36))

  (prn)


  ;; TODO: in this case we can see that we are restoring nodes which already were stored
  ;; I think there is a case which recreates the same node and we overwrite the address
  (def s (pss/from-sequential
          cmp
          (range 0 256)
          {:storage storage}))

  (pss/store s)

  (prn)

  (def s-restored (pss/restore-by
                   cmp
                   (pss/store s)
                   storage
                   {:set-metadata (pss/set-metadata s)}))

  (doseq [x s-restored]
    (prn x))

  (doseq [x s]
    (prn x))

  (pss/store (disj s 100))

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

  (pss/walk-addresses
   s
   (fn [address]
     (prn address)
     nil))

  )
