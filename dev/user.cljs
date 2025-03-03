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
      ;; (prn data)
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
      ;; (prn data)
      (swap! storage assoc address data)
      address)))

(defn memory-storage []
  (MemoryStorage. (atom {})))


;; Main function for shadow-cljs
(defn main [& args]
  (println "Persistent Sorted Set REPL started")
  (println "Try running (test-persistent-set) to test the storage protocol")
  )

(def cmp compare #_(compare %2 %1))
(def storage (memory-storage))

;; Run this in the REPL
(comment
  (def s (pss/from-sequential
          cmp
          (range 0 10000)
          {:storage storage}))

  (def s2 (reduce
           (fn [acc x]
             (conj acc x))
           s
           (range 0 100000)))

  (def s3 (pss/from-sequential
           cmp
           (range 0 7000000)
           {:storage storage}))

  (doseq [x s3]
    #_(prn x))

  (disj s 36)

  (reduce
   (fn [acc x]
     (disj acc x))
   s
   (range 0 1000))

  (pss/store s3)

  (prn)

  (def s-restored (pss/restore-by
                   cmp
                   (pss/store s)
                   storage
                   {:set-metadata (pss/set-metadata s)}))

  (doseq [x s-restored]
    #_(prn x))

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
