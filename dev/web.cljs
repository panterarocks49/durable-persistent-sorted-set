(ns web
  (:require
   ["idb" :as idb]
   [promesa.core :as p]
   [me.tonsky.persistent-sorted-set.storage :refer [IStorage]]
   [me.tonsky.persistent-sorted-set.arrays :as arrays]
   [me.tonsky.persistent-sorted-set-async :as pss]))

(deftype IDBStorage [storage]
  IStorage
  (restore [_ address]
    (p/do
      (prn "RESTORE" address)
      (let [{:as data :keys [keys addresses]} (get @storage address)]
        ;; (prn data)
        (if addresses
          (pss/Node. keys (arrays/make-array (arrays/alength addresses)) addresses)
          (pss/Leaf. keys)))))

  (accessed [_ _address]
    nil) ; No-op for memory storage

  (store [_ node]
    (p/let [address (str (random-uuid))
            data    (cond-> {:keys (.-keys node)}
                      (instance? pss/Node node)
                      (assoc :addresses (.-_addresses node)))]
      (prn "STORE" address)
      ;; (prn data)
      (swap! storage assoc address data)
      address)))

(defn idb-storage []
  (IDBStorage. (atom {})))

(def cmp compare #_(compare %2 %1))
(def storage (idb-storage))

(-next (seq [1 2 3]))

(comment
  (def s (pss/from-sequential
          cmp
          (range 0 10000)
          {:storage storage}))

  (-> (pss/seek s 100)
      (p/then (fn [v]
                (prn v))))

  (-> (pss/slice s 100 700)
      (p/then (fn [v]
                (prn v))))

  (-> (pss/rslice s 700 100)
      (p/catch (fn [e] (js/console.error e)))
      (p/then (fn [v]
                (prn v))))

  (-> (reduce
       (fn [acc x]
         (prn x)
         acc)
       []
       s)
      (p/catch (fn [e]
                 (js/console.error e))))

  (mapv (fn [x] x) s)

  (-> (pss/store s)
      (p/then prn))

  (disj s 36)

  (conj s 10)



  (def s-restored (pss/restore-by
                   cmp
                   (pss/store s)
                   storage
                   {:set-metadata (pss/set-metadata s)}))


  )






;; start is called by init and after code reloading finishes
(defn ^:dev/after-load start []
  (js/console.log "start"))

(defn init []
  ;; init is called ONCE when the page loads
  ;; this is called in the index.html and must be exported
  ;; so it is available even in :advanced release builds
  (js/console.log "init")
  (start))

;; this is called before any code is reloaded
(defn ^:dev/before-load stop []
  (js/console.log "stop"))

