(ns me.tonsky.persistent-sorted-set.test.storage-async
  (:require
   [promesa.core :as p]
   [clojure.edn :as edn]
   [clojure.test :as t :refer [is are async deftest testing]]
   [me.tonsky.persistent-sorted-set.storage :refer [IStorage]]
   [me.tonsky.persistent-sorted-set.arrays :as arrays]
   [me.tonsky.persistent-sorted-set-async :as set])
  (:require-macros
   [me.tonsky.persistent-sorted-set.test.macros :refer [dobatches with-stats]]))

(def ^:dynamic *debug*
  false)

(defn gen-addr []
  (random-uuid)
  #_(str (str/join (repeatedly 10 #(rand-nth "ABCDEFGHIJKLMNOPQRSTUVWXYZ")))))

(def *stats
  (atom
   {:reads 0
    :writes 0
    :accessed 0}))

(defrecord Storage [*memory *disk]
  IStorage
  (store [_ nodes]
    (mapv
     (fn [[_ node]]
       (swap! *stats update :writes inc)
       (let [address (gen-addr)]
         (swap! *disk assoc address
                {:keys      (vec (.-keys node))
                 :addresses (when (instance? set/Node node)
                              (vec (.-_addresses node)))})
         address)
       )
     nodes))
  (accessed [_ _address]
    (swap! *stats update :accessed inc)
    nil)
  (restore [_ address]
    (or
     (@*memory address)
     (let [{:keys [keys
                   addresses]} (@*disk address)
           node (if addresses
                  (set/Node. (into-array keys) (arrays/make-array (count addresses)) (into-array addresses))
                  (set/Leaf. (into-array keys)))]
       ;; (prn "restore" keys #_addresses)
       (swap! *stats update :reads inc)
       (swap! *memory assoc address node)
       node))))

(defn storage
  (^IStorage []
   (->Storage (atom {}) (atom {})))
  (^IStorage [*disk]
   (->Storage (atom {}) *disk))
  (^IStorage [*memory *disk]
   (->Storage *memory *disk)))

(defn roundtrip [set]
  (p/let [storage (storage)
          address (set/store set storage)]
    (set/restore address storage {:set-metadata (set/set-metadata set)})))

(defn loaded-ratio
  ([^set/BTSet set]
   (let [storage  (.-_storage set)
         address  (.-_address set)
         settings (.-_settings set)
         root     (set/-root set)]
     (loaded-ratio settings (some-> storage :*memory deref) address root)))
  ([settings memory address node]
   (when *debug*
     (println address (contains? memory address) node (memory address)))
   (if (and address (not (contains? memory address)))
     0.0
     (let [node (set/-read-ref settings node)
           node (or node (memory address))]
       (if (instance? set/Leaf node)
         1.0
         (let [node     ^set/Node node
               children (.-pointers node)
               len      (count children)]
           (double
            (/ (->> (mapv
                     (fn [_ child-addr child]
                       (loaded-ratio settings memory child-addr child))
                     (range len)
                     (or (.-_addresses node) (repeat len nil))
                     (or children (repeat len nil)))
                    (reduce + 0))
               len))))))))

(defn durable-ratio
  ([^set/BTSet set]
   (double (durable-ratio (.-_settings set) (.-_address set) (set/-root set))))
  ([settings address node]
   (let [node (set/-read-ref settings node)]
     (cond
       (some? address)           1.0
       (instance? set/Leaf node) 0.0
       :else
       (let [node     ^set/Node node
             children (.-pointers node)
             len      (count children)]
         (/ (->> (map
                  (fn [_ child-addr child]
                    (durable-ratio settings child-addr child))
                  (range len)
                  (.-_addresses node)
                  children)
                 (reduce + 0))
            len))))))

(deftest test-lazy-remove
  "Check that invalidating middle branch does not invalidates siblings"
  (async done
    (-> (p/let [size 7000
                xs   (shuffle (range size))
                set  (into (set/sorted-set* {}) xs)]
          (set/store set (storage))
          (is (= 1.0 (durable-ratio set))
              (let [set' (disj set 3500)] ;; one of the middle branches
                (is (< 0.87 (durable-ratio set'))))))
        (p/then #(done))
        (p/catch (fn [e]
                   (js/console.error e)
                   (done))))))

(defn pdobatches [f coll]
  (p/loop [coll coll]
    (when (seq coll)
      (let [batch (rand-nth [1 2 3 4 5 10 20 30 40 50 100])
            [b tail] (split-at batch coll)]
        (f b)
        (p/recur tail)))))

(defn async-reduce
  "Like reduce but `f` can return a promise"
  [f acc coll]
  (reduce
   (fn [acc v]
     (p/then
      acc
      (fn [acc]
        (f acc v))))
   acc
   coll))

(deftest stresstest-stable-addresses
  (async done
    (-> (p/let [size      10000
                adds      (shuffle (range size))
                removes   (shuffle adds)
                *set      (atom (set/sorted-set))
                *disk     (atom {})
                storage   (storage *disk)
                invariant (fn invariant
                            ([^set/BTSet o]
                             (p/let [settings (.-_settings o)
                                     root (set/-root o)]
                               (invariant settings root (some? (.-_address o)))))
                            ([settings o stored?]
                             (condp instance? o
                               set/Node
                               (p/let [node ^set/Node o
                                       len  (arrays/alength (.-pointers node))]
                                 (p/doseq [i (range len)]
                                   (p/let [addr   (nth (.-_addresses node) i)
                                           child  (set/node-child node (int i) storage settings)
                                           {:keys [keys addresses]} (edn/read-string (@*disk addr))]
                                     ;; nodes inside stored? has to ALL be stored
                                     (when stored?
                                       (is (some? addr)))
                                     (when (some? addr)
                                       (is (= keys (vec (.-keys child))))
                                       (is (= addresses
                                              (when (instance? set/Node child)
                                                (vec (.-_addresses child))))))
                                     (invariant settings child (some? addr)))))
                               set/Leaf
                               true)))]
          (testing "Persist after each"
            (p/do!
             (pdobatches
              (fn [xs]
                (p/let [s    (async-reduce conj @*set xs)
                        set' (reset! *set s)]
                  (invariant set')
                  (set/store set' storage)))
              adds)
             (invariant @*set)
             (pdobatches
              (fn [xs]
                (p/let [s    (async-reduce disj @*set xs)
                        set' (reset! *set s)]
                  (invariant set')
                  (set/store set' storage)))
              removes)))
          (testing "Persist once"
            (p/let [s (into (set/sorted-set) adds)]
              (reset! *set s)
              (set/store @*set storage)
              (pdobatches
               (fn [xs]
                 (let [s    (async-reduce disj @*set xs)
                       set' (reset! *set s)]
                   (invariant set')))
               removes))))
        (p/then #(done))
        (p/catch (fn [e]
                   (js/console.error e)
                   (done))))))

(deftest test-walk
  (async done
    (-> (p/let [size    1000000
                xs      (shuffle (range size))
                set     (into (set/sorted-set* {}) xs)
                *stored (atom 0)]
          (set/walk-addresses set
                              (fn [addr]
                                (is (nil? addr))))
          (set/store set (storage))
          (set/walk-addresses set
                              (fn [addr]
                                (is (some? addr))
                                (swap! *stored inc)))
          (p/let [set'     (conj set (* 2 size))
                  *stored' (atom 0)]
            (set/walk-addresses set'
                                (fn [addr]
                                  (when (some? addr)
                                    (swap! *stored' inc))))
            ;; 3 is the depth of brnaching-factor 1024
            (is (= (- @*stored 3) @*stored'))))
        (p/then #(done))
        (p/catch (fn [e]
                   (js/console.error e)
                   (done))))))

(deftest test-lazyness
  (async done
    (-> (p/let [size       1000000
                xs         (shuffle (range size))
                rm         (vec (repeatedly (quot size 5) #(rand-nth xs)))
                original   (-> (reduce disj (into (set/sorted-set* {:branching-factor 64}) xs) rm)
                               (disj (quot size 4) (quot size 2)))
                storage    (storage)
                address    (with-stats
                             (set/store original storage))
                _          (is (= 0 (:reads @*stats)))
                ;; _          (is (> (:writes @*stats) (/ size PersistentSortedSet/MAX_LEN)))
                loaded     (set/restore address storage {:set-metadata (set/set-metadata original)})
                _          (is (= 0 (:reads @*stats)))
                _          (is (= 0.0 (loaded-ratio loaded)))
                _          (is (= 1.0 (durable-ratio loaded)))
                ;; touch first 100
                _       (is (= (set/slice loaded 0 100) (set/slice original 0 100)))
                ;; should be 3?
                _       (is (<= 2 (:reads @*stats) 4))
                l100    (loaded-ratio loaded)
                _       (is (< 0 l100 1.0))
                ;; touch first 5000
                _       (is (= (set/slice loaded 0 5000) (set/slice original 0 5000)))
                l5000   (loaded-ratio loaded)
                _       (is (< l100 l5000 1.0))
                ;; touch middle
                from    (- (quot size 2) (quot size 200))
                to      (+ (quot size 2) (quot size 200))
                _       (is (= (vec (set/slice loaded from to))
                               (vec (set/slice loaded from to))))
                lmiddle (loaded-ratio loaded)
                _       (is (< l5000 lmiddle 1.0))
                ;; touch 100 last
                _       (is (= (set/rslice loaded size (- size 100)) (set/rslice original size (- size 100))))
                lrseq   (loaded-ratio loaded)
                ;; lrseq is already loaded because we got the end of the seq
                ;; these should be equal
                _       (is (<= lmiddle lrseq 1.0))
                ;; touch 10000 last
                from    (- size (quot size 100))
                to      size
                _       (is (= (vec (set/slice loaded from to))
                               (vec (set/slice loaded from 1000000))))
                ltail   (loaded-ratio loaded)
                _       (is (< lrseq ltail 1.0))
                ;; conj to beginning
                loaded' (conj loaded -1)
                _       (is (= ltail (loaded-ratio loaded')))
                _       (is (< (durable-ratio loaded') 1.0))
                ;; conj to middle
                loaded' (conj loaded (quot size 2))
                _       (is (= ltail (loaded-ratio loaded')))
                _       (is (< (durable-ratio loaded') 1.0))
                ;; conj to end
                loaded' (conj loaded 2147483647 #_Long/MAX_VALUE)
                _       (is (= ltail (loaded-ratio loaded')))
                _       (is (< (durable-ratio loaded') 1.0))
                ;; conj to untouched area
                loaded' (conj loaded (quot size 4))
                _       (is (< ltail (loaded-ratio loaded') 1.0))
                _       (is (< ltail (loaded-ratio loaded) 1.0))
                _       (is (< (durable-ratio loaded') 1.0))
                ;; transients conj
                xs      (range -10000 0)
                loaded' (into loaded xs)
                _       (is (every? loaded' xs))
                _       (is (< ltail (loaded-ratio loaded')))
                _       (is (< (durable-ratio loaded') 1.0))
                ;; incremental persist
                _       (with-stats
                          (set/store loaded' storage))
                _       (is (< (:writes @*stats) 350)) ;; ~ 10000 / 32 + 10000 / 32 / 32 + 1
                _       (is (= 1.0 (durable-ratio loaded')))
                ;; transient disj
                xs      (take 100 loaded)
                loaded' (reduce disj loaded xs)
                _       (is (every? #(not (loaded' %)) xs))
                _       (is (< (durable-ratio loaded') 1.0))
                ;; count does not fetch everything, count is cached
                _       (is (= (count loaded) (count original)))
                ;; this refetches everything
                _       (is (= (into [] loaded) (into [] original)))
                l0      (loaded-ratio loaded)
                _       (is (= 1.0 l0))])
        (p/then #(done))
        (p/catch (fn [e]
                   (js/console.error e)
                   (done))))))
