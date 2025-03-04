(ns me.tonsky.persistent-sorted-set.test.storage
  (:require
   [clojure.edn :as edn]
   [clojure.test :as t :refer [is are deftest testing]]
   [me.tonsky.persistent-sorted-set.storage :refer [IStorage]]
   [me.tonsky.persistent-sorted-set.arrays :as arrays]
   [me.tonsky.persistent-sorted-set :as set])
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
  (store [_ node]
    (swap! *stats update :writes inc)
    (let [address (gen-addr)]
      (swap! *disk assoc address
             (pr-str
              {:keys      (vec (.-keys node))
               :addresses (when (instance? set/Node node)
                            (vec (.-_addresses node)))}))
      address))
  (accessed [_ _address]
    (swap! *stats update :accessed inc)
    nil)
  (restore [_ address]
    (or
     (@*memory address)
     (let [{:keys [keys
                   addresses]} (edn/read-string (@*disk address))
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
  (let [storage (storage)
        address (set/store set storage)]
    (set/restore address storage {:set-metadata (set/set-metadata set)})))

(defn loaded-ratio
  ([^set/BTSet set]
   (let [storage (.-_storage set)
         address (.-_address set)
         root    (set/-root set)]
     (loaded-ratio (some-> storage :*memory deref) address root)))
  ([memory address node]
   (when *debug*
     (println address (contains? memory address) node (memory address)))
   (if (and address (not (contains? memory address)))
     0.0
     (let [node (set/read-reference node)
           node (or node (memory address))]
       (if (instance? set/Leaf node)
         1.0
         (let [node ^set/Node node
               children (.-pointers node)
               len      (count children)]
           (double
            (/ (->> (mapv
                     (fn [_ child-addr child]
                       (loaded-ratio memory child-addr child))
                     (range len)
                     (or (.-_addresses node) (repeat len nil))
                     (or children (repeat len nil)))
                    (reduce + 0))
               len))))))))

(defn durable-ratio
  ([^set/BTSet set]
   (double (durable-ratio (.-_address set) (set/-root set))))
  ([address node]
   (cond
     (some? address)           1.0
     (instance? set/Leaf node) 0.0
     :else
     (let [node     ^set/Node node
           children (.-pointers node)
           len      (count children)]
       (/ (->> (map
                (fn [_ child-addr child]
                  (durable-ratio child-addr child))
                (range len)
                (.-_addresses node)
                children)
               (reduce + 0))
          len)))))

(deftest test-lazy-remove
  "Check that invalidating middle branch does not invalidates siblings"
  (let [size 7000
        xs   (shuffle (range size))
        set  (into (set/sorted-set* {}) xs)]
    (set/store set (storage))
    (is (= 1.0 (durable-ratio set))
        (let [set' (disj set 3500)] ;; one of the middle branches
          (is (< 0.87 (durable-ratio set')))))))

(deftest stresstest-stable-addresses
  (let [size      10000
        adds      (shuffle (range size))
        removes   (shuffle adds)
        *set      (atom (set/sorted-set))
        *disk     (atom {})
        storage   (storage *disk)
        invariant (fn invariant
                    ([^set/BTSet o]
                     (invariant (set/-root o) (some? (.-_address o))))
                    ([o stored?]
                     (condp instance? o
                       set/Node
                       (let [node ^set/Node o
                             len (arrays/alength (.-pointers node))]
                         (doseq [i (range len)
                                 :let [addr   (nth (.-_addresses node) i)
                                       child  (set/node-child node (int i) storage)
                                       {:keys [keys addresses]} (edn/read-string (@*disk addr))]]
                           ;; nodes inside stored? has to ALL be stored
                           (when stored?
                             (is (some? addr)))
                           (when (some? addr)
                             (is (= keys (vec (.-keys child))))
                             (is (= addresses
                                    (when (instance? set/Node child)
                                      (vec (.-_addresses child))))))
                           (invariant child (some? addr))))
                       set/Leaf
                       true)))]
    (testing "Persist after each"
      (dobatches [xs adds]
                 (let [set' (swap! *set into xs)]
                   (invariant set')
                   (set/store set' storage)))
      (invariant @*set)
      (dobatches [xs removes]
                 (let [set' (swap! *set #(reduce disj % xs))]
                   (invariant set')
                   (set/store set' storage))))
    (testing "Persist once"
      (reset! *set (into (set/sorted-set) adds))
      (set/store @*set storage)
      (dobatches [xs removes]
                 (let [set' (swap! *set #(reduce disj % xs))]
                   (invariant set'))))))

(deftest test-walk
  (let [size    1000000
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
    (let [set'     (conj set (* 2 size))
          *stored' (atom 0)]
      (set/walk-addresses set'
                          (fn [addr]
                            (when (some? addr)
                              (swap! *stored' inc))))
      ;; 3 is the depth of brnaching-factor 1024
      (is (= (- @*stored 3) @*stored')))))

(deftest test-lazyness
  (let [size       1000000
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
        _       (is (= (take 100 loaded) (take 100 original)))
        ;; the number of reads the cljs version does is more than clj
        ;; because we get the rightmost path from storage
        ;; clj doesn't do this for whatever reason
        _       (is (<= 5 (:reads @*stats) 7))
        l100    (loaded-ratio loaded)
        _       (is (< 0 l100 1.0))
        ;; touch first 5000
        _       (is (= (take 5000 loaded) (take 5000 original)))
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
        _       (is (= (take 100 (rseq loaded)) (take 100 (rseq original))))
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
        _       (is (= 1.0 l0))]))
