(ns me.tonsky.persistent-sorted-set.bench-async
  (:require
   [promesa.core :as p]
   [me.tonsky.persistent-sorted-set :as set]
   [me.tonsky.persistent-sorted-set.bench.core :as bench.core]
   [me.tonsky.persistent-sorted-set.test.storage :as storage]))

(def ints-10K
  (vec (shuffle (range 10000))))

(def set-10K
  (into (set/sorted-set) ints-10K))

(def ints-50K
  (vec (shuffle (range 50000))))

(def set-50K
  (into (set/sorted-set) ints-50K))

(def ints-300K
  (vec (shuffle (range 300000))))

(def set-300K
  (into (set/sorted-set) ints-300K))

(def storage-300K
  (storage/storage))

(def address-300K-set
  (into (set/sorted-set) ints-300K))

(def address-300K
  (set/store address-300K-set storage-300K))

(defn conj-10K []
  (reduce conj (set/sorted-set) ints-10K))

(defn disj-10K []
  (reduce disj set-10K ints-10K))

(defn contains-10K []
  (doseq [x ints-10K]
    (contains? set-10K x)))

(defn doseq-300K []
  (let [*res (volatile! 0)]
    (doseq [x set-300K]
      (vswap! *res + x))
    @*res))

(defn next-300K []
  (loop [xs  set-300K
         res 0]
    (if-some [x (first xs)]
      (recur (next xs) (+ res x))
      res)))

(defn reduce-300K []
  (reduce + 0 set-300K))

(defn into-50K []
  (into (set/sorted-set) ints-50K))

(defn store-50K []
  (set/store
   (into (set/sorted-set) ints-50K)
   (storage/storage)))

(defn reduce-300K-lazy []
  (reset! (:*memory storage-300K) {})
  (reduce + 0 (set/restore address-300K storage-300K {:set-metadata (set/set-metadata address-300K-set)})))

(def benches
  {"conj-10K"         conj-10K
   "disj-10K"         disj-10K
   "contains-10K"     contains-10K
   "doseq-300K"       doseq-300K
   "next-300K"        next-300K
   "reduce-300K"      reduce-300K
   "into-50K"         into-50K
   "store-50K"        store-50K
   "reduce-300K-lazy" reduce-300K-lazy
   })

(defn ^:export -main [& args]
  (let [names    (or (not-empty args) (sort (keys benches)))
        _        (apply println "CLJS:" names)
        longest  (last (sort-by count names))]
    (p/doseq [name names]
      (let [f (benches name)]
        (if (nil? f)
          (println "Unknown benchmark:" name)
          (p/let [{:keys [mean-ms]} (bench.core/bench-async (f))]
            (println
             (bench.core/right-pad name (count longest))
             " "
             (bench.core/left-pad (bench.core/round mean-ms) 6) "ms/op")))))))
