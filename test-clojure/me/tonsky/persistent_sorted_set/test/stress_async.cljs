(ns me.tonsky.persistent-sorted-set.test.stress-async
  (:require
   [promesa.core :as p]
   [me.tonsky.persistent-sorted-set-async :as set]
   [me.tonsky.persistent-sorted-set.test.storage-async :as storage]
   [clojure.test :as t :refer [is are async deftest testing]])
  (:require-macros
   [me.tonsky.persistent-sorted-set.test.macros :refer [deftest-async]]))

(def iters 100)

(defn into-via-doseq [to from]
  (let [res (transient [])]
    (doseq [x from]  ;; checking chunked iter
      (conj! res x))
    (persistent! res)))

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

(deftest stresstest-btset
  (println "  testing stresstest-btset...")
  (async done
    (-> (p/run!
         (fn [i]
           (let [size      10000
                 xs        (vec (repeatedly (+ 1 (rand-int size)) #(rand-int size)))
                 xs-sorted (vec (distinct (sort xs)))
                 rm        (vec (repeatedly (rand-int (* size 5)) #(rand-nth xs)))
                 full-rm   (shuffle (concat xs rm))
                 xs-rm     (reduce disj (into (sorted-set) xs) rm)]
             (p/run!
              (fn [[method set0]]
                (p/let [set0 set0
                        ;; I think thjs is working bc the storage is sync?
                        ;; maybe use async reduce here
                        set1 (reduce disj set0 rm)
                        set2 (reduce disj set0 rm)
                        set3 (reduce disj set0 full-rm)
                        set4 (reduce disj set0 full-rm)]
                  (testing
                      (str "Iter:" (inc i)  "/" iters
                           "set:" method
                           "adds:" (str (count xs) " (" (count xs-sorted) " distinct),")
                           "removals:" (str (count rm) " (down to " (count xs-rm) ")"))
                    (testing "conj, seq"
                      (is (= (vec set0) xs-sorted)))
                    (testing "eq"
                      (is (= set0 (set xs-sorted))))
                    (testing "count"
                      (is (= (count set0) (count xs-sorted))))
                    (testing "doseq"
                      (is (= (into-via-doseq [] set0) xs-sorted)))
                    (testing "disj"
                      (is (= (vec set1) (vec xs-rm)))
                      (is (= (count set1) (count xs-rm)))
                      (is (= set1 xs-rm)))
                    (testing "disj transient"
                      (is (= (vec set2) (vec xs-rm)))
                      (is (= (count set2) (count xs-rm)))
                      (is (= set2 xs-rm)))
                    (testing "full disj"
                      (is (= set3 #{}))
                      (is (= set4 #{}))))))
              [["conj" (into (set/sorted-set) xs)]
               ["bulk" (apply set/sorted-set xs)]
               ["lazy" (storage/roundtrip (into (set/sorted-set) xs))]])))
         (range 0 iters))
        (p/then #(done))
        (p/catch (fn [e]
                   (js/console.error e)
                   (done))))))

(deftest stresstest-slice
  (println "  testing stresstest-slice...")
  (async done
    (-> (p/run!
         (fn [i]
           (let [xs        (repeatedly (+ 1 (rand-int 20000)) #(rand-int 20000))
                 xs-sorted (distinct (sort xs))
                 [from to] (sort [(- 10000 (rand-int 20000)) (+ 10000 (rand-int 20000))])
                 expected  (filter #(<= from % to) xs-sorted)]
             (p/run!
              (fn [[method set]]
                (p/let [set set
                        set-range (set/slice set from to)
                        s (into (set/sorted-set) (shuffle xs-sorted))
                        x (set/rslice s 30000 -10)
                        y (set/rslice s 30000 -10)]
                  (testing
                      (str
                       "Iter: " (inc i) "/" iters
                       ", set:" method
                       ", from:" from " " (count xs-sorted) " elements"
                       ", down to:" to " " (count expected))
                    (is (= x (-> y rseq reverse)))
                    (is (= (vec set-range) (vec (seq set-range)))) ;; checking IReduce on BTSetIter
                    (is (= (vec set-range) expected))
                    (is (= (into-via-doseq [] set-range) expected))
                    (is (= (vec (rseq set-range)) (reverse expected)))
                    (is (= (vec (rseq (rseq set-range))) expected)))))
              [["conj" (into (set/sorted-set) xs)]
               ["lazy" (storage/roundtrip (into (set/sorted-set) xs))]])))
         (range 0 iters))
        (p/then #(done))
        (p/catch (fn [e]
                   (js/console.error e)
                   (done))))))

(deftest stresstest-rslice
  (println "  testing stresstest-rslice...")
  (dotimes [i 1000]
    (let [len 3000
          xs  (vec (shuffle (range 0 (inc 3000))))
          s   (into (set/sorted-set) xs)]
      (testing (str "Iter: " i "/1000")
        (is (= 
             (set/rslice s (+ 3000 100) -100)
             (-> (set/rslice s (+ 3000 100) -100) rseq reverse)))))))

;; TODO: fix seek?
#_
(deftest stresstest-seek
  (println "  testing stresstest-seek...")
  (dotimes [i iters]
    (let [xs        (repeatedly (inc (rand-int 20000)) #(rand-int 20000))
          xs-sorted (distinct (sort xs))
          seek-to   (rand-int 20000)
          set       (into (set/sorted-set) xs-sorted)]
      (testing (str "Iter: " i "/" iters ", seek to " seek-to)
        (is (= (seq (drop-while #(< % seek-to) xs-sorted))
               (set/seek (seq set) seek-to)))
        
        (is (= (seq (drop-while #(< % 19999) xs-sorted))
               (set/seek (seq set) 19999)))
        
        (is (= (seq (reverse (take-while #(<= % seek-to) xs-sorted)))
               (set/seek (rseq set) seek-to)))
        
        (is (= (seq (reverse (take-while #(<= % 1) xs-sorted)))
               (set/seek (rseq set) 1)))))))

(deftest test-overflow
  (println "  testing test-overflow...")
  (let [len  4000000
        part (quot len 100)
        xss  (partition-all part (shuffle (range 0 len)))
        s    (reduce into (set/sorted-set) xss)]
    (is (= 10 (count (take 10 s))))))
