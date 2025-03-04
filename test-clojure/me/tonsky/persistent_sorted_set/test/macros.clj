(ns me.tonsky.persistent-sorted-set.test.macros)

(defmacro with-stats [& body]
  `(do
     (reset! ~'*stats {:reads 0 :writes 0 :accessed 0})
     ~@body))

(defmacro dobatches [[sym coll] & body]
  `(loop [coll# ~coll]
     (when (seq coll#)
       (let [batch# (rand-nth [1 2 3 4 5 10 20 30 40 50 100])
             [~sym tail#] (split-at batch# coll#)]
         ~@body
         (recur tail#)))))
