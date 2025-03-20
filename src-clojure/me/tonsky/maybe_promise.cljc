(ns me.tonsky.maybe-promise
  (:refer-clojure
   :exclude
   [loop recur let reduce reduce-kv
    mapv filterv every?])
  (:require
   [clojure.core :as c]
   [promesa.exec :as exec]
   [promesa.core :as p])
  #?(:cljs
     (:require-macros
      [me.tonsky.maybe-promise :refer [loop]])))

;; all of the functions in this namespace will maybe return a promise, depending on their args
;; this is useful to avoid "infecting" your whole call stack
;; with promise waits, which, when done thousands of times
;; can add up to meaningful slowdowns
;; intended to be used with an i/o operation with a cache
;; on cache hit, we take the sync (faster) route

(defn all
  [coll]
  (if (some p/promise? coll)
    (p/all coll)
    coll))

(defn then
  [p f]
  (if (p/promise? p)
    (p/then p f)
    (f p)))

;; can't figure out how to override do so it's named do!
(defmacro do!
  [& body]
  (when (seq body)
    `(-> ~(first body)
         ~@(c/mapv
            (fn [form]
              `(then (fn [] ~form)))
            (rest body)))))

(defmacro let
  "If a value in the let binding is a promise, then await on the promise
  otherwise don't wait and don't return a promise
  In the best case, the code will run sync. Useful if you may or may not have a promise
  because it's faster to not await if you don't have too
  Body is run with `do!` and may contain promises as well"
  {:style/indent 1}
  [bindings & body]
  (c/let [[n v & more] bindings
          nsym         (gensym "n-")]
    `(c/let [~nsym ~v]
       (then
        ~nsym
        (fn [~n]
          ~(if (seq more)
             `(let ~more ~@body)
             `(do! ~@body)))))))

(defn cljs-env?
  "Take the &env from a macro, and tell whether we are expanding into cljs."
  [env]
  (boolean (:ns env)))

(defmacro try-catchall
  "A cross-platform variant of try-catch that catches all exceptions.
   Does not (yet) support finally, and does not need or want an exception class."
  [& body]
  (c/let [try-body (butlast body)
          [catch sym & catch-body] (last body)]
    (assert (= catch 'catch))
    (assert (symbol? sym))
    (if (cljs-env? &env)
      `(try ~@try-body (~'catch js/Object ~sym ~@catch-body))
      `(try ~@try-body (~'catch Throwable ~sym ~@catch-body)))))

(defrecord Recur [bindings])

(defn recur?
  [o]
  (instance? Recur o))

(defmacro recur
  [& args]
  `(->Recur [~@args]))

(defmacro aloop*
  [bindings body]
  (c/let [binds (partition 2 2 bindings)
          names (map first binds)
          fvals (map second binds)
          tsym  (gensym "loop-fn-")
          res-s (gensym "res-")
          err-s (gensym "err-")
          rej-s (gensym "reject-fn-")
          rsv-s (gensym "resolve-fn-")
          inner `(p/finally
                   (fn [~res-s ~err-s]
                     (if (some? ~err-s)
                       (~rej-s ~err-s)
                       (if (recur? ~res-s)
                         (do
                           (exec/run!
                            ;; vthread wasn't available in older promesa
                            exec/default-executor
                            ~(if (seq names)
                               `(fn [] (apply ~tsym (:bindings ~res-s)))
                               tsym))
                           nil)
                         (~rsv-s ~res-s)))))]
    `(p/create
      (fn [~rsv-s ~rej-s]
        (c/let [~tsym (fn ~tsym [~@names]
                        (if (some p/promise? [~@names])
                          (-> (p/let [~@(mapcat (fn [nsym] [nsym nsym]) names)]
                                ~body)
                              ~inner)
                          (c/let [~res-s (try-catchall
                                          ~body
                                          (catch ~err-s
                                              (~rej-s ~err-s)))]
                            (cond
                              (p/promise? ~res-s)
                              (-> ~res-s
                                  ~inner)
                              (recur? ~res-s)
                              ;; recur is weird, I think it's defined in fns and loop
                              ;; and not globally
                              (recur ~@(map (fn [n]
                                              `(nth (:bindings ~res-s) ~n))
                                            (range 0 (count names))))
                              :else
                              (~rsv-s ~res-s)))))]
          (exec/run!
           exec/default-executor
           ~(if (seq names)
              `(fn [] (~tsym ~@fvals))
              tsym)))))))

(defmacro loop
  "Loop/recur with support for resolving promises. It will conditionally be async depending on
  how it's used, it may or may not return a promise. This is useful because every time a new promise
  is awaited on it can add time to the call, we take the fast (sync) path when we can"
  [bindings body]
  (c/let [binds (partition 2 2 bindings)
          names (map first binds)
          res-s (gensym "res-")
          inner `(p/then
                  (fn [~res-s]
                    (if (recur? ~res-s)
                      (aloop*
                       [~@(->> names
                               (map-indexed (fn [i nsym]
                                              [nsym `(nth (:bindings ~res-s) ~i)]))
                               (mapcat identity))]
                       ~body)
                      ~res-s)))]
    `(c/loop ~bindings
       (if (some p/promise? [~@names])
         (-> (p/let [~@(mapcat (fn [nsym] [nsym nsym]) names)]
               ~body)
             ~inner)
         (c/let [~res-s ~body]
           (cond
             (p/promise? ~res-s)
             (-> ~res-s
                 ~inner)
             (recur? ~res-s)
             (recur ~@(map (fn [n]
                             `(nth (:bindings ~res-s) ~n))
                           (range 0 (count names))))
             :else
             ~res-s))))))

(defn reduce
  "Like reduce but `f` can return a promise"
  ([f coll]
   (if-let [s (seq coll)]
     (reduce f (first s) (next s))
     (f)))
  ([f acc coll]
   (c/reduce
    (fn [acc v]
      (then
       acc
       (fn [acc]
         (f acc v))))
    acc
    coll)))

(defn reduce-kv
  "Like reduce-kv but `f` can return a promise"
  [f acc coll]
  (c/reduce-kv
   (fn [acc k v]
     (then
      acc
      (fn [acc]
        (f acc k v))))
   acc
   coll))

(defn mapv
  [f coll]
  (-> (reduce
       (fn [acc x]
         (then
          (f x)
          (fn [new-x]
            (conj! acc new-x))))
       (transient [])
       coll)
      (then persistent!)))

(defn filterv
  [f coll]
  (-> (reduce
       (fn [acc x]
         (then
          (f x)
          (fn [keep?]
            (if keep?
              (conj! acc x)
              acc))))
       (transient [])
       coll)
      (then persistent!)))

(defn every? [pred coll]
  (loop [coll (seq coll)]
    (if (seq coll)
      (-> (pred (first coll))
          (then (fn [res?]
                  (if res?
                    (->Recur [(rest coll)])
                    false))))
      true)))

