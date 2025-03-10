(ns me.tonsky.promises
  (:require
   [promesa.core :as p]))

(defmacro plet-ifp
  "If a value in the let binding is a promise, then use p/let, otherwise use let
  In the best case, the code will run sync. Useful if you may or may not have a promise
  because it's faster to not use promises if you don't have to"
  {:style/indent 1}
  [bindings & body]
  (let [[n v & more] bindings
        inner (if (seq more)
                `(plet-ifp ~more ~@body)
                `(do ~@body))]
    `(let [~n ~v]
       (if (p/promise? ~n)
         (p/then ~n
                 (fn [~n]
                   ~inner))
         ;; (p/let [~n ~n]
         ;;   ~inner)
         ~inner))))

(comment

  ;; (macroexpand-1)
  (clojure.pprint/pprint
   (clojure.walk/macroexpand-all
    '(plet-ifp [x 1
                y (p/resolved 2)
                z (+ y x)]
               y)))


  )
