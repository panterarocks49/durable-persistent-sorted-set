(ns me.tonsky.chunk)

;; replace with cljs.core/ArrayChunk after https://dev.clojure.org/jira/browse/CLJS-2470
(deftype Chunk [arr off end]
  ICounted
  (-count [_] (- end off))

  IIndexed
  (-nth [this i]
    (aget arr (+ off i)))

  (-nth [this i not-found]
    (if (and (>= i 0) (< i (- end off)))
      (aget arr (+ off i))
      not-found))

  IChunk
  (-drop-first [this]
    (if (== off end)
      (throw (js/Error. "-drop-first of empty chunk"))
      (Chunk. arr (inc off) end)))

  IReduce
  (-reduce [this f]
    (if (== off end)
      (f)
      (-reduce (-drop-first this) f (aget arr off))))

  (-reduce [this f start]
    (loop [val start, n off]
      (if (< n end)
        (let [val' (f val (aget arr n))]
          (if (reduced? val')
            @val'
            (recur val' (inc n))))
        val))))
