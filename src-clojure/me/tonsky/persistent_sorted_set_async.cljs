(ns ^{:doc
      "A B-tree based persistent sorted set. Supports transients, custom comparators, fast iteration, efficient slices (iterator over a part of the set) and reverse slices. Almost a drop-in replacement for [[clojure.core/sorted-set]], the only difference being this one can’t store nil."
      :author "Nikita Prokopov"}
    me.tonsky.persistent-sorted-set-async
  (:refer-clojure :exclude [iter conj disj sorted-set sorted-set-by set])
  (:require
   [promesa.core :as p]
   [me.tonsky.chunk :refer [Chunk]]
   [me.tonsky.maybe-promise :as mp]
   [me.tonsky.persistent-sorted-set.storage :as storage]
   [me.tonsky.persistent-sorted-set.arrays :as arrays])
  (:require-macros
   [me.tonsky.persistent-sorted-set.arrays :as arrays]))

; B+ tree
; -------

; Leaf:     keys[]     :: array of values

; Node:     pointers[] :: links to children nodes
;           keys[]     :: max value for whole subtree
;                         node.keys[i] == max(node.pointers[i].keys)
; All arrays are 16..32 elements, inclusive

; BTSet:    storage    :: IStorage protocol
;           root       :: Node or Leaf
;           shift      :: depth - 1
;           cnt        :: size of a set, integer, rolling
;           comparator :: comparator used for ordering
;           meta       :: clojure meta map
;           _hash      :: hash code, same as for clojure collections, on-demand, cached

; Path: conceptually a vector of indexes from root to leaf value, but encoded in a single number.
;       E.g. we have path [7 30 11] representing root.pointers[7].pointers[30].keys[11].
;       In our case level-shift is 5, meaning each index will take 5 bits:
;       (7 << 10) | (30 << 5) | (11 << 0) = 8139
;         00111       11110       01011

; AsyncIter:     set       :: Set this iterator belongs to
;           left      :: Current path
;           right     :: Right bound path (exclusive)
;           keys      :: Cached ref for keys array for a leaf
;           idx       :: Cached idx in keys array
; Keys and idx are cached for fast iteration inside a leaf"

(def ^:const max-safe-path
  "js limitation for bit ops"
  (js/Math.pow 2 31))

;; have to be careful in changing this only certain values make sense
;; so we don't limit the overall size
(def ^:const bits-per-level
  "tunable param"
  ;; 5
  ;; 8 is pretty good tradeoff in performance
  ;; but I think we want largest for storage
  10)

(def ^:const max-len
  (js/Math.pow 2 bits-per-level)) ;; 32

(def ^:const min-len
  (/ max-len 2)) ;; 16

(def ^:private ^:const avg-len
  (arrays/half (+ max-len min-len))) ;; 24

(def ^:const max-safe-level
  (js/Math.floor (/ 31 bits-per-level))) ;; 6

(def ^:const bit-mask
  (- max-len 1)) ;; 0b011111 = 5 bit

(def factors
  (arrays/into-array (map #(js/Math.pow 2 %) (range 0 52 bits-per-level))))

(def ^:const empty-path 0)

(defprotocol INodeStore
  ;; kind of dumb but not every type implements all var args
  (-store [this] [this storage settings])
  (-walk-addresses [this on-address] [this storage settings on-address]))

(defn- path-get ^number [^number path ^number level]
  (if (< level max-safe-level)
    (-> path
        (unsigned-bit-shift-right (* level bits-per-level))
        (bit-and bit-mask))
    (-> path
        (/ (arrays/aget factors level))
        (js/Math.floor)
        (bit-and bit-mask))))

(defn- path-set ^number [^number path ^number level ^number idx]
  (let [smol? (and (< path max-safe-path) (< level max-safe-level))
        old   (path-get path level)
        minus (if smol?
                (bit-shift-left old (* level bits-per-level))
                (* old (arrays/aget factors level)))
        plus  (if smol?
                (bit-shift-left idx (* level bits-per-level))
                (* idx (arrays/aget factors level)))]
    (-> path
        (- minus)
        (+ plus))))

(defn- path-set-zero-lower
  "Set the path at level to idx, zero out the lower levels"
  [path level idx]
  (loop [path (path-set path level idx)
         level (dec level)]
    (if (<= 0 level)
      (recur (path-set path level 0) (dec level))
      path)))

(defn- path-inc ^number [^number path]
  (inc path))

(defn- path-dec ^number [^number path]
  (dec path))

(defn- path-cmp ^number [^number path1 ^number path2]
  (- path1 path2))

(defn- path-lt ^boolean [^number path1 ^number path2]
  (< path1 path2))

(defn- path-lte ^boolean [^number path1 ^number path2]
  (<= path1 path2))

(defn- path-eq ^boolean [^number path1 ^number path2]
  (== path1 path2))

(defn- path-same-leaf ^boolean [^number path1 ^number path2]
  (if (and
       (< path1 max-safe-path)
       (< path2 max-safe-path))
    (==
     (unsigned-bit-shift-right path1 bits-per-level)
     (unsigned-bit-shift-right path2 bits-per-level))
    (==
     (Math/floor (/ path1 max-len))
     (Math/floor (/ path2 max-len)))))

(defn- path-str [^number path]
  (loop [res ()
         path path]
    (if (not= path 0)
      (recur (cljs.core/conj res (mod path max-len)) (Math/floor (/ path max-len)))
      (vec res))))

(defn- binary-search-l [cmp arr r k]
  (loop [l 0
         r (long r)]
    (if (<= l r)
      (let [m  (arrays/half (+ l r))
            mk (arrays/aget arr m)]
        (if (neg? (cmp mk k))
          (recur (inc m) r)
          (recur l (dec m))))
      l)))

(defn- binary-search-r [cmp arr r k]
  (loop [l 0
         r (long r)]
    (if (<= l r)
      (let [m  (arrays/half (+ l r))
            mk (arrays/aget arr m)]
        (if (pos? (cmp mk k))
          (recur l (dec m))
          (recur (inc m) r)))
      l)))

(defn- lookup-exact [cmp arr key]
  (let [arr-l (arrays/alength arr)
        idx   (binary-search-l cmp arr (dec arr-l) key)]
    (if (and (< idx arr-l)
             (== 0 (cmp (arrays/aget arr idx) key)))
      idx
      -1)))

(defn- lookup-range [cmp arr key]
  (let [arr-l (arrays/alength arr)
        idx   (binary-search-l cmp arr (dec arr-l) key)]
    (if (== idx arr-l)
      -1
      idx)))

;; Array operations

(defn- cut-n-splice [arr cut-from cut-to splice-from splice-to xs]
  (let [xs-l (arrays/alength xs)
        l1   (- splice-from cut-from)
        l2   (- cut-to splice-to)
        l1xs (+ l1 xs-l)
        new-arr (arrays/make-array (+ l1 xs-l l2))]
    (arrays/acopy arr cut-from splice-from new-arr 0)
    (arrays/acopy xs 0 xs-l new-arr l1)
    (arrays/acopy arr splice-to cut-to new-arr l1xs)
    new-arr))

(defn- splice [arr splice-from splice-to xs]
  (cut-n-splice arr 0 (arrays/alength arr) splice-from splice-to xs))

(defn- insert [arr idx xs]
  (cut-n-splice arr 0 (arrays/alength arr) idx idx xs))

(defn- merge-n-split [a1 a2]
  (let [a1-l    (arrays/alength a1)
        a2-l    (arrays/alength a2)
        total-l (+ a1-l a2-l)
        r1-l    (arrays/half total-l)
        r2-l    (- total-l r1-l)
        r1      (arrays/make-array r1-l)
        r2      (arrays/make-array r2-l)]
    (if (<= a1-l r1-l)
      (do
        (arrays/acopy a1 0             a1-l          r1 0)
        (arrays/acopy a2 0             (- r1-l a1-l) r1 a1-l)
        (arrays/acopy a2 (- r1-l a1-l) a2-l          r2 0))
      (do
        (arrays/acopy a1 0    r1-l r1 0)
        (arrays/acopy a1 r1-l a1-l r2 0)
        (arrays/acopy a2 0    a2-l r2 (- a1-l r1-l))))
    (arrays/array r1 r2)))

(defn- ^boolean eq-arr [cmp a1 a1-from a1-to a2 a2-from a2-to]
  (let [len (- a1-to a1-from)]
    (and
     (== len (- a2-to a2-from))
     (loop [i 0]
       (cond
         (== i len)
         true

         (not (== 0 (cmp
                     (arrays/aget a1 (+ i a1-from))
                     (arrays/aget a2 (+ i a2-from)))))
         false

         :else
         (recur (inc i)))))))

(defn- check-n-splice [cmp arr from to new-arr]
  (if (eq-arr cmp arr from to new-arr 0 (arrays/alength new-arr))
    arr
    (splice arr from to new-arr)))

(defn- return-array
  "Drop non-nil references and return array of arguments"
  ([a1]
   (arrays/array a1))
  ([a1 a2]
   (if a1
     (if a2
       (arrays/array a1 a2)
       (arrays/array a1))
     (arrays/array a2)))
  ([a1 a2 a3]
   (if a1
     (if a2
       (if a3
         (arrays/array a1 a2 a3)
         (arrays/array a1 a2))
       (if a3
         (arrays/array a1 a3)
         (arrays/array a1)))
     (if a2
       (if a3
         (arrays/array a2 a3)
         (arrays/array a2))
       (arrays/array a3)))))

;;

(defprotocol INode
  (node-lim-key       [_])
  (node-len           [_])
  (node-merge         [_ next])
  (node-merge-n-split [_ next])
  ;; all functions below may or may not return a promise
  ;; it's slower to always return a promise
  (node-child         [_ idx storage settings])
  (node-lookup        [_ cmp key storage settings])
  (node-conj          [_ cmp key storage settings])
  (node-disj          [_ cmp key root? left right storage settings]))

(defn- rotate [node root? left right]
  (cond
    ;; root never merges
    root?
    (return-array node)

    ;; enough keys, nothing to merge
    (> (node-len node) min-len)
    (return-array left node right)

    ;; left and this can be merged to one
    (and left (<= (node-len left) min-len))
    (return-array (node-merge left node) right)

    ;; right and this can be merged to one
    (and right (<= (node-len right) min-len))
    (return-array left (node-merge node right))

    ;; left has fewer nodes, redestribute with it
    (and left (or (nil? right)
                  (< (node-len left) (node-len right))))
    (let [nodes (node-merge-n-split left node)]
      (return-array (arrays/aget nodes 0) (arrays/aget nodes 1) right))

    ;; right has fewer nodes, redestribute with it
    :else
    (let [nodes (node-merge-n-split node right)]
      (return-array left (arrays/aget nodes 0) (arrays/aget nodes 1)))))

(declare Node)

(defn default-make-reference [node]
  ;; keep nodes/branches in memory forever
  ;; should be less than 1% of the size of the set
  (if (instance? Node node)
    node
    (js/WeakRef. node)))

(defn default-read-reference [node]
  (if (instance? js/WeakRef node)
    (.deref node)
    node))

(defprotocol IReference
  (-make-ref [this node])
  (-read-ref [this node]))

(defrecord Settings [make-reference read-reference store-group-size]
  IReference
  (-make-ref [_ node]
    (make-reference node))
  (-read-ref [_ node]
    (read-reference node)))

(defn map->settings
  [{:keys [make-reference read-reference store-group-size]}]
  (Settings.
   (or make-reference default-make-reference)
   (or read-reference default-read-reference)
   (or store-group-size 1)))

(defn settings->map
  [settings]
  {:branching-factor max-len
   :make-reference   (:make-reference settings)
   :read-reference   (:read-reference settings)
   :store-group-size (:store-group-size settings)})

(defn- ensure-addresses!
  [^Node node size]
  ;; TODO: this check essentially does nothing because addresses right now is always initialized
  ;; eventually if we use this as in memory as well as durable then we might want to set this up
  ;; such that addresses is only initialized when we need it
  ;; but it doesn't matter at the moment
  ;; in practice with 1024 branching factor this is very small overhead
  (when (nil? (.-_addresses node))
    (let [addresses (arrays/make-array size)]
      (set! (.-_addresses node) addresses)
      addresses)))

(deftype Node [keys pointers ^:mutable _addresses]
  INodeStore
  ;; every node stores all of it's children, but not itself
  ;; it first calls store on the children, so lower levels store first
  (-store [this storage ^Settings settings]
    (let [len        (arrays/alength pointers)
          _          (ensure-addresses! this len)
          group-size (:store-group-size settings)]
      (mp/loop [grouped-addrs (->> (vec _addresses)
                                   (map-indexed vector)
                                   (partition-all group-size))]
        (when (seq grouped-addrs)
          (mp/let [addrs (first grouped-addrs)]
            (when (some #(nil? (second %)) addrs)
              (mp/let [children
                       (mp/mapv
                        (fn [[idx addr]]
                          ;; we might read in the middle of writing
                          ;; because we could be resaving
                          (mp/let [child (node-child this idx storage settings)
                                   ;; store the lower level first
                                   _ (when (nil? addr)
                                       (-store child storage settings))]
                            [addr child]))
                        addrs)
                       stored-addrs (storage/store storage children)]
                (assert (and (= (count children) (count stored-addrs))
                             (every? some? stored-addrs)))
                (doseq [[[idx _] address] (map vector addrs stored-addrs)]
                  (let [child (node-child this idx storage settings)]
                    (when address
                      (arrays/aset _addresses idx address)
                      (arrays/aset pointers idx (-make-ref settings child)))))))
            (mp/recur (rest grouped-addrs)))))))

  (-walk-addresses [this storage settings on-address]
    (mp/let [len (arrays/alength pointers)]
      (ensure-addresses! this len)
      (mp/loop [idx 0]
        (when (< idx len)
          (mp/do
            (mp/let [address    (arrays/aget _addresses idx)
                     child-node (node-child this idx storage settings)]
              (if address
                (when (on-address address)
                  (-walk-addresses child-node storage settings on-address))
                (-walk-addresses child-node storage settings on-address)))
            (mp/recur (inc idx)))))))

  INode
  (node-lim-key [_]
    (arrays/alast keys))

  (node-len [_]
    (arrays/alength keys))

  (node-merge [this ^Node next]
    (ensure-addresses! this (arrays/alength pointers))
    (ensure-addresses! next (arrays/alength (.-pointers next)))
    (Node. (arrays/aconcat keys (.-keys next))
           (arrays/aconcat pointers (.-pointers next))
           (arrays/aconcat _addresses (.-_addresses next))))

  (node-merge-n-split [this ^Node next]
    (ensure-addresses! this (arrays/alength pointers))
    (ensure-addresses! next (arrays/alength (.-pointers next)))
    (let [ks (merge-n-split keys     (.-keys next))
          ps (merge-n-split pointers (.-pointers next))
          as (merge-n-split _addresses (.-_addresses next))]
      (return-array (Node. (arrays/aget ks 0)
                           (arrays/aget ps 0)
                           (arrays/aget as 0))
                    (Node. (arrays/aget ks 1)
                           (arrays/aget ps 1)
                           (arrays/aget as 1)))))

  (node-child [_this idx ^storage/IStorage storage settings]
    ;; TODO: Remove when the implementation is stable
    #_#_
    (assert (and (<= 0 idx)
                 (< idx (arrays/alength pointers))))
    (assert (or (and pointers (arrays/aget pointers idx))
                (and _addresses (arrays/aget _addresses idx))))
    (let [child   (-read-ref settings (arrays/aget pointers idx))
          address (when _addresses (arrays/aget _addresses idx))]
      (if child
        (do (when (and storage address)
              (storage/accessed storage address))
            child)
        (mp/let [child (storage/restore storage address)]
          (when-not child (throw (ex-info "node-child not found" {:address address})))
          (arrays/aset pointers idx (-make-ref settings child))
          child))))

  (node-lookup [this cmp key storage settings]
    (let [idx (lookup-range cmp keys key)]
      (when-not (== -1 idx)
        (mp/let [child (node-child this idx storage settings)]
          (node-lookup child cmp key storage settings)))))

  (node-conj [this cmp key storage settings]
    (ensure-addresses! this (arrays/alength pointers))
    (mp/let [idx   (binary-search-l cmp keys (- (arrays/alength keys) 2) key)
             child (node-child this idx storage settings)
             nodes (node-conj child cmp key storage settings)]
      (when nodes
        (let [new-keys      (check-n-splice cmp keys       idx (inc idx) (arrays/amap node-lim-key nodes))
              new-pointers  (splice             pointers   idx (inc idx) nodes)
              ;; in conj, we init nil addresses because the nodes returned are always new
              new-addresses (splice             _addresses idx (inc idx) (arrays/make-array (arrays/alength nodes)))]
          (if (<= (arrays/alength new-pointers) max-len)
            ;; ok as is
            (arrays/array (Node. new-keys new-pointers new-addresses))
            ;; gotta split it up
            (let [middle (arrays/half (arrays/alength new-pointers))]
              (arrays/array
               (Node. (.slice new-keys      0 middle)
                      (.slice new-pointers  0 middle)
                      (.slice new-addresses 0 middle))
               (Node. (.slice new-keys      middle)
                      (.slice new-pointers  middle)
                      (.slice new-addresses middle)))))))))

  (node-disj [this cmp key root? left right storage settings]
    (ensure-addresses! this (arrays/alength pointers))
    (let [idx (lookup-range cmp keys key)]
      (when-not (== -1 idx) ;; short-circuit, key not here
        (mp/let [child       (node-child this idx storage settings)
                 left-child  (when (>= (dec idx) 0)
                               (node-child this (dec idx) storage settings))
                 right-child (when (< (inc idx) (arrays/alength pointers))
                               (node-child this (inc idx) storage settings))
                 disjned     (node-disj child cmp key false left-child right-child storage settings)]
          (when disjned     ;; short-circuit, key not here
            (let [left-idx      (if left-child  (dec idx) idx)
                  right-idx     (if right-child (+ 2 idx) (+ 1 idx))
                  find-address  (fn [node]
                                  (cond
                                    (identical? left-child node)
                                    (arrays/aget _addresses (dec idx))
                                    (identical? child node)
                                    (arrays/aget _addresses idx)
                                    (identical? right-child node)
                                    (arrays/aget _addresses (inc idx))))
                  new-keys      (check-n-splice cmp keys       left-idx right-idx (arrays/amap node-lim-key disjned))
                  new-pointers  (splice             pointers   left-idx right-idx disjned)
                  new-addresses (splice             _addresses left-idx right-idx (arrays/amap find-address disjned))]
              (rotate (Node. new-keys new-pointers new-addresses) root? left right))))))))

(deftype Leaf [keys]
  INodeStore
  ;; noop because nodes store their children
  (-store [_ _ _])

  ;; noop on leaf
  (-walk-addresses [_ _ _ _])

  INode
  (node-lim-key [_]
    (arrays/alast keys))
  ;;   Object
  ;;   (toString [_] (pr-str* (vec keys)))

  (node-len [_]
    (arrays/alength keys))

  (node-merge [_ next]
    (Leaf. (arrays/aconcat keys (.-keys next))))

  (node-merge-n-split [_ next]
    (let [ks (merge-n-split keys (.-keys next))]
      (return-array (Leaf. (arrays/aget ks 0))
                    (Leaf. (arrays/aget ks 1)))))

  (node-child [_ idx _ _]
    (arrays/aget keys idx))

  (node-lookup [this cmp key storage settings]
    (let [idx (lookup-exact cmp keys key)]
      (when-not (== -1 idx)
        (node-child this idx storage settings))))

  (node-conj [this cmp key storage settings]
    (let [idx    (binary-search-l cmp keys (dec (arrays/alength keys)) key)
          keys-l (arrays/alength keys)]
      (cond
        ;; element already here
        (and (< idx keys-l)
             (== 0 (cmp key (node-child this idx storage settings))))
        nil
        ;; splitting
        (== keys-l max-len)
        (let [middle (arrays/half (inc keys-l))]
          (if (> idx middle)
            ;; new key goes to the second half
            (arrays/array
             (Leaf. (.slice keys 0 middle))
             (Leaf. (cut-n-splice keys middle keys-l idx idx (arrays/array key))))
            ;; new key goes to the first half
            (arrays/array
             (Leaf. (cut-n-splice keys 0 middle idx idx (arrays/array key)))
             (Leaf. (.slice keys middle keys-l)))))
        ;; ok as is
        :else
        (arrays/array (Leaf. (splice keys idx idx (arrays/array key)))))))

  (node-disj [_ cmp key root? left right _ _]
    (let [idx (lookup-exact cmp keys key)]
      (when-not (== -1 idx) ;; key is here
        (let [new-keys (splice keys idx (inc idx) (arrays/array))]
          (rotate (Leaf. new-keys) root? left right))))))

;; BTSet

(declare get-leaves)

(defn async-iter->vec
  [iter]
  (p/let [t (.now js/performance)
          ret (reduce
               -conj!
               (-as-transient (.-EMPTY PersistentVector))
               iter)]
    (prn "time" (- (.now js/performance) t))
    (-persistent! ret)))

(declare conj disj btset-async-iter -slice rslice)

(def ^:private ^:const uninitialized-hash nil)
(def ^:private ^:const uninitialized-address nil)

(defprotocol IRoot
  (-root [_]))

(deftype BTSet [^:mutable _storage ^:mutable _root shift cnt comparator meta ^:mutable _hash ^:mutable _address _settings]
  Object
  (toString [this] (pr-str* this))

  ICloneable
  (-clone [_] (BTSet. _storage _root shift cnt comparator meta _hash _address _settings))

  IWithMeta
  (-with-meta [_ new-meta] (BTSet. _storage _root shift cnt comparator new-meta _hash _address _settings))

  IMeta
  (-meta [_] meta)

  IEmptyableCollection
  (-empty [_] (BTSet. _storage (Leaf. (arrays/array)) 0 0 comparator meta uninitialized-hash uninitialized-address _settings))

  IEquiv
  ;; TODO: this is probably broken
  (-equiv [this other]
    (and
     (set? other)
     (== cnt (count other))
     (every? #(contains? this %) other)))

  IHash
  ;; TODO: this is probably broken
  (-hash [this] (caching-hash this hash-unordered-coll _hash))

  ICollection
  (-conj [this key] (conj this key comparator))

  ISet
  (-disjoin [this key] (disj this key comparator))

  IRoot
  ;; we don't bother doing weak refs on the root
  ;; I figure you should always want to keep this in memory at least
  (-root [_]
    (or _root
        (when _address
          (mp/let [node (storage/restore _storage _address)]
            (when-not node (throw (ex-info "Root not found" {:address _address})))
            (set! _root node)
            node))))

  INodeStore
  (-store [this]
    (mp/do
      (assert (some? _storage) "Can't store without a storage")
      (when (nil? _address)
        (mp/let [root  (-root this)
                 _     (-store root _storage _settings)
                 addrs (storage/store _storage [[nil root]])]
          (set! _address (first addrs))))
      _address))
  (-store [this storage _]
    (set! _storage storage)
    (-store this))

  (-walk-addresses [this on-address]
    (mp/let [root (-root this)]
      (if _address
        (when (on-address _address)
          (-walk-addresses root _storage _settings on-address))
        (-walk-addresses root _storage _settings on-address))))

  ILookup
  (-lookup [this k]
    (mp/let [root (-root this)]
      (node-lookup root comparator k _storage _settings)))
  (-lookup [this k not-found]
    (mp/let [root (-root this)]
      (or (node-lookup root comparator k _storage _settings) not-found)))

  ISeqable
  (-seq [this]
    ;; this is mostly done for tests, maybe change later
    ;; not sure if it's reasonable for this to return a promise here
    ;; probably not so I should remove
    (-slice this nil nil comparator))

  IReduce
  (-reduce [this f]
    (mp/let [i (seq this)]
      (if i
        (-reduce i f)
        (f))))
  (-reduce [this f start]
    (mp/let [i (seq this)]
      (if i
        (-reduce i f start)
        start)))

  IReversible
  (-rseq [this]
    (rslice this nil nil comparator))

  ;; ISorted
  ;; (-sorted-seq [this ascending?])
  ;; (-sorted-seq-from [this k ascending?])
  ;; (-entry-key [this entry] entry)
  ;; (-comparator [this] comparator)

  ICounted
  (-count [_] cnt)

  IEditableCollection
  (-as-transient [this] this)

  ITransientCollection
  (-conj! [this key] (conj this key comparator))
  (-persistent! [this] this)

  ITransientSet
  (-disjoin! [this key] (disj this key comparator))

  IFn
  (-invoke [this k] (-lookup this k))
  (-invoke [this k not-found] (-lookup this k not-found))

  IPrintWithWriter
  (-pr-writer [this writer opts]
    (pr-sequential-writer writer pr-writer "#{" " " "}" opts #_("BTSet") (seq this))))

(defn- keys-for [^BTSet set path]
  (mp/loop [level (.-shift set)
            node  (-root set)]
    (if (pos? level)
      (mp/recur
       (dec level)
       (node-child node (path-get path level) (.-_storage set) (.-_settings set)))
      (.-keys node))))

(defn- alter-btset [^BTSet set root shift cnt]
  (BTSet. (.-_storage set) root shift cnt (.-comparator set) (.-meta set) uninitialized-hash uninitialized-address (.-_settings set)))


;; iteration

(defn- -next-path [^BTSet set node ^number path ^number level]
  (let [idx (path-get path level)]
    (if (pos? level)
      ;; inner node
      (mp/let [child    (node-child node idx (.-_storage set) (.-_settings set))
               sub-path (-next-path set child path (dec level))]
        (if (nil? sub-path)
          ;; nested node overflow
          (if (< (inc idx) (arrays/alength (.-pointers node)))
            ;; advance current node idx, reset subsequent indexes
            (path-set empty-path level (inc idx))
            ;; current node overflow
            nil)
          ;; keep current idx
          (path-set sub-path level idx)))
      ;; leaf
      (if (< (inc idx) (arrays/alength (.-keys node)))
        ;; advance leaf idx
        (path-set empty-path 0 (inc idx))
        ;; leaf overflow
        nil))))

(defn- -rpath
  "Returns rightmost path possible starting from node and going deeper"
  [^BTSet set node ^number path ^number level]
  (mp/loop [node  node
            path  path
            level level]
    (if (pos? level)
      ;; inner node
      (let [end-idx (dec (arrays/alength (.-pointers node)))]
        (mp/recur
         (node-child node end-idx (.-_storage set) (.-_settings set))
         (path-set path level end-idx)
         (dec level)))
      ;; leaf
      (path-set path 0 (dec (arrays/alength (.-keys node)))))))

(defn- next-path
  "Returns path representing next item after `path` in natural traversal order.
   Will overflow at leaf if at the end of the tree"
  [set ^number path]
  (if (neg? path)
    empty-path
    (mp/let [root (-root set)
             np   (-next-path set root path (.-shift set))]
      (if np
        np
        (mp/let [rp (-rpath set root empty-path (.-shift set))]
          (path-inc rp))))))

(defn- -prev-path [^BTSet set node ^number path ^number level]
  (let [idx (path-get path level)]
    (cond
      ;; leaf overflow
      (and (== 0 level) (== 0 idx))
      nil

      ;; leaf
      (== 0 level)
      (path-set empty-path 0 (dec idx))

      ;; branch that was overflow before
      (>= idx (node-len node))
      (-rpath set node path level)

      :else
      (mp/let [child (node-child node idx (.-_storage set) (.-_settings set))
               path' (-prev-path set child path (dec level))]
        (cond
          ;; no sub-overflow, keep current idx
          (some? path')
          (path-set path' level idx)

          ;; nested overflow + this node overflow
          (== 0 idx)
          nil

          ;; nested overflow, advance current idx, reset subsequent indexes
          :else
          (mp/let [pchild (node-child node (dec idx) (.-_storage set) (.-_settings set))
                   path'  (-rpath set pchild path (dec level))]
            (path-set path' level (dec idx))))))))

(defn- prev-path
  "Returns path representing previous item before `path` in natural traversal order.
   Will overflow at leaf if at beginning of tree"
  [set ^number path]
  (mp/let [root (-root set)]
    (if (> (path-get path (inc (.-shift set))) 0) ;; overflow
      (-rpath set root path (.-shift set))
      (mp/let [pp (-prev-path set root path (.-shift set))]
        (if pp
          pp
          (path-dec empty-path))))))

(declare async-iter async-riter)

(defn- btset-async-iter
  "AsyncIterator that represents the whole set"
  [set]
  (p/let [root (-root set)]
    (when (pos? (node-len root))
      (p/let [left  empty-path
              rpath (-rpath set root empty-path (.-shift set))
              right (next-path set rpath)]
        (async-iter set left right)))))

(defprotocol IAsyncIter
  (-copy [this left right]))

(defprotocol ISeek
  (-seek
    [this key]
    [this key comparator]))

(declare -seek* -rseek* ReverseIter)

;; likely faster but haven't had the time to flush out
#_
(deftype Iter2 [^js arr arr-len leaves-idx leaves-len leaves idx end-idx]
  IEquiv
  #_:clj-kondo/ignore
  (-equiv [this other] (equiv-sequential this other))

  ISequential
  ISeqable
  (-seq [this] this)

  ISeq
  (-first [_]
    (arrays/aget arr idx))

  (-rest [this]
    (or (-next this) '()))

  INext
  (-next [_]
    (let [inc-idx         (inc idx)
          next-leaves-idx (inc leaves-idx)]
      (if (< next-leaves-idx leaves-len)
        (if (< inc-idx arr-len)
          (Iter2. arr arr-len leaves-idx leaves-len leaves inc-idx end-idx)
          (let [next-leaf (arrays/aget leaves next-leaves-idx)
                keys (.-keys next-leaf)]
            (Iter2. keys (arrays/alength keys) next-leaves-idx leaves-len leaves 0 end-idx)))
        (when (< inc-idx end-idx)
          (Iter2. arr arr-len leaves-idx leaves-len leaves inc-idx end-idx)))))

  ;; IChunkedSeq
  ;; (-chunked-first [_]
  ;;   (let [next-leaf (arrays/aget leaves (inc leaves-idx))
  ;;         end-idx   (if next-leaf
  ;;                     (arrays/alength arr)
  ;;                     end-idx)]
  ;;     (Chunk. arr idx end-idx)))

  ;; (-chunked-rest [this]
  ;;   (or (-chunked-next this) ()))

  ;; IChunkedNext
  ;; (-chunked-next [_]
  ;;   (let [next-leaves-idx (inc leaves-idx)
  ;;         next-leaf       (arrays/aget leaves next-leaves-idx)]
  ;;     (when next-leaf
  ;;       (Iter2. (.-keys next-leaf) next-leaves-idx leaves-len leaves 0 end-idx))))

  ;; IReduce
  ;; (-reduce [this f]
  ;;   (if (nil? arr)
  ;;     (f)
  ;;     (let [first (-first this)
  ;;           next  (-next this)]
  ;;       (if (some? next)
  ;;         (-reduce next f first)
  ;;         first))))

  ;; (-reduce [_ f start]
  ;;   (loop [arr        arr
  ;;          leaves-idx leaves-idx
  ;;          idx        idx
  ;;          acc        start]
  ;;     (let [new-acc (f acc (arrays/aget arr idx))
  ;;           inc-idx (inc idx)]
  ;;       (if (reduced? new-acc)
  ;;         @new-acc
  ;;         (let [next-leaves-idx (inc leaves-idx)
  ;;               next-leaf       (arrays/aget leaves next-leaves-idx)]
  ;;           (if next-leaf
  ;;             (if (< inc-idx (arrays/alength arr))
  ;;               (recur arr leaves-idx inc-idx new-acc)
  ;;               (recur (.-keys next-leaf) next-leaves-idx 0 new-acc))
  ;;             (if (< inc-idx end-idx)
  ;;               (recur arr leaves-idx inc-idx new-acc)
  ;;               new-acc)))))))

  ;; TODO:
  ;; IReversible
  ;; (-rseq [_]
  ;;   (let [rev-leaves (-> '()
  ;;                        (clojure.core/conj cur-leaf)
  ;;                        (into leaves))
  ;;         first-leaf (first rev-leaves)]
  ;;     (ReverseIter. (.-keys first-leaf) first-leaf (next rev-leaves) (dec end-idx) (dec idx))))

  ;; ISeek
  ;; (-seek [this key]
  ;;   (-seek this key (.-comparator set)))

  ;; (-seek [this key cmp]
  ;;   (throw (ex-info "seek not impl yet" {})))

  Object
  (toString [this] (pr-str* this))

  IPrintWithWriter
  #_:clj-kondo/ignore
  (-pr-writer [this writer opts]
    (pr-sequential-writer writer pr-writer "(" " " ")" opts (seq this))))

;; arr is the first leaf keys
;; arr should always exist
(deftype Iter [^js arr cur-leaf leaves idx end-idx]
  IEquiv
  #_:clj-kondo/ignore
  (-equiv [this other] (equiv-sequential this other))

  ISequential
  ISeqable
  (-seq [this] this)

  ISeq
  (-first [_]
    (arrays/aget arr idx))

  (-rest [this]
    (or (-next this) '()))

  INext
  (-next [_]
    (let [inc-idx (inc idx)]
      (if leaves
        (if (< inc-idx (arrays/alength arr))
          (Iter. arr cur-leaf leaves inc-idx end-idx)
          (let [first-leaf (-first leaves)]
            (Iter. (.-keys first-leaf) first-leaf (-next leaves) 0 end-idx)))
        (when (< inc-idx end-idx)
          (Iter. arr cur-leaf leaves inc-idx end-idx)))))

  IChunkedSeq
  (-chunked-first [_]
    (let [end-idx (if leaves
                    (arrays/alength arr)
                    end-idx)]
      (Chunk. arr idx end-idx)))

  (-chunked-rest [this]
    (or (-chunked-next this) ()))

  IChunkedNext
  (-chunked-next [_]
    (when leaves
      (let [first-leaf (-first leaves)]
        (Iter. (.-keys first-leaf) first-leaf (-next leaves) 0 end-idx))))


  IReduce
  (-reduce [this f]
    (if (nil? arr)
      (f)
      (let [first (-first this)
            next  (-next this)]
        (if (some? next)
          (-reduce next f first)
          first))))

  (-reduce [_ f start]
    (loop [arr    arr
           leaves leaves
           idx    idx
           acc    start]
      (let [new-acc (f acc (arrays/aget arr idx))
            inc-idx (inc idx)]
        (if (reduced? new-acc)
          @new-acc
          (if leaves
            (if (< inc-idx (arrays/alength arr))
              (recur arr leaves inc-idx new-acc)
              (recur (.-keys (-first leaves)) (-next leaves) 0 new-acc))
            (if (< inc-idx end-idx)
              (recur arr leaves inc-idx new-acc)
              new-acc))))))

  IReversible
  (-rseq [_]
    (let [rev-leaves (-> '()
                         (clojure.core/conj cur-leaf)
                         (into leaves))
          first-leaf (first rev-leaves)]
      (ReverseIter. (.-keys first-leaf) first-leaf (next rev-leaves) (dec end-idx) (dec idx))))

  ;; ISeek
  ;; (-seek [this key]
  ;;   (-seek this key (.-comparator set)))

  ;; (-seek [this key cmp]
  ;;   (throw (ex-info "seek not impl yet" {})))

  Object
  (toString [this] (pr-str* this))

  IPrintWithWriter
  #_:clj-kondo/ignore
  (-pr-writer [this writer opts]
    (pr-sequential-writer writer pr-writer "(" " " ")" opts (seq this))))

(deftype ReverseIter [^js arr cur-leaf rev-leaves idx end-idx]
  IEquiv
  #_:clj-kondo/ignore
  (-equiv [this other] (equiv-sequential this other))

  ISequential
  ISeqable
  (-seq [this] this)

  ISeq
  (-first [_]
    (arrays/aget arr idx))

  (-rest [this]
    (or (-next this) '()))

  INext
  (-next [_]
    ;; reverse so we walk backwards in the array
    ;; but leaves are in reverse order already
    (if rev-leaves
      (if (< 0 idx)
        (ReverseIter. arr cur-leaf rev-leaves (dec idx) end-idx)
        (let [first-leaf (-first rev-leaves)
              first-arr  (.-keys first-leaf)]
          (ReverseIter. first-arr first-leaf (next rev-leaves) (dec (arrays/alength first-arr)) end-idx)))
      (when (< end-idx (dec idx))
        (ReverseIter. arr cur-leaf rev-leaves (dec idx) end-idx))))

  ;; TODO: reduce protocol? would be faster
  ;; but datascript doesn't use rslice much

  IReversible
  (-rseq [_]
    (let [leaves     (-> '()
                         (clojure.core/conj cur-leaf)
                         (into rev-leaves))
          first-leaf (first leaves)]
      (Iter. (.-keys first-leaf) first-leaf (next leaves) (inc end-idx) (inc idx))))

  ;; ISeek
  ;; (-seek [this key]
  ;;   (-seek this key (.-comparator set)))

  ;; (-seek [this key cmp]
  ;;   (throw (ex-info "seek not impl yet" {})))

  Object
  (toString [this] (pr-str* this))

  IPrintWithWriter
  #_:clj-kondo/ignore
  (-pr-writer [this writer opts]
    (pr-sequential-writer writer pr-writer "(" " " ")" opts (seq this))))

(deftype AsyncIter [^BTSet set left right keys idx]
  IAsyncIter
  (-copy [_ l r]
    (p/let [ks (keys-for set l)]
      (AsyncIter. set l r ks (path-get l 0))))

  IEquiv
  ;; TODO: this is broken
  #_:clj-kondo/ignore
  (-equiv [this other] (equiv-sequential this other))

  ISequential
  ISeqable
  (-seq [this] (when keys this))

  ISeq
  (-first [_]
    (p/do!
     (when keys
       (arrays/aget keys idx))))

  (-rest [this]
    (p/let [n (-next this)]
      (or n ())))

  INext
  (-next [this]
    (p/do!
     (when keys
       (if (< (inc idx) (arrays/alength keys))
         ;; can use cached array to move forward
         (let [left' (path-inc left)]
           (when (path-lt left' right)
             (AsyncIter. set left' right keys (inc idx))))
         (p/let [left' (next-path set left)]
           (when (path-lt left' right)
             (-copy this left' right)))))))

  IReduce
  (-reduce [this f]
    (p/do!
     (if (nil? keys)
       (f)
       (p/let [first (-first this)
               next  (-next this)]
         (if (some? next)
           (-reduce next f first)
           first)))))

  (-reduce [_ f start]
    (p/loop [left left
             keys keys
             idx  idx
             acc  start]
      (if (nil? keys)
        acc
        (let [new-acc (f acc (arrays/aget keys idx))]
          (cond
            (reduced? new-acc)
            @new-acc

            (< (inc idx) (arrays/alength keys)) ;; can use cached array to move forward
            (let [left' (path-inc left)]
              (if (path-lt left' right)
                (p/recur left' keys (inc idx) new-acc)
                new-acc))

            :else
            (p/let [left' (next-path set left)]
              (if (path-lt left' right)
                (p/recur left' (keys-for set left') (path-get left' 0) new-acc)
                new-acc)))))))

  IReversible
  (-rseq [_]
    (p/do!
     (when keys
       (p/let [pl (prev-path set left)
               pr (prev-path set right)]
         (async-riter set pl pr)))))

  ISeek
  (-seek [this key]
    (-seek this key (.-comparator set)))

  (-seek [this key cmp]
    (p/do!
     (cond
       (nil? key)
       (throw (js/Error. "seek can't be called with a nil key!"))

       (nat-int? (cmp (arrays/aget keys idx) key))
       this

       :else
       (p/let [left' (-seek* set key cmp)]
         (when (some? left')
           (p/let [ks (keys-for set left')]
             (AsyncIter. set left' right ks (path-get left' 0))))))))

  Object
  (toString [this] (pr-str* this))

  IPrintWithWriter
  #_:clj-kondo/ignore
  (-pr-writer [_ writer opts]
    (pr-sequential-writer writer pr-writer "(" " " ")" opts '("AsyncIter") #_(seq this))))

(defn async-iter [set left right]
  (p/let [ks (keys-for set left)]
    (AsyncIter. set left right ks (path-get left 0))))

;; reverse iteration

(deftype AsyncReverseIter [^BTSet set left right keys idx]
  IAsyncIter
  (-copy [_ l r]
    (p/let [ks (keys-for set r)]
      (AsyncReverseIter. set l r ks (path-get r 0))))

  IEquiv
  ;; TODO: broken
  #_:clj-kondo/ignore
  (-equiv [this other] (equiv-sequential this other))

  ISequential
  ISeqable
  (-seq [this] (when keys this))

  ISeq
  (-first [_]
    (p/do!
     (when keys
       (arrays/aget keys idx))))

  (-rest [this]
    (p/let [n (-next this)]
      (or n ())))

  INext
  (-next [this]
    (p/do!
     (when keys
       (if (> idx 0)
         ;; can use cached array to advance
         (let [right' (path-dec right)]
           (when (path-lt left right')
             (AsyncReverseIter. set left right' keys (dec idx))))
         (p/let [right' (prev-path set right)]
           (when (path-lt left right')
             (-copy this left right')))))))

  IReduce
  (-reduce [this f]
    (p/do!
     (if (nil? keys)
       (f)
       (p/let [first (-first this)
               next  (-next this)]
         (if (some? next)
           (-reduce next f first)
           first)))))

  (-reduce [_ f start]
    (p/loop [right right
             keys  keys
             idx   idx
             acc   start]
      (if (nil? keys)
        acc
        (let [new-acc (f acc (arrays/aget keys idx))]
          (cond
            (reduced? new-acc)
            @new-acc

            (> idx 0) ;; can use cached array to move forward
            (let [right' (path-dec right)]
              (if (path-lt left right')
                (p/recur right' keys (dec idx) new-acc)
                new-acc))

            :else
            (p/let [right' (prev-path set right)]
              (if (path-lt left right')
                (p/recur right' (keys-for set right') (path-get right' 0) new-acc)
                new-acc)))))))

  IReversible
  (-rseq [_]
    (p/do!
     (when keys
       (p/let [nl (next-path set left)
               nr (next-path set right)]
         (async-iter set nl nr)))))

  ISeek
  (-seek [this key]
    (-seek this key (.-comparator set)))

  (-seek [this key cmp]
    (p/do!
     (cond
       (nil? key)
       (throw (js/Error. "seek can't be called with a nil key!"))

       (nat-int? (cmp key (arrays/aget keys idx)))
       this

       :else
       (p/let [rs     (-rseek* set key cmp)
               right' (prev-path set rs)]
         (when (and
                (nat-int? right')
                (path-lte left right')
                (path-lt  right' right))
           (p/let [ks (keys-for set right')]
             (AsyncReverseIter. set left right' ks (path-get right' 0))))))))

  Object
  (toString [this] (pr-str* this))

  IPrintWithWriter
  #_:clj-kondo/ignore
  (-pr-writer [_ writer opts]
    (pr-sequential-writer writer pr-writer "(" " " ")" opts '("AsyncReverseIter") #_(seq this))))

(defn async-riter [set left right]
  (p/let [ks (keys-for set right)]
    (AsyncReverseIter. set left right ks (path-get right 0))))

;; Slicing

(defn- -seek*
  "Returns path to first element >= key,
   or -1 if all elements in a set < key"
  [^BTSet set key comparator]
  (if (nil? key)
    empty-path
    (mp/loop [node  (-root set)
              path  empty-path
              level (.-shift set)]
      (let [keys-l (node-len node)]
        (if (== 0 level)
          (let [keys (.-keys node)
                idx  (binary-search-l comparator keys (dec keys-l) key)]
            (if (== keys-l idx)
              nil
              (path-set path 0 idx)))
          (let [keys (.-keys node)
                idx  (binary-search-l comparator keys (- keys-l 2) key)]
            (mp/recur
             (node-child node idx (.-_storage set) (.-_settings set))
             (path-set path level idx)
             (dec level))))))))

(defn- -rseek*
  "Returns path to the first element that is > key.
   If all elements in a set are <= key, returns `(-rpath set) + 1`.
   It’s a virtual path that is bigger than any path in a tree"
  [^BTSet set key comparator]
  (if (nil? key)
    (mp/let [root (-root set)
             rp   (-rpath set root empty-path (.-shift set))]
      (path-inc rp))
    (mp/loop [node  (-root set)
              path  empty-path
              level (.-shift set)]
      (let [keys-l (node-len node)]
        (if (== 0 level)
          (let [keys (.-keys node)
                idx  (binary-search-r comparator keys (dec keys-l) key)
                res  (path-set path 0 idx)]
            res)
          (let [keys (.-keys node)
                idx  (binary-search-r comparator keys (- keys-l 2) key)
                res  (path-set path level idx)]
            (mp/recur
             (node-child node idx (.-_storage set) (.-_settings set))
             res
             (dec level))))))))

(defn get-leaves
  "Get the leaf nodes starting at path going until till-path."
  ([^BTSet set path till-path]
   (when (path-lt path till-path)
     (mp/let [root   (-root set)
              level  (.-shift set)
              leaves (get-leaves (.-_storage set) (.-_settings set) root path till-path level)]
       ;; level 0 the root is a leaf
       (if (== 0 level)
         #js [leaves]
         leaves
         ))))
  ([storage settings node path till-path level]
   (if (== 0 level)
     ;; leaf
     node
     ;; inner node
     ;; we could avoid creating an array if it's inside this child but it didn't seem to speed up
     (let [children-len   (arrays/alength (.-pointers ^Node node))
           child-promises #js []]
       ;; fetch all the children in parallel
       (loop [path path]
         ;; if we are past the path, return up
         (when (path-lt path till-path)
           (let [idx (path-get path level)
                 p   (mp/let [child (node-child node idx storage settings)]
                       (get-leaves storage settings child path till-path (dec level)))]
             ;; if we reached the end of this node, return up
             (when (< (inc idx) children-len)
               ;; have to zero out lower levels when inc this level
               ;; really only need to do this on first iteration
               (.push child-promises p)
               (recur (path-set-zero-lower path level (inc idx)))))))
       (mp/let [children (mp/js-all child-promises)]
         ;; if we are at level 1, all of the children are leaves
         (if (== 1 level)
           children
           (.flat children)))))))


(defn- -slice [set key-from key-to comparator]
  (mp/let [path (-seek* set key-from comparator)]
    (when (some? path)
      (mp/let [till-path (-rseek* set key-to comparator)]
        (when (path-lt path till-path)
          #_
          (mp/let [leaves (get-leaves2 set path till-path)
                   first-leaf (arrays/aget leaves 0)]
            (when first-leaf
              (let [end-idx (path-get till-path 0)
                    end-idx (if (== 0 end-idx)
                              (arrays/alength (.-keys (arrays/alast leaves)))
                              end-idx)
                    keys    (.-keys first-leaf)]
                (Iter2. keys (arrays/alength keys) 0 (arrays/alength leaves) leaves (path-get path 0) end-idx))))
          ;; #_
          (mp/let [js-leaves (get-leaves set path till-path)]
            (let [leaves     (vec js-leaves)
                  first-leaf (first leaves)]
              (when first-leaf
                (let [end-idx (path-get till-path 0)
                      end-idx (if (== 0 end-idx)
                                (arrays/alength (.-keys (last leaves)))
                                end-idx)]
                  (Iter. (.-keys first-leaf) first-leaf (next leaves) (path-get path 0) end-idx)))))
          )))))

(defn- arr-map-inplace [f arr]
  (let [len (arrays/alength arr)]
    (loop [i 0]
      (when (< i len)
        (arrays/aset arr i (f (arrays/aget arr i)))
        (recur (inc i))))
    arr))


(defn- arr-partition-approx
  "Splits `arr` into arrays of size between min-len and max-len,
   trying to stick to (min+max)/2"
  [min-len max-len arr]
  (let [chunk-len avg-len
        len       (arrays/alength arr)
        acc       (transient [])]
    (when (pos? len)
      (loop [pos 0]
        (let [rest (- len pos)]
          (cond
            (<= rest max-len)
            (conj! acc (.slice arr pos))
            (>= rest (+ chunk-len min-len))
            (do
              (conj! acc (.slice arr pos (+ pos chunk-len)))
              (recur (+ pos chunk-len)))
            :else
            (let [piece-len (arrays/half rest)]
              (conj! acc (.slice arr pos (+ pos piece-len)))
              (recur (+ pos piece-len)))))))
    (to-array (persistent! acc))))


(defn- sorted-arr-distinct? [arr cmp]
  (let [al (arrays/alength arr)]
    (if (<= al 1)
      true
      (loop [i 1
             p (arrays/aget arr 0)]
        (if (>= i al)
          true
          (let [e (arrays/aget arr i)]
            (if (== 0 (cmp e p))
              false
              (recur (inc i) e))))))))


(defn- sorted-arr-distinct
  "Filter out repetitive values in a sorted array.
   Optimized for no-duplicates case"
  [arr cmp]
  (if (sorted-arr-distinct? arr cmp)
    arr
    (let [al (arrays/alength arr)]
      (loop [acc (transient [(arrays/aget arr 0)])
             i   1
             p   (arrays/aget arr 0)]
        (if (>= i al)
          (into-array (persistent! acc))
          (let [e (arrays/aget arr i)]
            (if (== 0 (cmp e p))
              (recur acc (inc i) e)
              (recur (conj! acc e) (inc i) e))))))))



;; Public interface

(defn conj
  "Like `conj` but may or may not return a promise, depending on if the data is in memory"
  ([^BTSet set key]
   (conj set key (.-comparator set)))
  ([^BTSet set key cmp]
   (mp/let [set-root (-root set)
            roots    (node-conj set-root cmp key (.-_storage set) (.-_settings set))]
     (cond
       ;; tree not changed
       (nil? roots)
       set

       ;; keeping single root
       (== (arrays/alength roots) 1)
       (alter-btset set
                    (arrays/aget roots 0)
                    (.-shift set)
                    (inc (.-cnt set)))

       ;; introducing new root
       :else
       (alter-btset set
                    (Node. (arrays/amap node-lim-key roots)
                           roots
                           ;; in conj, we init nil addresses because the nodes returned are always new
                           (arrays/make-array (arrays/alength roots)))
                    (inc (.-shift set))
                    (inc (.-cnt set)))))))


(defn disj
  "Analogue to [[clojure.core/disj]] with comparator that overrides the one stored in set."
  ([^BTSet set key]
   (disj set key (.-comparator set)))
  ([^BTSet set key cmp]
   (mp/let [set-root  (-root set)
            new-roots (node-disj set-root cmp key true nil nil (.-_storage set) (.-_settings set))]
     (if (nil? new-roots) ;; nothing changed, key wasn't in the set
       set
       (let [new-root (arrays/aget new-roots 0)]
         (if (and (instance? Node new-root)
                  (== 1 (arrays/alength (.-pointers new-root))))

           ;; root has one child, make him new root
           (alter-btset set
                        (node-child new-root 0 (.-_storage set) (.-_settings set))
                        (dec (.-shift set))
                        (dec (.-cnt set)))

           ;; keeping root level
           (alter-btset set
                        new-root
                        (.-shift set)
                        (dec (.-cnt set)))))))))


(defn slice
  "An iterator for part of the set with provided boundaries.
   `(slice set from to)` returns iterator for all Xs where from <= X <= to.
   Optionally pass in comparator that will override the one that set uses. Supports efficient [[clojure.core/rseq]]."
  ([^BTSet set key-from key-to]
   (-slice set key-from key-to (.-comparator set)))
  ([^BTSet set key-from key-to comparator]
   (-slice set key-from key-to comparator)))


(defn rslice
  "A reverse iterator for part of the set with provided boundaries.
   `(rslice set from to)` returns backwards iterator for all Xs where from <= X <= to.
   Optionally pass in comparator that will override the one that set uses. Supports efficient [[clojure.core/rseq]]."
  ([^BTSet set key]
   (mp/let [s (-slice set key key (.-comparator set))]
     (when s
       (rseq s))))
  ([^BTSet set key-from key-to]
   (mp/let [s (-slice set key-to key-from (.-comparator set))]
     (when s
       (rseq s))))
  ([^BTSet set key-from key-to comparator]
   (mp/let [s (-slice set key-to key-from comparator)]
     (when s
       (rseq s)))))


;; TODO: seek is broken and shouldn't work the same way anyhow
(defn seek
  "An efficient way to seek to a specific key in a seq (either returned by [[clojure.core.seq]] or a slice.)
  `(seek (seq set) to)` returns iterator for all Xs where to <= X.
  Optionally pass in comparator that will override the one that set uses."
  ([seq to]
   (-seek seq to))
  ([seq to cmp]
   (-seek seq to cmp)))


(defn from-sorted-array
  "Fast path to create a set if you already have a sorted array of elements on your hands."
  ([cmp arr]
   (from-sorted-array cmp arr (arrays/alength arr) {}))
  ([cmp arr _len]
   (from-sorted-array cmp arr _len {}))
  ([cmp arr _len opts]
   (let [settings (map->settings opts)
         leaves   (->> arr
                       (arr-partition-approx min-len max-len)
                       (arr-map-inplace #(Leaf. %)))
         storage  (:storage opts)]
     (loop [current-level leaves
            shift         0]
       (case (count current-level)
         0 (BTSet. storage (Leaf. (arrays/array)) 0 0 cmp nil uninitialized-hash uninitialized-address settings)
         1 (BTSet. storage (first current-level) shift (arrays/alength arr) cmp nil uninitialized-hash uninitialized-address settings)
         (recur
          (->> current-level
               (arr-partition-approx min-len max-len)
               (arr-map-inplace #(Node. (arrays/amap node-lim-key %) % (arrays/make-array (arrays/alength %)))))
          (inc shift)))))))


(defn from-sequential
  "Create a set with custom comparator and a collection of keys. Useful when you don’t want to call [[clojure.core/apply]] on [[sorted-set-by]]."
  ([cmp seq]
   (from-sequential cmp seq {}))
  ([cmp seq opts]
   (let [arr (-> (into-array seq) (arrays/asort cmp) (sorted-arr-distinct cmp))
         len (arrays/alength arr)]
     (from-sorted-array cmp arr len opts))))


(defn sorted-set*
  "Create a set with custom comparator, metadata and settings"
  [opts]
  (BTSet. (:storage opts) (Leaf. (arrays/array)) 0 0 (or (:cmp opts) compare) (:meta opts) uninitialized-hash uninitialized-address (map->settings opts)))


(defn sorted-set-by
  ([cmp] (BTSet. nil (Leaf. (arrays/array)) 0 0 cmp nil uninitialized-hash uninitialized-address (map->settings {})))
  ([cmp & keys] (from-sequential cmp keys)))


(defn sorted-set
  ([] (sorted-set-by compare))
  ([& keys] (from-sequential compare keys)))


(defn restore-by
  "Constructs lazily-loaded set from storage, root address and custom comparator.
   Supports all operations that normal in-memory impl would,
   will fetch missing nodes by calling IStorage::restore when needed"
  ([cmp address storage]
   (restore-by cmp address storage {}))
  ([cmp address storage {:as opts :keys [set-metadata]}]
   (BTSet. storage nil (:shift set-metadata) (:count set-metadata) cmp nil uninitialized-hash address (map->settings opts))))


(defn restore
  "Constructs lazily-loaded set from storage and root address.
   Supports all operations that normal in-memory impl would,
   will fetch missing nodes by calling IStorage::restore when needed"
  ([address storage]
   (restore-by compare address storage {}))
  ([address storage opts]
   (restore-by compare address storage opts)))


(defn walk-addresses
  "Visit each address used by this set. Usable for cleaning up
   garbage left in storage from previous versions of the set"
  [^BTSet set consume-fn]
  (-walk-addresses set consume-fn))


(defn store
  "Store each not-yet-stored node by calling IStorage::store and remembering
   returned address. Incremental, won’t store same node twice on subsequent calls.
   Returns root address. Remember it and use it for restore"
  ([^BTSet set]
   (-store set))
  ([^BTSet set storage]
   ;; settings are ignored here
   (-store set storage nil)))


(defn set-metadata [^BTSet set]
  {:count (.-cnt set)
   :shift (.-shift set)})


(defn settings [^BTSet set]
  (settings->map (.-_settings set)))
