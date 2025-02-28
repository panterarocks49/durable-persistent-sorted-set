(ns me.tonsky.persistent-sorted-set.storage)

(defprotocol IStorage
  ;; returns an INode node/leaf
  (restore [this address]
    "Restore a node from a given address")
  (accessed [this address]
    "Optional: notify that address was accessed (useful for caching)")
  (store [this node]
    "Store a node and return its address"))
