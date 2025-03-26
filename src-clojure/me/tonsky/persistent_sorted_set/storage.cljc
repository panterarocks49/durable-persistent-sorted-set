(ns me.tonsky.persistent-sorted-set.storage)

(defprotocol IStorage
  ;; returns an INode node/leaf
  (restore [this address]
    "Restore a node from a given address")
  (accessed [this address]
    "Optional: notify that address was accessed (useful for caching)")
  (store [this addr+nodes]
    "Store multiple nodes at the same time. Passed a coll of tuples of [addr node/leaf]
    addr might be nil if it's not stored, or an existing address if it's still stored
    Must return a coll of the same length of addrs of the passed in nodes

    This pattern of re-storing already stored nodes is useful if you need larger groupings of items than a single fragment
    The default store-group-size of 1 means that this function will only ever get unstored nodes"))
