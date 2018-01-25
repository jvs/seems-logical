package seems.logical


case class MultisetMap[K, V](state: Map[K, Multiset[V]] = Map[K, Multiset[V]]()) {
  def apply(key: K): Iterator[V] = {
    if (state.contains(key)) {
      state(key).items.keysIterator
    } else {
      Iterator[V]()
    }
  }

  def update(key: K, value: V, isInsert: Boolean): (MultisetMap[K, V], Boolean) = {
    val prev = state.getOrElse(key, Multiset[V]())
    val (next, didChange) = prev.update(value, isInsert)
    if (isInsert || next.items.size > 0) {
      (MultisetMap(state + (key -> next)), didChange)
    } else {
      (MultisetMap(state - key), didChange)
    }
  }
}


case class Multiset[A](items: Map[A, Int] = Map[A, Int]()) {
  def toSet: Set[A] = items.keySet

  def update(item: A, isInsert: Boolean): (Multiset[A], Boolean) = {
    if (isInsert) insert(item) else remove(item)
  }

  private def insert(item: A) = {
    val prev = items.getOrElse(item, 0)
    val next = items + (item -> (prev + 1))
    (Multiset(next), prev == 0)
  }

  private def remove(item: A) = {
    val prev = items.getOrElse(item, 0)
    val next = if (prev == 1) {
      items - item
    } else if (prev > 0) {
      items + (item -> (prev - 1))
    } else {
      items
    }
    (Multiset(next), prev == 1)
  }
}
