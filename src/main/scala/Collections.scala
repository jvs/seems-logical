package seems.logical


case class MultisetMap[K, V](state: Map[K, Multiset[V]] = Map[K, Multiset[V]]()) {
  def apply(key: K): Iterator[V] = {
    if (state.contains(key)) {
      state(key).items.keysIterator
    } else {
      Iterator[V]()
    }
  }

  def update(key: K, value: V, isInsert: Boolean, time: Long): (MultisetMap[K, V], Boolean) = {
    val prev = state.getOrElse(key, Multiset[V]())
    val (next, didChange) = prev.update(value, isInsert, time)
    if (isInsert || next.items.size > 0) {
      (MultisetMap(state + (key -> next)), didChange)
    } else {
      (MultisetMap(state - key), didChange)
    }
  }
}


case class Multiset[A](items: Map[A, (Int, Long)] = Map[A, (Int, Long)]()) {
  def toSet: Set[A] = items.keySet

  def update(item: A, isInsert: Boolean, time: Long): (Multiset[A], Boolean) = {
    if (isInsert) insert(item, time) else remove(item, time)
  }

  private def insert(item: A, time: Long) = {
    val (prevCount, prevTime) = items.getOrElse(item, (0, -1L))
    if (time <= prevTime) {
      (this, false)
    } else {
      val next = items + (item -> (prevCount + 1, time))
      (Multiset(next), prevCount == 0)
    }
  }

  private def remove(item: A, time: Long) = {
    val (prevCount, prevTime) = items.getOrElse(item, (0, -1L))
    val next = if (prevCount == 1) {
      items - item
    } else if (prevCount > 0) {
      items + (item -> (prevCount - 1, time))
    } else {
      items
    }
    (Multiset(next), prevCount == 1)
  }
}
