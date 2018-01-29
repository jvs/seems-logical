package seems.logical

import scala.collection.mutable.ArrayBuffer


case class Summand(on: Vector[Int], rows: Set[Row] = Set[Row]()) {
  def update(
    cast: Broadcast,
    otherSide: Summand,
    isRecursive: Boolean,
    inserted: ArrayBuffer[Row],
    deleted: ArrayBuffer[Row]
  ): Summand = {
    var newRows = rows
    for (raw <- cast.inserts) {
      val row = on.map { i => raw(i) }
      if (!newRows(row) && (!isRecursive || !otherSide.rows(row))) {
        newRows += row
        if (!otherSide.rows(row)) {
          inserted += row
        }
      }
    }

    for (raw <- cast.deletes) {
      val row = on.map { i => raw(i) }
      if (newRows(row)) {
        newRows -= row
        if (!otherSide.rows(row)) {
          deleted += row
        }
      }
    }
    if (newRows eq rows) this else Summand(on, newRows)
  }
}


case class PositiveSide(
  on: Vector[Int],
  groups: Map[Row, Set[Row]] = Map[Row, Set[Row]]())
{
  def apply(key: Row): Set[Row] = groups.getOrElse(key, Set())

  def update(
    cast: Broadcast,
    neg: NegativeSide,
    inserted: ArrayBuffer[Row],
    deleted: ArrayBuffer[Row]
  ): PositiveSide = {
    var newGroups = groups
    for (row <- cast.inserts) {
      val key = on.map { i => row(i) }
      val group = newGroups.getOrElse(key, Set())
      if (!group.contains(row)) {
        newGroups += (key -> (group + row))
        if (!neg(key)) {
          inserted += row
        }
      }
    }

    for (row <- cast.deletes) {
      val key = on.map { i => row(i) }
      if (newGroups.contains(key)) {
        val group = newGroups(key)
        if (group.contains(row)) {
          if (group.size == 1) {
            newGroups -= key
          } else {
            newGroups += (key -> (group - row))
          }
          if (!neg(key)) {
            deleted += row
          }
        }
      }
    }

    return if (newGroups eq groups) this else PositiveSide(on, newGroups)
  }
}


case class NegativeSide(on: Vector[Int], keys: Set[Row] = Set[Row]()) {
  def apply(key: Row) = keys(key)

  def update(
    cast: Broadcast,
    pos: PositiveSide,
    inserted: ArrayBuffer[Row],
    deleted: ArrayBuffer[Row]
  ): NegativeSide = {
    var newKeys = keys
    for (row <- cast.inserts) {
      val key = on.map { i => row(i) }
      if (!newKeys(key)) {
        newKeys += key
        for (other <- pos(key)) {
          deleted += other
        }
      }
    }

    for (row <- cast.deletes) {
      val key = on.map { i => row(i) }
      if (newKeys(key)) {
        newKeys -= key
        for (other <- pos(key)) {
          inserted += other
        }
      }
    }

    if (newKeys eq keys) this else NegativeSide(on, newKeys)
  }
}


case class Multiplicand(
  on: Vector[Int],
  merge: Vector[Int],
  groups: Map[Row, Set[Row]] = Map[Row, Set[Row]]())
{
  def apply(key: Row): Set[Row] = groups.getOrElse(key, Set())

  def update(
    cast: Broadcast,
    otherSide: Multiplicand,
    inserted: ArrayBuffer[Row],
    deleted: ArrayBuffer[Row]
  ): Multiplicand = {
    var newGroups = groups
    for (row <- cast.inserts) {
      val key = on.map { i => row(i) }
      val group = newGroups.getOrElse(key, Set())
      if (!group.contains(row)) {
        newGroups += (key -> (group + row))
        for (other <- otherSide(key)) {
          val paired = row ++ other
          inserted += merge.map { i => paired(i) }
        }
      }
    }

    for (row <- cast.deletes) {
      val key = on.map { i => row(i) }
      val group = newGroups(key) - row
      if (group.size > 0) {
        newGroups += (key -> group)
      } else {
        newGroups -= key
      }
      for (other <- otherSide(key)) {
        val paired = row ++ other
        deleted += merge.map { i => paired(i) }
      }
    }

    return if (newGroups eq groups) this else Multiplicand(on, merge, newGroups)
  }
}


case class RowCounter(rows: Map[Row, Int] = Map[Row, Int]()){
  def contains(row: Row) = rows.contains(row)
  def toSet: Set[Row] = rows.keySet

  def update(cast: Broadcast, inserted: ArrayBuffer[Row], deleted: ArrayBuffer[Row]) = {
    var newRows = rows

    for (row <- cast.inserts) {
      // Increment this row's count.
      val prev = newRows.getOrElse(row, 0)
      newRows += (row -> (prev + 1))
      // If the previous count was zero, then record that we inserted this row
      // by adding it to the "inserted" output buffer.
      if (prev == 0) {
        inserted += row
      }
    }

    for (row <- cast.deletes) {
      val prev = newRows.getOrElse(row, 0)
      // If this row's count was 1, then delete it from the table.
      // (And add it to the "deleted" output buffer.)
      if (prev == 1) {
        newRows -= row
        deleted += row
      } else if (prev > 0) {
        // If the count is positive (basically, not 0), then decrement it.
        newRows += (row -> (prev - 1))
      }
    }

    RowCounter(newRows)
  }
}
