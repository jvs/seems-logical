package seems.logical

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


private class Compiler {
  val datasets = mutable.Map[Dataset, Node]()
  val nodes = ArrayBuffer[Node]()
  val edges = ArrayBuffer[mutable.Set[Edge]]()

  def add(datasets: List[Dataset]): this.type = {
    datasets.foreach(compile)
    this
  }

  def compile() = new Database(
    datasets = datasets.mapValues(_.id).toMap,
    nodes = nodes.toVector,
    edges = edges.map(_.toVector).toVector
  )

  private def nextId = nodes.length

  private def compile(term: Term): List[CompiledTerm] = term match {
    case And(a, b) => join(a, b)
    case Changing(term, func) => transform(term, func)
    case Or(a, b) => compile(a) ++ compile(b)
    case Rename(ds, fields) => List(CompiledTerm(compile(ds), fields))
    case Where(term, pred) => filter(term, pred)
    case _ => throw new RuntimeException("not implemented")
  }

  private def compile(dataset: Dataset): Node = dataset match {
    case t: Table => compile(t)
    case v: View => compile(v)
  }

  private def compile(table: Table): Node = {
    datasets.getOrElse(table, add(table, new Source(nextId, Set())))
  }

  private def compile(view: View): Node = {
    if (!datasets.contains(view)) {
      val node = add(view, new Sink(nextId, RowCounter()))
      for (source <- compile(view.body)) {
        connectSink(source, node, view.fields)
      }
    }
    datasets(view)
  }

  private def filter(term: Term, pred: Record => Boolean): List[CompiledTerm] = {
    for (left <- compile(term)) yield {
      val right = add(new Filter(nextId, wrapPredicate(pred, left.schema)))
      connect(left.node, right)
      CompiledTerm(right, left.schema)
    }
  }

  private def wrapPredicate(func: Record => Boolean, schema: Vector[String]): Row => Boolean = {
    (row: Row) => func(new Record(row, schema))
  }

  private def transform(term: Term, func: Record => Record): List[CompiledTerm] = {
    for (left <- compile(term)) yield {
      val right = add(new Tranform(nextId, wrapTransform(func, left.schema)))
      connect(left.node, right)
      CompiledTerm(right, left.schema)
    }
  }

  private def wrapTransform(func: Record => Record, schema: Vector[String]): Row => Row = {
    (row: Row) => {
      val result = func(new Record(row, schema))
      if (result.schema == schema) {
        result.row
      } else {
        val get = result.schema.zip(result.row).toMap
        schema.map { x => get(x) }
      }
    }
  }

  private def join(left: Term, right: Term): List[CompiledTerm] = {
    val compiledLeft = compile(left)
    val compiledRight = compile(right)
    val pairs = for (a <- compiledLeft; b <- compiledRight) yield (a, b)
    pairs.flatMap { case (a, b) => join(a, b) }
  }

  private def join(left: CompiledTerm, right: CompiledTerm): List[CompiledTerm] = {
    val (s1, s2) = (left.schema, right.schema)

    val both = s1.filter { x => s2.contains(x) }
    val onleft = both.map { x => s1.indexOf(x) }
    val onright = both.map { x => s2.indexOf(x) }
    val rest = s2.filter { x => !both.contains(x) }
    val restIndices = rest.map { x => s2.indexOf(x) }
    val schema = s1 ++ rest
    val leftmerge = s1.zipWithIndex.map(_._2) ++ restIndices.map(_ + s1.length)
    val rightmerge = s1.zipWithIndex.map(_._2 + s2.length) ++ restIndices

    val leftside = Side(onleft, leftmerge)
    val rightside = Side(onright, rightmerge)
    val joinNode = add(new Join(nextId, leftside, rightside))

    connect(left.node, joinNode, isLeftSide = true)
    connect(right.node, joinNode, isLeftSide = false)
    List(CompiledTerm(joinNode, schema))
  }

  private def connectSink(source: CompiledTerm, sink: Node, schema: Vector[String]) = {
    val (src, dst) = (source.schema, schema)

    val missing = dst.toSet -- src.toSet
    if (missing.size > 0) {
      throw new SchemaError(s"Cannot connect schema $src to $dst.")
    }

    if (src == dst) {
      connect(source.node, sink)
    } else {
      val cols = dst.map { x => src.indexOf(x) }
      val swizzle = add(new Tranform(nextId, row => cols.map { i => row(i) }))
      connect(source.node, swizzle)
      connect(swizzle, sink)
    }
  }

  private def connect(source: Node, target: Node, isLeftSide: Boolean = true): Unit = {
    edges(source.id) += Edge(target.id, isLeftSide)
  }

  private def add(dataset: Dataset, node: Node): Node = {
    datasets += (dataset -> node)
    add(node)
  }

  private def add(node: Node): Node = {
    if (node.id != nodes.length || node.id != edges.length) {
      throw new RuntimeException("Internal error. Unexpected node.")
    }
    nodes += node
    // Create the adjacency-set for this node.
    edges += mutable.Set[Edge]()
    node
  }
}


private case class CompiledTerm(node: Node, schema: Vector[String])


private class Filter(id: Int, predicate: Row => Boolean) extends Node(id) {
  def receive(cast: Broadcast, isLeftSide: Boolean) = Response(
    replacement = None,
    inserts = cast.inserts.filter(predicate),
    deletes = cast.deletes.filter(predicate)
  )
}


private class Join(id: Int, left: Side, right: Side)
  extends Node(id) with Reset
{
  def reset() = new Join(id, left.reset(), right.reset())

  def receive(cast: Broadcast, isLeftSide: Boolean): Response = {
    val (target, other) = if (isLeftSide) (left, right) else (right, left)
    val inserted = ArrayBuffer[Row]()
    val deleted = ArrayBuffer[Row]()
    val newTarget = target.update(cast, other, inserted, deleted)
    val repl = if (newTarget eq target) None else Some(new Join(
      id = id,
      left = if (isLeftSide) newTarget else left,
      right = if (isLeftSide) right else newTarget
    ))
    Response(repl, inserted, deleted)
  }
}


private class Tranform(id: Int, function: Row => Row) extends Node(id) {
  def receive(cast: Broadcast, isLeftSide: Boolean) = Response(
    replacement = None,
    inserts = cast.inserts.map(function),
    deletes = cast.deletes.map(function)
  )
}


/** Represents a View in a database. It is essentially just a multiset of rows.
  * If you add a row to a Sink multiple times, then you have to remove it the
  * same number of times to really get it out of there. */
private class Sink(id: Int, val rows: RowCounter) extends Node(id) with Reset {
  def reset() = new Sink(id, rows.reset())

  def receive(cast: Broadcast, isLeftSide: Boolean): Response = {
    val inserted = ArrayBuffer[Row]()
    val deleted = ArrayBuffer[Row]()
    val newRows = rows.update(cast, inserted, deleted)
    val repl = if (newRows eq rows) None else Some(new Sink(id, newRows))
    Response(repl, inserted, deleted)
  }
}


/** Represents a Table in a database. It is essentially just a set of rows. */
private class Source(id: Int, val rows: Set[Row]) extends Node(id) {
  def receive(cast: Broadcast, isLeftSide: Boolean): Response = {
    val inserted = cast.inserts.filter(x => !rows(x))
    val deleted = cast.deletes.filter(x => rows(x))
    val isNop = inserted.isEmpty && deleted.isEmpty
    val repl = if (isNop) None else Some(new Source(id, rows ++ inserted -- deleted))
    Response(repl, inserted, deleted)
  }
}


case class Side(
  on: Vector[Int],
  merge: Vector[Int],
  rows: RowCounter = RowCounter(),
  groups: Map[Row, Set[Row]] = Map[Row, Set[Row]]())
{
  def reset() = Side(on, merge, rows.reset(), groups)
  def apply(key: Row): Set[Row] = groups.getOrElse(key, Set())

  def update(
    cast: Broadcast,
    otherGroup: Side,
    inserted: ArrayBuffer[Row],
    deleted: ArrayBuffer[Row]
  ): Side = {
    // Update our row-counter with our new rows.
    val tmpInserted = ArrayBuffer[Row]()
    val tmpDeleted = ArrayBuffer[Row]()
    val newRows = rows.update(cast, tmpInserted, tmpDeleted)

    // If the row-counter didn't change, then we won't change either. Exit early.
    if (newRows eq rows) {
      return this
    }

    var newGroups = groups
    for (row <- tmpInserted) {
      val key = on.map { i => row(i) }
      if (newGroups.contains(key)) {
        newGroups += (key -> (newGroups(key) + row))
      } else {
        newGroups += (key -> Set(row))
      }
      for (other <- otherGroup(key)) {
        val paired = row ++ other
        inserted += merge.map { i => paired(i) }
      }
    }

    for (row <- tmpDeleted) {
      val key = on.map { i => row(i) }
      val group = newGroups(key) - row
      if (group.size > 0) {
        newGroups += (key -> group)
      } else {
        newGroups -= key
      }
      for (other <- otherGroup(key)) {
        val paired = row ++ other
        deleted += merge.map { i => paired(i) }
      }
    }

    return Side(on, merge, newRows, newGroups)
  }
}


case class RowCounter(
  rows: Map[Row, Int] = Map[Row, Int](),
  visited: Set[Row] = Set[Row]()
) {
  def toSet: Set[Row] = rows.keySet
  def reset() = if (visited.isEmpty) this else RowCounter(rows, Set())

  def update(cast: Broadcast, inserted: ArrayBuffer[Row], deleted: ArrayBuffer[Row]) = {
    var newRows = rows
    var newVisited = visited

    for (row <- cast.inserts) {
      // Ignore this row if we've already seen it.
      if (!newVisited(row)) {
        // Record that we've now visited this row.
        newVisited += row
        // Increment this row's count.
        val prev = newRows.getOrElse(row, 0)
        newRows += (row -> (prev + 1))
        // If the previous count was zero, then record that we inserted this row
        // by adding it to the "inserted" output buffer.
        if (prev == 0) {
          inserted += row
        }
      }
    }

    for (row <- cast.deletes) {
      // As above, ignore this row if we've already seen it.
      if (!newVisited(row)) {
        // Record that we've now visited this row.
        newVisited += row
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
    }

    // If we didn't visit any new rows, then we know that nothing changed.
    if (newVisited eq visited) this else RowCounter(newRows, newVisited)
  }
}
