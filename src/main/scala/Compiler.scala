package seems.logical

import scala.collection.mutable


private class Compiler {
  val datasets = mutable.Map[Dataset, Node]()
  val nodes = mutable.ArrayBuffer[Node]()
  val edges = mutable.ArrayBuffer[mutable.Set[Edge]]()

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
      val node = add(view, new Sink(nextId, Multiset()))
      for (source <- compile(view.body)) {
        connectSink(source, node, view.fields)
      }
    }
    datasets(view)
  }

  private def filter(term: Term, pred: Row => Boolean): List[CompiledTerm] = {
    for (left <- compile(term)) yield {
      val right = add(new Filter(nextId, pred))
      connect(left.node, right)
      CompiledTerm(right, left.schema)
    }
  }

  private def transform(term: Term, func: Row => Row): List[CompiledTerm] = {
    for (left <- compile(term)) yield {
      val right = add(new Tranform(nextId, func))
      connect(left.node, right)
      CompiledTerm(right, left.schema)
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
    val schema = s1 ++ rest
    val merge = s1.zipWithIndex.map(_._2) ++ rest
      .map { x => s1.length + s2.indexOf(x) }

    val leftside = Grouping(onleft, MultisetMap[Row, Row]())
    val rightside = Grouping(onright, MultisetMap[Row, Row]())
    val joiner = add(new Join(nextId, leftside, rightside, merge))

    connect(left.node, joiner, isLeftSide = true)
    connect(right.node, joiner, isLeftSide = false)
    List(CompiledTerm(joiner, schema))
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
  def receive(row: Row, isInsert: Boolean, isLeftSide: Boolean) = Response(
    replacement = None,
    inserts = if (isInsert && predicate(row)) Some(row) else None,
    deletes = if (!isInsert && predicate(row)) Some(row) else None
  )
}


private class Join(id: Int, left: Grouping, right: Grouping, merge: Vector[Int])
  extends Node(id) with Reset
{
  def reset() = new Join(id, left.reset(), right.reset(), merge)

  def receive(row: Row, isInsert: Boolean, isLeftSide: Boolean): Response = {
    val (target, source) = if (isLeftSide) (left, right) else (right, left)
    val key = target.on.map { i => row(i) }
    val (updatedRows, didChange) = target.rows.update(key, row, isInsert)
    val updatedSide = Grouping(target.on, updatedRows)

    val rows = if (didChange) {
      source.rows(key).map { other =>
        val full = if (isLeftSide) row ++ other else other ++ row
        merge.map { i => full(i) }
      }.toList
    } else {
      List()
    }

    Response(
      replacement = Some(new Join(
        id = id,
        left = if (isLeftSide) updatedSide else left,
        right = if (isLeftSide) right else updatedSide,
        merge = merge
      )),
      inserts = if (isInsert) rows else None,
      deletes = if (isInsert) None else rows
    )
  }
}


private case class Grouping(on: Vector[Int], rows: MultisetMap[Row, Row]) {
  def reset() = Grouping(on, rows.reset())
}


private class Tranform(id: Int, function: Row => Row) extends Node(id) {
  def receive(row: Row, isInsert: Boolean, isLeftSide: Boolean) = Response(
    replacement = None,
    inserts = if (isInsert) Some(function(row)) else None,
    deletes = if (isInsert) None else Some(function(row))
  )
}


/** Represents a View in a database. It is essentially just a multiset of rows.
  * If you add a row to a Sink multiple times, then you have to remove it the
  * same number of times to really get it out of there. */
private class Sink(id: Int, val rows: Multiset[Row]) extends Node(id) with Reset {
  def reset() = new Sink(id, rows.reset())

  def receive(row: Row, isInsert: Boolean, isLeftSide: Boolean): Response = {
    val (newRows, didChange) = rows.update(row, isInsert)
    Response(
      replacement = Some(new Sink(id, newRows)),
      inserts = if (didChange && isInsert) Some(row) else None,
      deletes = if (didChange && !isInsert) Some(row) else None
    )
  }
}


/** Represents a Table in a database. It is essentially just a set of rows. */
private class Source(id: Int, val rows: Set[Row]) extends Node(id) {
  def receive(row: Row, isInsert: Boolean, isLeftSide: Boolean): Response = {
    (isInsert, rows.contains(row)) match {
      case (true, true) | (false, false) => Response(None, None, None)
      case (true, false) => Response(Some(new Source(id, rows + row)), Some(row), None)
      case (false, true) => Response(Some(new Source(id, rows - row)), None, Some(row))
    }
  }
}
