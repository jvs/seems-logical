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

  private def compile(term: Term): CompiledTerm = term match {
    case And(a, b) => multiply(compile(a), compile(b))
    case ButNot(a, b) => subtract(compile(a), compile(b))
    case Changing(term, func) => transform(compile(term), func)
    case Expanding(term, schema, func) => expand(compile(term), schema, func)
    case Or(a, b) => add(compile(a), compile(b))
    case Rename(ds, fields) => CompiledTerm(compile(ds), fields)
    case Where(term, pred) => filter(compile(term), pred)
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
      val viewNode = add(view, new Source(nextId, Set()))
      connectView(compile(view.body), viewNode, view.fields)
    }
    datasets(view)
  }

  private def add(left: CompiledTerm, right: CompiledTerm): CompiledTerm = {
    val (s1, s2) = (left.schema, right.schema)
    val schema = s1.filter { x => s2.contains(x) }
    val keepleft = schema.map { x => s1.indexOf(x) }
    val keepright = schema.map { x => s2.indexOf(x) }
    val node = add(new Add(nextId, Summand(keepleft), Summand(keepright)))
    connect(left.node, node, isLeftSide = true)
    connect(right.node, node, isLeftSide = false)
    CompiledTerm(node, schema)
  }

  private def expand(
    left: CompiledTerm,
    schema: Vector[String],
    func: Record => List[Record]
  ): CompiledTerm = {
    val right = add(new Expand(nextId, wrapExpand(func, left.schema, schema)))
    connect(left.node, right)
    CompiledTerm(right, schema)
  }

  private def wrapExpand(
    func: Record => List[Record],
    inputSchema: Vector[String],
    outputSchema: Vector[String]
  ): Row => List[Row] = {
    (row: Row) => {
      val records = func(new Record(row, inputSchema))
      records.map { rec =>
        if (rec.schema == outputSchema) {
          rec.row
        } else {
          val get = rec.schema.zip(rec.row).toMap
          outputSchema.map { x => get(x) }
        }
      }
    }
  }

  private def filter(left: CompiledTerm, pred: Record => Boolean): CompiledTerm = {
    val right = add(new Filter(nextId, wrapPredicate(pred, left.schema)))
    connect(left.node, right)
    CompiledTerm(right, left.schema)
  }

  private def wrapPredicate(func: Record => Boolean, schema: Vector[String]): Row => Boolean = {
    (row: Row) => func(new Record(row, schema))
  }

  private def transform(left: CompiledTerm, func: Record => Record): CompiledTerm = {
    val right = add(new Transform(nextId, wrapTransform(func, left.schema)))
    connect(left.node, right)
    CompiledTerm(right, left.schema)
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

  private def multiply(left: CompiledTerm, right: CompiledTerm): CompiledTerm = {
    val (s1, s2) = (left.schema, right.schema)

    val both = s1.filter { x => s2.contains(x) }
    val onleft = both.map { x => s1.indexOf(x) }
    val onright = both.map { x => s2.indexOf(x) }
    val rest = s2.filter { x => !both.contains(x) }
    val restIndices = rest.map { x => s2.indexOf(x) }
    val schema = s1 ++ rest
    val leftmerge = s1.zipWithIndex.map(_._2) ++ restIndices.map(_ + s1.length)
    val rightmerge = s1.zipWithIndex.map(_._2 + s2.length) ++ restIndices

    val leftside = Multiplicand(onleft, leftmerge)
    val rightside = Multiplicand(onright, rightmerge)
    val node = add(new Multiply(nextId, leftside, rightside))

    connect(left.node, node, isLeftSide = true)
    connect(right.node, node, isLeftSide = false)
    CompiledTerm(node, schema)
  }

  private def subtract(left: CompiledTerm, right: CompiledTerm): CompiledTerm = {
    val (s1, s2) = (left.schema, right.schema)
    val both = s1.filter { x => s2.contains(x) }
    val onleft = both.map { x => s1.indexOf(x) }
    val onright = both.map { x => s2.indexOf(x) }
    val sub = add(new Subtract(nextId, PositiveSide(onleft), NegativeSide(onright)))
    connect(left.node, sub, isLeftSide = true)
    connect(right.node, sub, isLeftSide = false)
    CompiledTerm(sub, left.schema)
  }

  private def connectView(term: CompiledTerm, viewNode: Node, schema: Vector[String]) = {
    val (src, dst) = (term.schema, schema)

    val missing = dst.toSet -- src.toSet
    if (missing.size > 0) {
      throw new SchemaError(s"Cannot connect schema $src to $dst.")
    }

    if (src == dst) {
      connect(term.node, viewNode)
    } else {
      val cols = dst.map { x => src.indexOf(x) }
      val swizzle = add(new Transform(nextId, row => cols.map { i => row(i) }))
      connect(term.node, swizzle)
      connect(swizzle, viewNode)
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


private class Add(id: Int, left: Summand, right: Summand) extends Node(id) {
  def receive(cast: Broadcast, isLeftSide: Boolean): Response = {
    val (target, other) = if (isLeftSide) (left, right) else (right, left)
    val isRecursive = cast.visited(id)
    val inserted = ArrayBuffer[Row]()
    val deleted = ArrayBuffer[Row]()
    val newTarget = target.update(cast, other, isRecursive, inserted, deleted)
    val repl = if (newTarget eq target) None else Some(new Add(
      id = id,
      left = if (isLeftSide) newTarget else left,
      right = if (isLeftSide) right else newTarget
    ))
    Response(repl, inserted, deleted)
  }
}


private class Expand(
  id: Int,
  function: Row => List[Row],
  output: RowCounter = RowCounter()
) extends Node(id) {
  def receive(cast: Broadcast, isLeftSide: Boolean) = {
    val cooked = cast.copy(
      inserts = cast.inserts.flatMap(function),
      deletes = cast.deletes.flatMap(function)
    )
    val inserted = ArrayBuffer[Row]()
    val deleted = ArrayBuffer[Row]()
    val newOutput = output.update(cooked, inserted, deleted)
    Response(Some(new Expand(id, function, newOutput)), inserted, deleted)
  }
}


private class Filter(id: Int, predicate: Row => Boolean) extends Node(id) {
  def receive(cast: Broadcast, isLeftSide: Boolean) = Response(
    replacement = None,
    inserts = cast.inserts.filter(predicate),
    deletes = cast.deletes.filter(predicate)
  )
}


private class Multiply(id: Int, left: Multiplicand, right: Multiplicand) extends Node(id) {
  def receive(cast: Broadcast, isLeftSide: Boolean): Response = {
    val (target, other) = if (isLeftSide) (left, right) else (right, left)
    val inserted = ArrayBuffer[Row]()
    val deleted = ArrayBuffer[Row]()
    val newTarget = target.update(cast, other, inserted, deleted)
    val repl = if (newTarget eq target) None else Some(new Multiply(
      id = id,
      left = if (isLeftSide) newTarget else left,
      right = if (isLeftSide) right else newTarget
    ))
    Response(repl, inserted, deleted)
  }
}


private class Transform(
  id: Int,
  function: Row => Row,
  output: RowCounter = RowCounter()
) extends Node(id) {
  def receive(cast: Broadcast, isLeftSide: Boolean) = {
    val cooked = cast.copy(
      inserts = cast.inserts.map(function),
      deletes = cast.deletes.map(function)
    )
    val inserted = ArrayBuffer[Row]()
    val deleted = ArrayBuffer[Row]()
    val newOutput = output.update(cooked, inserted, deleted)
    Response(Some(new Transform(id, function, newOutput)), inserted, deleted)
  }
}


/** Represents a Table or a View in a database. It is just a set of rows. */
private class Source(id: Int, val rows: Set[Row]) extends Node(id) {
  def receive(cast: Broadcast, isLeftSide: Boolean): Response = {
    val inserted = cast.inserts.filter(x => !rows(x))
    val deleted = cast.deletes.filter(x => rows(x))
    val isNop = inserted.isEmpty && deleted.isEmpty
    val repl = if (isNop) None else Some(new Source(id, rows ++ inserted -- deleted))
    Response(repl, inserted, deleted)
  }
}


private class Subtract(id: Int, pos: PositiveSide, neg: NegativeSide) extends Node(id) {
  def receive(cast: Broadcast, isLeftSide: Boolean): Response = {
    val inserted = ArrayBuffer[Row]()
    val deleted = ArrayBuffer[Row]()
    val repl = if (isLeftSide) {
      val newPos = pos.update(cast, neg, inserted, deleted)
      if (newPos eq pos) None else Some(new Subtract(id, newPos, neg))
    } else {
      val newNeg = neg.update(cast, pos, inserted, deleted)
      if (newNeg eq neg) None else Some(new Subtract(id, pos, newNeg))
    }
    Response(repl, inserted, deleted)
  }
}
