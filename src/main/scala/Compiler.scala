package seems.logical

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


private class Compiler {
  val datasets = mutable.Map[Dataset, CompiledTerm]()
  val nodes = ArrayBuffer[Node]()
  val edges = ArrayBuffer[mutable.Set[Edge]]()

  def accept(datasets: List[Dataset]): this.type = {
    datasets.foreach(compile)
    this
  }

  def run() = new Database(
    datasets = datasets.mapValues(_.node.id).toMap,
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
    case Project(term, fields) => project(compile(term), fields)
    case Rename(term, fields) => CompiledTerm(compile(term).node, fields)
    case Where(term, pred) => filter(compile(term), pred)

    case t: Table => compile(t)
    case s: Statement => compile(s)
    case v: View => compile(v)
  }

  private def compile(table: Table): CompiledTerm = {
    if (!datasets.contains(table)) {
      val node = register(new Source(nextId, Set()))
      datasets += (table -> CompiledTerm(node, table.fields))
    }
    datasets(table)
  }

  private def compile(view: View): CompiledTerm = {
    if (!datasets.contains(view)) {
      val node = register(new Source(nextId, Set()))
      val result = CompiledTerm(node, inferSchema(view))
      datasets += (view -> result)
      val src = compile(view.body)
      if (src.schema != result.schema) {
        throw new SchemaError(
          s"Unexpected schema. Inferred ${result.schema}. Received ${src.schema}"
        )
      }
      connect(src.node, result.node)
    }
    datasets(view)
  }

  private def inferSchema(term: Term): Vector[String] = term match {
    case t: Table => t.fields
    case v: View => inferSchema(v.body)
    case Expanding(_, s, _) => s
    case Project(_, s) => s
    case Rename(_, s) => s
    case Where(t, _) => inferSchema(t)
    case s: Statement if s.select == Vector(NamedColumn("*")) => inferSchema(s.from)
    case s: Statement => s.select.map(_.name)
    case _ => throw new SchemaError(
      s"Schema inference is not implemented for $term"
    )
  }

  private def compile(stmt: Statement): CompiledTerm = {
    val tmp = compile(stmt.from)
    val src = stmt.predicate match {
      case Some(p) => filter(tmp, p)
      case None => tmp
    }

    (stmt.select, stmt.groups) match {
      case (Vector(NamedColumn("*")), Vector()) => return src
      case (Vector(NamedColumn("*")), _) => throw new SchemaError(
        "Cannot use both \"SELECT *\" and \"GROUP_BY\" in the same statement"
      )
      case (cols, Vector()) if cols.exists(_.isInstanceOf[AggregateColumn]) => {
        // SHOULD: Support this, if all of the columns are aggregates.
        throw new SchemaError("Aggregate columns require a \"GROUP_BY\" expression.")
      }
      case (cols, Vector()) => {
        val srcSchema = cols.map {
          case NamedColumn(a) => a
          case AliasColumn(a, _) => a.name
          case _ => throw new SchemaError("Expected non-aggregated columns.")
        }
        val dstSchema = cols.map {
          case NamedColumn(a) => a
          case AliasColumn(_, a) => a
          case _ => throw new SchemaError("Expected non-aggregated columns.")
        }
        return CompiledTerm(adaptSchema(src, srcSchema), dstSchema)
      }
      case (cols, groups) => {
        throw new RuntimeException("WIP")
      }
    }
  }

  private def add(left: CompiledTerm, right: CompiledTerm): CompiledTerm = {
    val schema = left.schema.filter { x => right.schema.contains(x) }
    val node = register(new Add(nextId, Summand(), Summand()))
    connect(adaptSchema(left, schema), node, isLeftSide = true)
    connect(adaptSchema(right, schema), node, isLeftSide = false)
    CompiledTerm(node, schema)
  }

  private def expand(
    left: CompiledTerm,
    schema: Vector[String],
    func: Record => List[Record]
  ): CompiledTerm = {
    val right = register(new Expand(nextId, wrapExpand(func, left.schema, schema)))
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
    val right = register(new Filter(nextId, wrapPredicate(pred, left.schema)))
    connect(left.node, right)
    CompiledTerm(right, left.schema)
  }

  private def wrapPredicate(func: Record => Boolean, schema: Vector[String]): Row => Boolean = {
    (row: Row) => func(new Record(row, schema))
  }

  private def transform(left: CompiledTerm, func: Record => Record): CompiledTerm = {
    val right = register(new Transform(nextId, wrapTransform(func, left.schema)))
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
    val node = register(new Multiply(nextId, leftside, rightside))

    connect(left.node, node, isLeftSide = true)
    connect(right.node, node, isLeftSide = false)
    CompiledTerm(node, schema)
  }

  private def project(term: CompiledTerm, schema: Vector[String]) = {
    CompiledTerm(adaptSchema(term, schema), schema)
  }

  private def subtract(left: CompiledTerm, right: CompiledTerm): CompiledTerm = {
    val (s1, s2) = (left.schema, right.schema)
    val both = s1.filter { x => s2.contains(x) }
    val onleft = both.map { x => s1.indexOf(x) }
    val onright = both.map { x => s2.indexOf(x) }
    val sub = register(new Subtract(nextId, PositiveSide(onleft), NegativeSide(onright)))
    connect(left.node, sub, isLeftSide = true)
    connect(right.node, sub, isLeftSide = false)
    CompiledTerm(sub, left.schema)
  }

  private def adaptSchema(term: CompiledTerm, schema: Vector[String]): Node = {
    val missing = schema.toSet -- term.schema.toSet
    if (missing.nonEmpty) {
      throw new SchemaError(s"Cannot connect schema ${term.schema} to $schema.")
    }

    if (term.schema == schema) {
      term.node
    } else {
      val cols = schema.map { x => term.schema.indexOf(x) }
      val adapter = register(new Transform(nextId, row => cols.map { i => row(i) }))
      connect(term.node, adapter)
      adapter
    }
  }

  private def connect(source: Node, target: Node, isLeftSide: Boolean = true): Unit = {
    edges(source.id) += Edge(target.id, isLeftSide)
  }

  private def register(node: Node): Node = {
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


private class Add(id: Int, val left: Summand, val right: Summand) extends Node(id) {
  def contains(row: Row) = left.contains(row) || right.contains(row)

  def receive(cast: Broadcast, isLeftSide: Boolean): Response = {
    val (target, other) = if (isLeftSide) (left, right) else (right, left)
    val inserted = ArrayBuffer[Row]()
    val deleted = ArrayBuffer[Row]()
    val maybeDeleted = ArrayBuffer[Row]()
    val newTarget = target.update(cast, other, inserted, deleted, maybeDeleted)
    val repl = new Add(
      id = id,
      left = if (isLeftSide) newTarget else left,
      right = if (isLeftSide) right else newTarget
    )
    Response(Some(repl), inserted, deleted, maybeDeleted)
  }
}


private class Expand(
  id: Int,
  function: Row => List[Row],
  output: RowCounter = RowCounter()
) extends Node(id) {
  def contains(row: Row) = output.contains(row)

  def receive(cast: Broadcast, isLeftSide: Boolean) = {
    val cooked = cast.copy(
      inserts = cast.inserts.flatMap(function),
      deletes = cast.deletes.flatMap(function)
    )
    val inserted = ArrayBuffer[Row]()
    val deleted = ArrayBuffer[Row]()
    val maybeDeleted = ArrayBuffer[Row]()
    val newOutput = output.update(cooked, inserted, deleted, maybeDeleted)
    Response(Some(new Expand(id, function, newOutput)), inserted, deleted, maybeDeleted)
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
  def contains(row: Row) = output.contains(row)

  def receive(cast: Broadcast, isLeftSide: Boolean) = {
    val cooked = cast.copy(
      inserts = cast.inserts.map(function),
      deletes = cast.deletes.map(function)
    )
    val inserted = ArrayBuffer[Row]()
    val deleted = ArrayBuffer[Row]()
    val maybeDeleted = ArrayBuffer[Row]()
    val newOutput = output.update(cooked, inserted, deleted, maybeDeleted)
    val repl = new Transform(id, function, newOutput)
    Response(Some(repl), inserted, deleted, maybeDeleted)
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
