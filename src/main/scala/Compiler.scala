package seems.logical

import scala.collection.mutable


private class Compiler {
  val datasets = mutable.Map[Dataset, Node]()
  val terms = mutable.Map[Term, Node]()
  val nodes = mutable.ArrayBuffer[Node]()
  val edges = mutable.ArrayBuffer[mutable.ArrayBuffer[Node]]()

  def add(datasets: List[Dataset]): this.type = {
    datasets.map {
      case t: Table => compile(t)
      case v: View => compile(v)
    }
    this
  }

  def compile() = {
    // MUST: Implement this method.
    new Database(Map(), Vector(), Vector())
  }

  private def compile(table: Table): Node = {
    datasets.getOrElse(table, add(table, new Source(nodes.length, Set())))
  }

  private def compile(view: View): Node = {
    datasets.getOrElse(view, add(view, new Sink(nodes.length, Multiset())))
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
    edges += mutable.ArrayBuffer[Node]()
    node
  }
}


private abstract class Node(val id: Int) {
  def receive(row: Row, isInsert: Boolean): Response
}


private case class Response(
  replacements: List[Node],
  inserts: List[Row],
  deletes: List[Row] = List()
)


/** Represents a Table in a database. It is essentially just a set of rows. */
private class Source(id: Int, val rows: Set[Row]) extends Node(id) {
  def receive(row: Row, isInsert: Boolean): Response = {
    (isInsert, rows.contains(row)) match {
      case (true, true) | (false, false) => Response(List(), List(), List())
      case (true, false) => Response(List(new Source(id, rows + row)), List(row), List())
      case (false, true) => Response(List(new Source(id, rows - row)), List(), List(row))
    }
  }
}


/** Represents a View in a database. It is essentially just a multiset of rows.
  * If you add a row to a Sink multiple times, then you have to remove it the
  * same number of times to really get it out of there. */
private class Sink(id: Int, val rows: Multiset[Row]) extends Node(id) {
  def receive(row: Row, isInsert: Boolean): Response = {
    val (newRows, didChange) = rows.update(row, isInsert)
    Response(
      replacements = List(new Sink(id, newRows)),
      inserts = if (didChange && isInsert) List(row) else List(),
      deletes = if (didChange && !isInsert) List() else List(row)
    )
  }
}
