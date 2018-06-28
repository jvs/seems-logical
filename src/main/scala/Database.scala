package seems.logical

import scala.collection.mutable.{ArrayBuffer, Queue}


class Database(
  private [logical] val datasets: Map[Dataset, Int],
  private [logical] val nodes: Vector[Node],
  private [logical] val edges: Vector[Vector[Edge]])
{
  def datasetDefs = datasets.keySet
  def tableDefs = datasets.keySet.collect { case x: Table => x }

  def apply(dataset: Dataset): Set[Row] = {
    nodes(datasets(dataset)) match {
      case s: Source => s.rows
    }
  }

  def apply(stmt: InsertStatement): Database = {
    update(stmt.table, stmt.values, isInsert = true)
  }

  def apply(stmt: DeleteStatement): Database = {
    update(stmt.table, stmt.values, isInsert = false)
  }

  def THEN(stmt: InsertStatement) = apply(stmt)
  def THEN(stmt: DeleteStatement) = apply(stmt)

  private def update(table: Table, values: Vector[Any], isInsert: Boolean): Database = {
    val expected = table.schema.length
    if (values.length % expected != 0) {
      throw new RuntimeException(
        s"Expected number of values to be divisible by ${expected}."
      )
    }
    values.grouped(expected).foldLeft(this) {
      case (db, row) => transact(db, table, row, isInsert)
    }
  }

  private [logical] def update(node: Option[Node]): Database = node match {
    case Some(n) => new Database(datasets, nodes.updated(n.id, n), edges)
    case None => this
  }
}


object Database {
  def apply(datasets: Dataset*): Database = {
    new Compiler().accept(datasets).run()
  }
}


private abstract class Node(val id: Int) {
  def receive(cast: Broadcast, isLeftSide: Boolean): Response
}

private case class Edge(receiver: Int, isLeftSide: Boolean)

private case class Broadcast(
  sender: Int,
  inserts: ArrayBuffer[Row],
  deletes: ArrayBuffer[Row]
)

private case class Response(
  replacement: Option[Node],
  inserts: ArrayBuffer[Row],
  deletes: ArrayBuffer[Row],
  speculativeDeletes: ArrayBuffer[Row] = ArrayBuffer()
)


object transact {
  def apply(database: Database, table: Table, row: Row, isInsert: Boolean): Database = {
    val node = database.nodes(database.datasets(table))
    val cast = Broadcast(
      sender = -1,
      inserts = if (isInsert) ArrayBuffer(row) else ArrayBuffer(),
      deletes = if (isInsert) ArrayBuffer() else ArrayBuffer(row)
    )
    val resp = node.receive(cast, isInsert)
    val next = database.update(resp.replacement)
    if (resp.inserts.nonEmpty || resp.deletes.nonEmpty) {
      apply(next, Broadcast(node.id, resp.inserts, resp.deletes))
    } else {
      next
    }
  }

  def apply(database: Database, broadcast: Broadcast): Database = {
    var current = database
    val broadcasts = Queue[Broadcast](broadcast)
    while (broadcasts.nonEmpty) {
      val cast = broadcasts.dequeue()
      var specdels = List[(Int, ArrayBuffer[Row])]()

      for (edge <- current.edges(cast.sender)) {
        val node = current.nodes(edge.receiver)
        val resp = node.receive(cast, edge.isLeftSide)
        current = current.update(resp.replacement)
        if (resp.inserts.nonEmpty || resp.deletes.nonEmpty) {
          broadcasts.enqueue(Broadcast(node.id, resp.inserts, resp.deletes))
        }
        // SHOULD: Make a Broadcast variant that represents a speculative-delete.
        // That way the changes can happen in the same sequence as the others.
        if (resp.speculativeDeletes.nonEmpty) {
          specdels = specdels :+ (node.id -> resp.speculativeDeletes)
        }
      }

      // Flush all the pending speculative-deletes.
      for ((nodeId, rows) <- specdels) {
        for (row <- rows) {
          current = speculativelyDelete(current, nodeId, row)
        }
      }
    }
    current
  }

  private def speculativelyDelete(database: Database, nodeId: Int, row: Row) = {
    val cast = Broadcast(nodeId, ArrayBuffer(), ArrayBuffer(row))
    val next = apply(database, cast)

    val didDelete = next.nodes(nodeId) match {
      case a: Add => !a.contains(row)
      case a: Expand => !a.contains(row)
      case a: Transform => !a.contains(row)
      case _ => false
    }

    if (didDelete) next else database
  }
}
