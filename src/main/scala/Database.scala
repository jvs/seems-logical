package seems.logical

import scala.collection.mutable.ArrayBuffer


class Database(
  private [logical] val datasets: Map[Dataset, Int],
  private [logical] val nodes: Vector[Node],
  private [logical] val edges: Vector[Vector[Edge]])
{
  def apply(dataset: Dataset): Set[Row] = {
    nodes(datasets(dataset)) match {
      case s: Source => s.rows
    }
  }

  case class into(table: Table) {
    def insert(rows: Any*): Database = update(table, rows.toVector, true)
  }

  case class from(table: Table) {
    def remove(rows: Any*): Database = update(table, rows.toVector, false)
  }

  private def update(table: Table, rows: Vector[Any], isInsert: Boolean): Database = {
    val expected = table.schema.length
    if (rows.length % expected != 0) {
      throw new RuntimeException(
        s"Expected number of values to be divisible by ${expected}."
      )
    }
    rows.grouped(expected).foldLeft(this) {
      case (db, row) => transact(db, table, row, isInsert)
    }
  }

  private [logical] def update(node: Option[Node]): Database = node match {
    case Some(n) => new Database(datasets, nodes.updated(n.id, n), edges)
    case None => this
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
  maybeDeletes: ArrayBuffer[Row] = ArrayBuffer()
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
    val broadcasts = ArrayBuffer[Broadcast](broadcast)
    while (broadcasts.length > 0) {
      val cast = broadcasts.remove(broadcasts.length - 1)
      for (edge <- current.edges(cast.sender)) {
        val node = current.nodes(edge.receiver)
        val resp = node.receive(cast, edge.isLeftSide)
        current = current.update(resp.replacement)
        if (resp.inserts.nonEmpty || resp.deletes.nonEmpty) {
          broadcasts += Broadcast(node.id, resp.inserts, resp.deletes)
        }
        if (resp.maybeDeletes.nonEmpty) {
          current = maybeDelete(current, node.id, resp.maybeDeletes)
        }
      }
    }
    current
  }

  private def maybeDelete(
    database: Database,
    originator: Int,
    rows: ArrayBuffer[Row]
  ): Database = {
    var current = database
    for (row <- rows) {
      val rollback = current
      val cast = Broadcast(originator, ArrayBuffer(), ArrayBuffer(row))
      val next = apply(database, cast)
      current = if (didDelete(next, originator, row)) next else rollback
    }
    current
  }

  private def didDelete(database: Database, nodeId: Int, row: Row): Boolean = {
    database.nodes(nodeId) match {
      case a: Add => !a.contains(row)
      case a: Expand => !a.contains(row)
      case a: Transform => !a.contains(row)
      case _ => false
    }
  }
}
