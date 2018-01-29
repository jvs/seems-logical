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

  def insert(table: Table, rows: Any*): Database = {
    update(table, rows.toVector, true)
  }

  def remove(table: Table, rows: Any*): Database = {
    update(table, rows.toVector, false)
  }

  private def update(table: Table, rows: Vector[Any], isInsert: Boolean): Database = {
    val expected = table.fields.length
    if (rows.length % expected != 0) {
      throw new RuntimeException(
        s"Expected number of values to be divisible by ${expected}."
      )
    }
    rows.grouped(expected).foldLeft(this) {
      case (db, row) => run(db, table, row, isInsert)
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
  deletes: ArrayBuffer[Row],
  visited: Set[Int] = Set()
)

private case class Response(
  replacement: Option[Node],
  inserts: ArrayBuffer[Row],
  deletes: ArrayBuffer[Row]
)


private object run {
  def apply(start: Database, table: Table, row: Row, isInsert: Boolean): Database = {
    var current = start
    val broadcasts = ArrayBuffer[Broadcast]()

    val initialNode = current.nodes(current.datasets(table))
    val initialCast = Broadcast(
      sender = -1,
      inserts = if (isInsert) ArrayBuffer(row) else ArrayBuffer(),
      deletes = if (isInsert) ArrayBuffer() else ArrayBuffer(row)
    )
    val initialResp = initialNode.receive(initialCast, isInsert)
    current = current.update(initialResp.replacement)
    if (initialResp.inserts.nonEmpty || initialResp.deletes.nonEmpty) {
      broadcasts += Broadcast(initialNode.id, initialResp.inserts, initialResp.deletes)
    }

    while (broadcasts.length > 0) {
      val broadcast = broadcasts.remove(broadcasts.length - 1)
      for (edge <- current.edges(broadcast.sender)) {
        val node = current.nodes(edge.receiver)
        val resp = node.receive(broadcast, edge.isLeftSide)
        current = current.update(resp.replacement)
        if (resp.inserts.nonEmpty || resp.deletes.nonEmpty) {
          broadcasts += Broadcast(node.id, resp.inserts, resp.deletes,
            broadcast.visited + node.id)
        }
      }
    }
    current
  }
}
