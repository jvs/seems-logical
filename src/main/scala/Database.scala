package seems.logical

import scala.collection.mutable.ArrayBuffer


class Database(
  private [logical] val datasets: Map[Dataset, Int],
  private [logical] val nodes: Vector[Node],
  private [logical] val edges: Vector[Vector[Edge]],
  private [logical] val time: Long = 0
) {
  def apply(dataset: Dataset): Set[Row] = {
    nodes(datasets(dataset)) match {
      case s: Sink => s.rows.toSet
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

  private [logical] def update(node: Option[Node], time: Long): Database = node match {
    case Some(n) => new Database(datasets, nodes.updated(n.id, n), edges, time)
    case None => this
  }
}


private abstract class Node(val id: Int) {
  def receive(row: Row, isInsert: Boolean, isLeftSide: Boolean, time: Long): Response
}


private case class Edge(receiver: Int, isLeftSide: Boolean)
private case class Broadcast(sender: Int, inserts: List[Row], deletes: List[Row])


private case class Response(
  replacement: Option[Node],
  inserts: List[Row],
  deletes: List[Row] = List()
)


private object run {
  def apply(start: Database, table: Table, row: Row, isInsert: Boolean): Database = {
    val time = start.time + 1
    var current = start
    val broadcasts = ArrayBuffer[Broadcast]()

    val startNode = current.nodes(current.datasets(table))
    val initialResp = startNode.receive(row, isInsert, true, time)
    current = current.update(initialResp.replacement, time)
    broadcasts += Broadcast(startNode.id, initialResp.inserts, initialResp.deletes)

    while (broadcasts.length > 0) {
      val broadcast = broadcasts.remove(broadcasts.length - 1)
      for (edge <- current.edges(broadcast.sender)) {
        val isLeftSide = edge.isLeftSide
        for (row <- broadcast.deletes) {
          val node = current.nodes(edge.receiver)
          val resp = node.receive(row, false, edge.isLeftSide, time)
          current = current.update(resp.replacement, time)
          broadcasts += Broadcast(node.id, resp.inserts, resp.deletes)
        }
        for (row <- broadcast.inserts) {
          val node = current.nodes(edge.receiver)
          val resp = node.receive(row, true, edge.isLeftSide, time)
          current = current.update(resp.replacement, time)
          broadcasts += Broadcast(node.id, resp.inserts, resp.deletes)
        }
      }
    }
    current
  }
}
