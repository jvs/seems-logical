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
}


private abstract class Node(val id: Int) {
  def receive(row: Row, isInsert: Boolean, isLeftSide: Boolean, time: Long): Response
}


private case class Edge(receiver: Int, isLeftSide: Boolean)
private case class Broadcast(sender: Int, inserts: List[Row], deletes: List[Row])
private case class Packet(edge: Edge, row: Row, isInsert: Boolean)


private case class Response(
  replacement: Option[Node],
  inserts: List[Row],
  deletes: List[Row] = List()
)


private object run {
  def apply(start: Database, table: Table, row: Row, isInsert: Boolean): Database = {
    var current = start
    val packets = ArrayBuffer[Packet](
      Packet(Edge(current.datasets(table), true), row, isInsert)
    )
    val broadcasts = ArrayBuffer[Broadcast]()
    val time = current.time + 1

    var isBusy = true
    while (isBusy) {
      while (packets.length > 0) {
        val packet = packets.remove(packets.length - 1)
        val edge = packet.edge
        val node = current.nodes(edge.receiver)
        val resp = node.receive(packet.row, packet.isInsert, edge.isLeftSide, time)
        resp.replacement.map { repl =>
          current = new Database(
            current.datasets,
            current.nodes.updated(repl.id, repl),
            current.edges,
            time
          )
        }
        broadcasts += Broadcast(node.id, resp.inserts, resp.deletes)
      }

      if (broadcasts.length == 0) {
        isBusy = false
      } else {
        val broadcast = broadcasts.remove(broadcasts.length - 1)
        val edges = current.edges(broadcast.sender)
        for (edge <- edges) {
          for (row <- broadcast.deletes) {
            packets += Packet(edge, row, false)
          }
          for (row <- broadcast.inserts) {
            packets += Packet(edge, row, true)
          }
        }
      }
    }
    current
  }
}
