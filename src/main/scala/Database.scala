package seems.logical

import scala.collection.mutable.ArrayBuffer


private case class Broadcast(
  sender: Int,
  inserts: List[Row],
  deletes: List[Row]
)


private case class Packet(
  receiver: Int,
  row: Row,
  isInsert: Boolean
)


class Database(
  private [logical] val datasets: Map[Dataset, Int],
  private [logical] val nodes: Vector[Node],
  private [logical] val edges: Vector[Vector[Int]]
) {
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
      case (db, row) => run(this, table, row, isInsert)
    }
  }
}


private object run {
  def apply(start: Database, table: Table, row: Row, isInsert: Boolean): Database = {
    var current = start
    val packets = ArrayBuffer[Packet]()
    val broadcasts = ArrayBuffer[Broadcast](Broadcast(
      sender = start.datasets(table),
      inserts = if (isInsert) List(row) else List(),
      deletes = if (isInsert) List() else List(row)
    ))

    var isBusy = true
    while (isBusy) {
      while (packets.length > 0) {
        val packet = packets.remove(packets.length - 1)
        val node = current.nodes(packet.receiver)
        val resp = node.receive(packet.row, packet.isInsert)
        var nodes = current.nodes
        for (repl <- resp.replacements) {
          nodes = nodes.updated(repl.id, repl)
        }
        current = new Database(current.datasets, nodes, current.edges)
        broadcasts += Broadcast(node.id, resp.inserts, resp.deletes)
      }

      if (broadcasts.length == 0) {
        isBusy = false
      } else {
        val broadcast = broadcasts.remove(broadcasts.length - 1)
        val receivers = current.edges(broadcast.sender)
        for (receiver <- receivers) {
          for (row <- broadcast.deletes) {
            packets += Packet(receiver, row, false)
          }
          for (row <- broadcast.inserts) {
            packets += Packet(receiver, row, true)
          }
        }
      }
    }
    current
  }
}
