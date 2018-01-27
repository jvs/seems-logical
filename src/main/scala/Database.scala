package seems.logical

import scala.collection.mutable.ArrayBuffer


class Database(
  private [logical] val datasets: Map[Dataset, Int],
  private [logical] val nodes: Vector[Node],
  private [logical] val edges: Vector[Vector[Edge]]
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

  private [logical] def reset(): Database = {
    val newNodes = nodes.foldLeft(nodes) {
      case (acc, r: Reset) => acc.updated(r.id, r.reset())
      case (acc, _) => acc
    }
    new Database(datasets, newNodes, edges)
  }

  private [logical] def update(node: Option[Node]): Database = node match {
    case Some(n) => new Database(datasets, nodes.updated(n.id, n), edges)
    case None => this
  }
}


private abstract class Node(val id: Int) {
  def receive(row: Row, isInsert: Boolean, isLeftSide: Boolean): Response
}

private trait Reset {
  val id: Int
  def reset(): Node
}

private case class Edge(receiver: Int, isLeftSide: Boolean)
private case class Broadcast(sender: Int, inserts: Iterable[Row], deletes: Iterable[Row])

private case class Response(
  replacement: Option[Node],
  inserts: Iterable[Row],
  deletes: Iterable[Row] = None
)


private object run {
  def apply(start: Database, table: Table, row: Row, isInsert: Boolean): Database = {
    var current = start.reset()
    val broadcasts = ArrayBuffer[Broadcast]()

    val startNode = current.nodes(current.datasets(table))
    val initialResp = startNode.receive(row, isInsert, true)
    current = current.update(initialResp.replacement)
    broadcasts += Broadcast(startNode.id, initialResp.inserts, initialResp.deletes)

    while (broadcasts.length > 0) {
      val broadcast = broadcasts.remove(broadcasts.length - 1)
      for (edge <- current.edges(broadcast.sender)) {
        val isLeftSide = edge.isLeftSide
        for (row <- broadcast.deletes) {
          val node = current.nodes(edge.receiver)
          val resp = node.receive(row, false, edge.isLeftSide)
          current = current.update(resp.replacement)
          broadcasts += Broadcast(node.id, resp.inserts, resp.deletes)
        }
        for (row <- broadcast.inserts) {
          val node = current.nodes(edge.receiver)
          val resp = node.receive(row, true, edge.isLeftSide)
          current = current.update(resp.replacement)
          broadcasts += Broadcast(node.id, resp.inserts, resp.deletes)
        }
      }
    }
    current
  }
}
