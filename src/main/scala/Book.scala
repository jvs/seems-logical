package seems.logical

import scala.collection.mutable.{ArrayBuffer, Queue}


class Book(
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

  def apply(stmt: InsertStatement): Book = {
    update(stmt.table, stmt.values, isInsert = true)
  }

  def apply(stmt: DeleteStatement): Book = {
    update(stmt.table, stmt.values, isInsert = false)
  }

  def THEN(stmt: InsertStatement) = apply(stmt)
  def THEN(stmt: DeleteStatement) = apply(stmt)

  private def update(table: Table, values: Vector[Any], isInsert: Boolean): Book = {
    val expected = table.schema.length
    if (values.length % expected != 0) {
      throw new RuntimeException(
        s"Expected number of values to be divisible by ${expected}."
      )
    }
    values.grouped(expected).foldLeft(this) {
      case (book, row) => transact(book, table, row, isInsert)
    }
  }

  private [logical] def update(node: Option[Node]): Book = node match {
    case Some(n) => new Book(datasets, nodes.updated(n.id, n), edges)
    case None => this
  }
}


object Book {
  def apply(datasets: Dataset*): Book = {
    new Compiler().accept(datasets).run()
  }
}


private abstract class Node(val id: Int) {
  def receive(cast: Broadcast, isLeftSide: Boolean): Response
}

private case class Edge(receiver: Int, isLeftSide: Boolean)


private sealed trait Command

private case class Broadcast(
  sender: Int,
  inserts: ArrayBuffer[Row],
  deletes: ArrayBuffer[Row]
) extends Command


private case class Speculate(val nodeId: Int, val rows: Queue[Row]) extends Command
private case class Verify(rollback: Book, nodeId: Int, row: Row) extends Command


private case class Response(
  replacement: Option[Node],
  inserts: ArrayBuffer[Row],
  deletes: ArrayBuffer[Row],
  speculativeDeletes: Queue[Row] = Queue()
)


object transact {
  def apply(book: Book, table: Table, row: Row, isInsert: Boolean): Book = {
    // This logic is much more natural as a few mututally recursive functions.
    // But then many interesting logic programs could easliy overflow the stack.
    // So this function uses a big ugly loop to avoid using the call stack.
    var current = book
    var stack = List[Command]()

    def push(cmd: Command): Unit = {
      stack = cmd +: stack
    }

    val node = book.nodes(book.datasets(table))
    val cast = Broadcast(
      sender = -1,
      inserts = if (isInsert) ArrayBuffer(row) else ArrayBuffer(),
      deletes = if (isInsert) ArrayBuffer() else ArrayBuffer(row)
    )

    val resp = node.receive(cast, true)
    current = current.update(resp.replacement)
    if (resp.speculativeDeletes.nonEmpty) {
      push(Speculate(node.id, resp.speculativeDeletes))
    }
    if (resp.inserts.nonEmpty || resp.deletes.nonEmpty) {
      push(Broadcast(node.id, resp.inserts, resp.deletes))
    }

    while (stack.nonEmpty) {
      val cmd = stack.head
      stack = stack.tail
      cmd match {
        case b: Broadcast => {
          val queue = Queue[Broadcast](b)
          while (queue.nonEmpty) {
            val next = queue.dequeue()
            for (edge <- current.edges(next.sender)) {
              val node = current.nodes(edge.receiver)
              val resp = node.receive(next, edge.isLeftSide)
              current = current.update(resp.replacement)
              if (resp.speculativeDeletes.nonEmpty) {
                push(Speculate(node.id, resp.speculativeDeletes))
              }
              if (resp.inserts.nonEmpty || resp.deletes.nonEmpty) {
                queue.enqueue(Broadcast(node.id, resp.inserts, resp.deletes))
              }
            }
          }
        }
        case s: Speculate => {
          val row = s.rows.dequeue()
          if (s.rows.nonEmpty) {
            push(s)
          }
          if (contains(current, s.nodeId, row)) {
            push(Verify(current, s.nodeId, row))
            push(Broadcast(s.nodeId, ArrayBuffer(), ArrayBuffer(row)))
          }
        }
        case Verify(rollback, nodeId, row) => {
          if (contains(current, nodeId, row)) {
            current = rollback
          }
        }
      }
    }

    current
  }

  private def contains(current: Book, nodeId: Int, row: Row): Boolean = {
    current.nodes(nodeId) match {
      case a: Add => a.contains(row)
      case a: Expand => a.contains(row)
      case a: Transform => a.contains(row)
      case _ => true
    }
  }
}
