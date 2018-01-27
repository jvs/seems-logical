package seems

import scala.language.implicitConversions


package object logical {
  /** Define a Row as a Vector of "Any" objects. */
  type Row = Vector[Any]

  class Record(val row: Row, val schema: Vector[String]) {
    def apply[A](index: Int): A = row(index).asInstanceOf[A]
    def apply[A](field: String): A = apply[A](schema.indexOf(field))
  }

  object Record {
    def apply(items: (String, Any)*) = {
      new Record(items.map(_._2).toVector, items.map(_._1).toVector)
    }
  }

  class Dataset(val fields: Vector[String]) {
    def apply(args: String*) = Rename(this, args.toVector)
  }

  class Table(fields: Vector[String]) extends Dataset(fields)
  class View(fields: Vector[String], term: => Term) extends Dataset(fields) {
    def body: Term = term
  }

  sealed trait Term {
    def and(other: Term): Term = And(this, other)
    def butNot(other: Term): Term = ButNot(this, other)
    def changing(function: Record => Record) = Changing(this, function)
    def or(other: Term): Term = Or(this, other)
    def where(predicate: Record => Boolean) = Where(this, predicate)
    // def expanding(function: Record => List[Record]) = Expand(this, function)
  }

  case class And(left: Term, right: Term) extends Term
  case class ButNot(left: Term, right: Term) extends Term
  case class Changing(term: Term, transform: Record => Record) extends Term
  case class Or(left: Term, right: Term) extends Term
  case class Rename(dataset: Dataset, fields: Vector[String]) extends Term
  case class Where(term: Term, predicate: Record => Boolean) extends Term

  case class Statement(
    columns: Vector[Column],
    datasets: Vector[Dataset],
    groups: Vector[Column] = Vector()
  ) extends Term {
    def groupBy(columns: Column*) = this.copy(groups = this.groups ++ columns)
  }

  sealed trait Column {
    def name: String
    def as(alias: String) = AliasColumn(this, alias)
  }

  case class NamedColumn(name: String) extends Column

  implicit def namedColumn(name: String) = NamedColumn(name)

  case class AggregateColumn(functionName: String, columnName: String) extends Column {
    def name = s"$functionName($columnName)"
  }

  case class AliasColumn(source: Column, alias: String) extends Column {
    def name = alias
    override def as(alias: String) = AliasColumn(source, alias)
  }

  case class SchemaError(message: String) extends Exception(message)
}
