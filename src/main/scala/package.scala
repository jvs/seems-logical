package seems

import scala.language.implicitConversions


package object logical {
  class Dataset(val fields: Vector[String]) {
    def apply(args: String*) = Rename(this, args.toVector)
  }

  class Table(fields: Vector[String]) extends Dataset(fields)
  class View(fields: Vector[String], term: => Term) extends Dataset(fields)

  sealed trait Term {
    def and(other: Term): Term = And(this, other)
    def or(other: Term): Term = Or(this, other)
    def andNot(other: Term): Term = Subtract(this, other)
  }

  case class And(left: Term, right: Term) extends Term
  case class Or(left: Term, right: Term) extends Term
  case class Subtract(left: Term, right: Term) extends Term
  case class Rename(dataset: Dataset, fields: Vector[String]) extends Term

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

  /** Define a Row as a Vector of "Any" objects. */
  type Row = Vector[Any]
}