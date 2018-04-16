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

  sealed trait Term { self =>
    def and(other: Term): Term = And(this, other)
    def apply(args: String*) = Rename(this, args.toVector)
    def butNot(other: Term): Term = ButNot(this, other)
    def changing(function: Record => Record) = Changing(this, function)
    def expandingTo(schema: String*) = ExandBuilder(self, schema.toVector)
    def or(other: Term): Term = Or(this, other)
    def where(predicate: Record => Boolean): Term = Where(this, predicate)
  }

  sealed trait Dataset extends Term
  class Table(val fields: Vector[String]) extends Dataset
  class View(term: => Term) extends Dataset {
    def body: Term = term
  }

  case class And(left: Term, right: Term) extends Term
  case class ButNot(left: Term, right: Term) extends Term
  case class Changing(term: Term, transform: Record => Record) extends Term

  case class Expanding(
    term: Term,
    schema: Vector[String],
    expand: Record => List[Record]
  ) extends Term

  case class Or(left: Term, right: Term) extends Term
  case class Project(term: Term, fields: Vector[String]) extends Term
  case class Rename(term: Term, fields: Vector[String]) extends Term
  case class Where(term: Term, predicate: Record => Boolean) extends Term

  case class Statement(
    select: Vector[Column],
    from: Term,
    predicate: Option[Record => Boolean] = None,
    groups: Vector[Column] = Vector()
  ) extends Term {
    def GROUP_BY(columns: Column*) = copy(groups = groups ++ columns)

    def WHERE(p: Record => Boolean): Statement = {
      copy(predicate=Some(predicate match {
        case Some(q) => (x => q(x) && p(x))
        case None => p
      }))
    }
  }

  sealed trait Column {
    def name: String
    def AS(alias: String) = AliasColumn(this, alias)
  }

  case class NamedColumn(name: String) extends Column

  implicit def namedColumn(name: String) = NamedColumn(name)

  case class AggregateColumn(functionName: String, columnName: String) extends Column {
    def name = s"$functionName($columnName)"
  }

  case class AliasColumn(source: Column, alias: String) extends Column {
    def name = alias
    override def AS(alias: String) = AliasColumn(source, alias)
  }

  case class SchemaError(message: String) extends Exception(message)

  case class ExandBuilder(term: Term, schema: Vector[String]) {
    def using(function: Record => List[Record]) = Expanding(term, schema, function)
  }
}
