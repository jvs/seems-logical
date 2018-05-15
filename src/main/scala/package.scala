package seems

import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions


package object logical {
  type Row = Vector[Any]

  class Record(val row: Row, val schema: Vector[String]) {
    def apply[A](index: Int): A = row(index).asInstanceOf[A]
    def apply[A](field: String): A = apply[A](schema.indexOf(field))

    def apply(items: (String, Any)*): Record = {
      val argmap = items.toMap
      val values = schema.zipWithIndex.map { case (name, index) =>
        argmap.getOrElse(name, row(index))
      }
      new Record(values, schema)
    }

    def ++(other: Record): Record = {
      new Record(row ++ other.row, schema ++ other.schema)
    }
  }

  object Record {
    def apply(items: (String, Any)*) = {
      new Record(items.map(_._2).toVector, items.map(_._1).toVector)
    }
  }

  case class SELECT(first: Column, columns: Column*) {
    def FROM(term: Term) = Statement(first +: columns.toVector, term)
  }

  object INSERT {
    case class INTO(table: Table) {
      def VALUES(values: Any*) = InsertStatement(table, values.toVector)
    }
  }

  object DELETE {
    case class FROM(table: Table) {
      def VALUES(values: Any*) = DeleteStatement(table, values.toVector)
      // def WHERE(predicate: Record => Boolean)
    }
  }

  case class InsertStatement(table: Table, values: Vector[Any])
  case class DeleteStatement(table: Table, values: Vector[Any])


  private def aggFunc(func: String) = (col: String) => AggregateColumn(func, col)
  val AVG = aggFunc("AVG")
  val COUNT = aggFunc("COUNT")
  val MAX = aggFunc("MAX")
  val MEDIAN = aggFunc("MEDIAN")
  val MIN = aggFunc("MIN")
  val MODE = aggFunc("MODE")
  // Maybe provide "LIST_OF" or "BAG_OF" as well?
  val SET_OF = aggFunc("SET_OF")
  val SUM = aggFunc("SUM")

  sealed trait Term {
    def and(other: Term): Term = And(this, other)
    def apply(args: String*) = Rename(this, args.toVector)
    def butNot(other: Term): Term = ButNot(this, other)
    def changing(function: Record => Record) = Changing(this, function)
    def changingTo(schema: String*) = ChangeBuilder(this, schema.toVector)
    def expandingTo(schema: String*) = ExandBuilder(this, schema.toVector)
    def or(other: Term): Term = Or(this, other)
    def where(predicate: Record => Boolean): Term = Where(this, predicate)

    val schema: Vector[String]
  }

  sealed trait Dataset extends Term

  class Table(val schema: Vector[String]) extends Dataset

  object Table {
    def apply(schema: String*) = new Table(schema.toVector)
  }

  class View(term: => Term) extends Dataset {
    def body: Term = term
    lazy val schema = term.schema
  }

  object View {
    def apply(schema: String*) = Projector(schema.toVector)
    def apply(term: => Term) = new View(term)
  }

  case class Projector(schema: Vector[String]) {
    def apply(term: => Term): View = new View(Project(term, schema))
    def requires(term: => Term): View = apply(term)
  }

  case class And(left: Term, right: Term) extends Term {
    lazy val schema = {
      left.schema ++ right.schema.filter { x => !left.schema.contains(x) }
    }
  }

  case class ButNot(left: Term, right: Term) extends Term {
    lazy val schema = left.schema
  }

  case class Changing(term: Term, transform: Record => Record) extends Term {
    lazy val schema = term.schema
  }

  case class Expanding(
    term: Term,
    schema: Vector[String],
    expand: Record => List[Record]
  ) extends Term

  case class Or(left: Term, right: Term) extends Term {
    lazy val schema = left.schema.filter { x => right.schema.contains(x) }
  }

  case class Project(term: Term, schema: Vector[String]) extends Term
  case class Rename(term: Term, schema: Vector[String]) extends Term

  case class Where(term: Term, predicate: Record => Boolean) extends Term {
    lazy val schema = term.schema
  }

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

    lazy val schema = select match {
      case Vector(NamedColumn("*")) => from.schema
      case cols => cols.map(_.name)
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

  case class ChangeBuilder(term: Term, schema: Vector[String]) {
    def using(function: Record => Record) = {
      def wrapper(record: Record) = List(function(record))
      Expanding(term, schema, wrapper)
    }
  }

  case class ExandBuilder(term: Term, schema: Vector[String]) {
    def using(function: Record => List[Record]) = Expanding(term, schema, function)
  }
}
