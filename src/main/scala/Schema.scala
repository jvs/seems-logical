package seems.logical

import scala.collection.mutable.ArrayBuffer


/** The main interface to the DSL. Extend this trait and define your tables
  * and views. Call the "create" method to get an empty Database object for
  * your schema. Call the database object's "insert" and "remove" methods
  * to add and remove rows from your tables. Note that these methods return
  * a new database object. A database object is immutable. */
trait Schema {
  // Record every table and view that the object creates.
  private val tables = ArrayBuffer[Table]()
  private val views = ArrayBuffer[View]()

  def tableSet = tables.toSet
  def viewSet = views.toSet

  /** Start creating a SQL-like statement. Call its "from" method to get a
    * Statement object. A statement object can be used as a term within the
    * defintion of a view. */
  case class SELECT(first: Column, columns: Column*) {
    def FROM(term: Term) = Statement(first +: columns.toVector, term)
  }

  /** Define our aggragate functions. These are used in the SQL-like sublanguage.
    * For example:
    *   SELECT("foo", SUM("bar") AS "bar_total") FROM Baz GROUP_BY "fiz"
    */
  private def aggFunc(func: String) = (col: String) => AggregateColumn(func, col)
  val AVG = aggFunc("AVG")
  val COUNT = aggFunc("COUNT")
  val MAX = aggFunc("MAX")
  val MEDIAN = aggFunc("MEDIAN")
  val MIN = aggFunc("MIN")
  val MODE = aggFunc("MODE")
  val SET_OF = aggFunc("SET_OF") // Maybe provide "listOf" or "bagOf" as well?
  val SUM = aggFunc("SUM")


  def Table(fields: String*) = {
    val table = new Table(fields.toVector)
    tables += table
    table
  }

  def View(term: => Term): View = {
    val view = new View(term)
    views += view
    view
  }

  def View(fields: String*) = Projector(fields.toVector)

  case class Projector(fields: Vector[String]) {
    def apply(term: => Term): View = {
      val result = new View(Project(term, fields))
      views += result
      result
    }

    def requires(term: => Term): View = apply(term)
  }

  def create(): Database = new Compiler()
    .accept(tables.toList)
    .accept(views.toList)
    .run()
}
