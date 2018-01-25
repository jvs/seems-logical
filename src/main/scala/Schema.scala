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

  /** Start creating a SQL-like statement. Call it's "from" method to get a
    * Statement object. A statement object can be used as a term within the
    * defintion of a view. */
  case class select(columns: Column*) {
    def from(dataset: Dataset*) = Statement(columns.toVector, dataset.toVector)
  }

  /** Define our aggragate functions. These are used in the SQL-like sublanguage.
    * For example:
    *   select("foo", sum("bar") as "bar_total") from Baz groupBy "fiz"
    */
  private def aggFunc(func: String) = (col: String) => AggregateColumn(func, col)
  val avg = aggFunc("avg")
  val count = aggFunc("count")
  val max = aggFunc("max")
  val median = aggFunc("median")
  val min = aggFunc("min")
  val mode = aggFunc("mode")
  val sum = aggFunc("sum")

  /** Creates a new table and registers it in our lists of tables. */
  def Table(fields: String*) = {
    val table = new Table(fields.toVector)
    tables += table
    table
  }

  /** Starts creating a new view. Note that a view needs a term. So when you
    * call this function, you'll receive a ViewBuilder object. Use that
    * object's "requires" method to get a finished view object. */
  def View(fields: String*) = {
    ViewBuilder(fields.toVector)
  }

  /** Creates a new view from the provided SQL-like statement. */
  def View(statement: Statement) = {
    ViewBuilder(statement.columns.map(x => x.name))(statement)
  }

  /** Utility class for creating a new view. It needs a "Term" object to create
    * a view. You can provide the term object by calling this object's
    * "requires" method or by applying this object like a function to a term. */
  case class ViewBuilder(fields: Vector[String]) {
    /** Same as the "requires" method. Creates a new view and registers it. */
    def apply(term: => Term) = {
      val view = new View(fields, term)
      views += view
      view
    }

    /** Same as the "apply" method. Creates a new view and registers it. */
    def requires(term: => Term) = apply(term)
  }

  /** Creates a new and empty database object from this schema. */
  def create(): Database = new Compiler()
    .add(tables.toList)
    .add(views.toList)
    .compile()
}
