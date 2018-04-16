package seems.logical

import scala.collection.mutable.ArrayBuffer


trait LogicProgram {
  private val tables = ArrayBuffer[Table]()
  private val views = ArrayBuffer[View]()

  def tableSet = tables.toSet
  def viewSet = views.toSet

  case class SELECT(first: Column, columns: Column*) {
    def FROM(term: Term) = Statement(first +: columns.toVector, term)
  }

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
