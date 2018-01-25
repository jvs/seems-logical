package test.seems.logical

import utest._
import seems.logical.Schema

object DslTests extends TestSuite { val tests = Tests {

"test1" - {
  object SymbolTable extends Schema {
    val Scope = Table("id")
    val Package = Table("path")
    val PackageScope = Table("package", "scope")
    val Binding = Table("name", "treeId", "scope")

    val NumBindings = View(
      select("scope", count("name") as "numBindings")
      from Binding
      groupBy "scope"
    )

    val Fullpath = View("treeId", "package", "name") {
      Binding("name", "treeId", "scope") and
      PackageScope("package", "scope")
    }
  }

  assert(SymbolTable.Scope.fields == Vector("id"))
  assert(SymbolTable.Binding.fields == Vector("name", "treeId", "scope"))
  assert(SymbolTable.NumBindings.fields == Vector("scope", "numBindings"))
}

}}
