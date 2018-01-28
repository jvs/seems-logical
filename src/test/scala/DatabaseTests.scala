package test.seems.logical

import utest._
import seems.logical.{Record, Row, Schema, View}

object R {
  def apply(items: Any*): Row = items.toVector
}

object DslTests extends TestSuite { val tests = Tests {

"Make sure DSL seems to create the expected things." - {
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

"Try a simple view that just contains one 'or' operation." - {
  object Foo extends Schema {
    val Likes = Table("x", "y")
    val Friends = View("x", "y") { Likes("x", "y") or Likes("y", "x") }
  }
  val start = Foo.create()
  val snap1 = start.insert(Foo.Likes, 1, 2, 3, 4, 5, 6)
  val snap2 = snap1.remove(Foo.Likes, 3, 4)
  assert(snap1(Foo.Likes) == Set(R(1, 2), R(3, 4), R(5, 6)))
  assert(snap1(Foo.Friends) == Set(
    R(1, 2), R(2, 1),
    R(3, 4), R(4, 3),
    R(5, 6), R(6, 5)
  ))
  assert(snap2(Foo.Likes) == Set(R(1, 2), R(5, 6)))
  assert(snap2(Foo.Friends) == Set(
    R(1, 2), R(2, 1),
    R(5, 6), R(6, 5)
  ))
}

"Try the same simple view as before, only this time make it recursive." - {
  object Foo extends Schema {
    val Likes = Table("x", "y")
    val Friends: View = View("x", "y") { Likes("x", "y") or Friends("y", "x") }
  }
  // Everything is the same as before:
  val start = Foo.create()
  val snap1 = start.insert(Foo.Likes, 1, 2, 3, 4, 5, 6)
  val snap2 = snap1.remove(Foo.Likes, 3, 4)
  assert(snap1(Foo.Likes) == Set(R(1, 2), R(3, 4), R(5, 6)))
  assert(snap1(Foo.Friends) == Set(
    R(1, 2), R(2, 1),
    R(3, 4), R(4, 3),
    R(5, 6), R(6, 5)
  ))
  assert(snap2(Foo.Likes) == Set(R(1, 2), R(5, 6)))
  assert(snap2(Foo.Friends) == Set(
    R(1, 2), R(2, 1),
    R(5, 6), R(6, 5)
  ))
}

"Try joining two simple tables." - {
  object Foo extends Schema {
    val Bar = Table("x", "y")
    val Baz = Table("z", "w")
    val Fiz = View("x", "yz", "w") requires {
      Bar("x", "yz") and Baz("yz", "w")
    }
  }
  val start = Foo.create()
  val snap1 = start.insert(Foo.Bar, 1, 2, 3, 4, 5, 6)
  val snap2 = snap1.insert(Foo.Baz, 2, 3, 6, 7)
  val snap3 = snap2.remove(Foo.Bar, 5, 6).insert(Foo.Baz, 4, 5)

  assert(snap1(Foo.Bar) == Set(R(1, 2), R(3, 4), R(5, 6)))
  assert(snap1(Foo.Baz) == Set())
  assert(snap1(Foo.Fiz) == Set())

  assert(snap2(Foo.Bar) == Set(R(1, 2), R(3, 4), R(5, 6)))
  assert(snap2(Foo.Baz) == Set(R(2, 3), R(6, 7)))
  assert(snap2(Foo.Fiz) == Set(R(1, 2, 3), R(5, 6, 7)))

  assert(snap3(Foo.Bar) == Set(R(1, 2), R(3, 4)))
  assert(snap3(Foo.Baz) == Set(R(2, 3), R(4, 5), R(6, 7)))
  assert(snap3(Foo.Fiz) == Set(R(1, 2, 3), R(3, 4, 5)))
}

"Try a simple ancestor relation." - {
  object Family extends Schema {
    val Father = Table("father", "child")
    val Mother = Table("mother", "child")

    val Parent = View("parent", "child") requires {
      Father("parent", "child") or Mother("parent", "child")
    }

    val Ancestor: View = View("ancestor", "descendant") requires {
      Parent("ancestor", "descendant") or (
        Ancestor("ancestor", "relative") and
        Ancestor("relative", "descendant")
      )
    }

    val SameFather = View("a", "b") requires {
      Father("f", "a") and Father("f", "b")
    }

    val SameMother = View("a", "b") requires {
      Mother("m", "a") and Mother("m", "b")
    }

    val Siblings = View("a", "b") requires {
      SameFather("a", "b") and
      SameMother("a", "b") where {
        record => record[String]("a") != record[String]("b")
      }
    }
  }

  val snap1 = Family.create()
    .insert(Family.Father,
      "Phillip", "Charles",
      "Charles", "William",
      "Charles", "Harry",
      "William", "George"
    )
    .insert(Family.Mother,
      "Elizabeth II", "Charles",
      "Diana", "William",
      "Diana", "Harry",
      "Catherine", "George"
    )

  val snap2 = snap1
    .insert(Family.Father, "William", "Charlotte")
    .insert(Family.Mother, "Catherine", "Charlotte")

  val snap3 = snap2
    .remove(Family.Father, "William", "Charlotte")
    .remove(Family.Mother, "Catherine", "Charlotte")

  val snap4 = snap3
    .insert(Family.Father, "William", "Charlotte")
    .insert(Family.Mother, "Catherine", "Charlotte")

  assert(snap1(Family.Parent) == Set(
    R("Phillip", "Charles"),
    R("Charles", "William"),
    R("Charles", "Harry"),
    R("William", "George"),

    R("Elizabeth II", "Charles"),
    R("Diana", "William"),
    R("Diana", "Harry"),
    R("Catherine", "George")
  ))

  assert(snap1(Family.Ancestor) == Set(
    R("Phillip", "Charles"),
    R("Phillip", "William"),
    R("Phillip", "Harry"),
    R("Phillip", "George"),

    R("Charles", "William"),
    R("Charles", "Harry"),
    R("Charles", "George"),

    R("William", "George"),

    R("Elizabeth II", "Charles"),
    R("Elizabeth II", "William"),
    R("Elizabeth II", "Harry"),
    R("Elizabeth II", "George"),

    R("Diana", "William"),
    R("Diana", "Harry"),
    R("Diana", "George"),

    R("Catherine", "George")
  ))

  assert(snap1(Family.Siblings) == Set(
    R("Harry", "William"),
    R("William", "Harry")
  ))

  // snap2
  assert(snap2(Family.Parent) == snap1(Family.Parent) ++ Set(
    R("William", "Charlotte"),
    R("Catherine", "Charlotte")
  ))

  assert(snap2(Family.Ancestor) == snap1(Family.Ancestor) ++ Set(
    R("Phillip", "Charlotte"),
    R("Charles", "Charlotte"),
    R("William", "Charlotte"),

    R("Elizabeth II", "Charlotte"),
    R("Diana", "Charlotte"),
    R("Catherine", "Charlotte")
  ))

  assert(snap2(Family.Siblings) == snap1(Family.Siblings) ++ Set(
    R("George", "Charlotte"),
    R("Charlotte", "George")
  ))

  // snap3
  assert(snap3(Family.Parent) == snap1(Family.Parent))
  assert(snap3(Family.Ancestor) == snap1(Family.Ancestor))
  assert(snap3(Family.Siblings) == snap1(Family.Siblings))

  // snap4
  assert(snap4(Family.Parent) == snap2(Family.Parent))
  assert(snap4(Family.Ancestor) == snap2(Family.Ancestor))
  assert(snap4(Family.Siblings) == snap2(Family.Siblings))
}

"Try using a simple transform." - {
  object Foo extends Schema {
    val Bar = Table("x", "y")
    val Baz = View("x", "y") requires {
      Bar("x", "y") changing {
        record => Record(
          "x" -> (record[Int](1) + 1),
          "y" -> (record[Int](0) + 1)
        )
      }
    }
  }
  val start = Foo.create()
  val snap1 = start.insert(Foo.Bar, 1, 2, 3, 4, 5, 6)
  val snap2 = snap1.remove(Foo.Bar, 3, 4)
  assert(snap1(Foo.Baz) == Set(R(7, 6), R(5, 4), R(3, 2)))
  assert(snap2(Foo.Baz) == Set(R(7, 6), R(3, 2)))
}

"Try using a simple transform that changes the order of the fields." - {
  object Foo extends Schema {
    val Bar = Table("x", "y")
    val Baz = View("x", "y") requires {
      Bar("x", "y") changing {
        record => Record(
          "y" -> (record[Int](0) + 1),
          "x" -> (record[Int](1) + 1)
        )
      }
    }
  }
  // The rest is the same as before.
  val start = Foo.create()
  val snap1 = start.insert(Foo.Bar, 1, 2, 3, 4, 5, 6)
  val snap2 = snap1.remove(Foo.Bar, 3, 4)
  assert(snap1(Foo.Baz) == Set(R(7, 6), R(5, 4), R(3, 2)))
  assert(snap2(Foo.Baz) == Set(R(7, 6), R(3, 2)))
}
}}
