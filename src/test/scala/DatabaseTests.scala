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
  assert(snap2(Foo.Likes) == Set(R(1, 2), R(5, 6)))
  assert(snap1(Foo.Friends) == Set(
    R(1, 2), R(2, 1),
    R(3, 4), R(4, 3),
    R(5, 6), R(6, 5)
  ))
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
  assert(snap2(Foo.Likes) == Set(R(1, 2), R(5, 6)))
  assert(snap1(Foo.Friends) == Set(
    R(1, 2), R(2, 1),
    R(3, 4), R(4, 3),
    R(5, 6), R(6, 5)
  ))
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

"Try a simple view that drops a column and introduces duplicates." - {
  object Z extends Schema {
    val Foo = Table("x", "y", "z")
    val Bar = View("x", "y") requires { Foo("x", "y", "z") }
  }
  val snap1 = Z.create().insert(Z.Foo, 1, 2, 3, 1, 2, 4, 2, 3, 4)
  val snap2 = snap1.remove(Z.Foo, 1, 2, 4)
  val snap3 = snap2.remove(Z.Foo, 1, 2, 3)
  val snap4 = snap3.insert(Z.Foo, 2, 3, 5).remove(Z.Foo, 2, 3, 4, 2, 3, 5)
  assert(snap1(Z.Bar) == Set(R(1, 2), R(2, 3)))
  assert(snap2(Z.Bar) == Set(R(1, 2), R(2, 3)))
  assert(snap3(Z.Bar) == Set(R(2, 3)))
  assert(snap4(Z.Bar) == Set())
}

"Try a simple view that redundantly adds two tables." - {
  object Z extends Schema {
    val Foo = Table("x", "y")
    val Bar = View("x", "y") requires { Foo("x", "y") or Foo("x", "y") }
  }
  val snap1 = Z.create().insert(Z.Foo, 1, 2, 3, 4, 5, 6)
  val snap2 = snap1.remove(Z.Foo, 3, 4)
  val snap3 = snap2.remove(Z.Foo, 1, 2, 5, 6)
  assert(snap1(Z.Bar) == Set(R(1, 2), R(3, 4), R(5, 6)))
  assert(snap2(Z.Bar) == Set(R(1, 2), R(5, 6)))
  assert(snap3(Z.Bar) == Set())
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

"Try using a transform that recursively generates many rows." - {
  object Foo extends Schema {
    val Bar = Table("x")
    val Baz: View = View("x") requires {
      Bar("x") or (Baz("x") changing { rec =>
        val x = rec[Int]("x")
        if (x <= 0) rec else Record("x" -> (x - 1))
      })
    }
  }
  val start = Foo.create()
  val snap1 = start.insert(Foo.Bar, 10)
  val snap2 = snap1.insert(Foo.Bar, 9)
  val snap3 = snap2.remove(Foo.Bar, 10)
  val snap4 = snap2.remove(Foo.Bar, 9)
  val snap5 = snap2.remove(Foo.Bar, 10, 9)
  val snap6 = snap2.remove(Foo.Bar, 9, 10)
  assert(snap1(Foo.Baz) == Set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10).map(i => R(i)))
  assert(snap2(Foo.Baz) == Set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10).map(i => R(i)))
  assert(snap3(Foo.Baz) == Set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9).map(i => R(i)))
  assert(snap4(Foo.Baz) == Set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10).map(i => R(i)))
  assert(snap5(Foo.Baz) == Set())
  assert(snap6(Foo.Baz) == Set())
}

"Try using a simple expansion." - {
  object Foo extends Schema {
    val Bar = Table("x", "y", "z", "w")
    val Baz = View("x", "other") requires {
      Bar("x", "y", "z", "w") expandingTo ("x", "other") using {
        rec => List(
          Record("x" -> rec[Int]("x"), "other" -> rec[Int]("y")),
          Record("x" -> rec[Int]("x"), "other" -> rec[Int]("z")),
          // Try flipping the order one time, to make sure that the record
          // is adapted correctly.
          Record("other" -> rec[Int]("w"), "x" -> rec[Int]("x"))
        )
      }
    }
  }
  val start = Foo.create()
  val snap1 = start.insert(Foo.Bar, 1, 2, 3, 4, 5, 6, 7, 8)
  val snap2 = snap1.remove(Foo.Bar, 5, 6, 7, 8)
  assert(snap1(Foo.Baz) == Set(
    R(1, 2), R(1, 3), R(1, 4),
    R(5, 6), R(5, 7), R(5, 8)
  ))
  assert(snap2(Foo.Baz) == Set(R(1, 2), R(1, 3), R(1, 4)))
}

"Try using simple subtraction." - {
  object Z extends Schema {
    val Flights = Table("airline", "from", "to")
    val Reaches: View = View("airline", "from", "to") {
      Flights("airline", "from", "to") or (
        Reaches("airline", "from", "layover") and
        Reaches("airline", "layover", "to")
      )
    }
    val Other = View("airline", "from", "to") {
      Reaches("airline", "from", "to") where {
        rec => rec[String]("airline") != "UA"
      }
    }
    val OnlyUA = View("airline", "from", "to") {
      Reaches("airline", "from", "to") butNot
      Other("some-other-airline", "from", "to")
    }
  }
  val snap1 = Z.create().insert(
    Z.Flights,
    "UA", "SF", "DEN",
    "AA", "SF", "DAL",
    "UA", "DEN", "CHI",
    "UA", "DEN", "DAL",
    "AA", "DAL", "CHI",
    "AA", "DAL", "NY",
    "AA", "CHI", "NY",
    "UA", "CHI", "NY"
  )
  val snap2 = snap1.insert(Z.Flights, "UA", "NY", "BOS")
  val snap3 = snap2.insert(Z.Flights, "AA", "NY", "BOS")
  val snap4 = snap3.remove(Z.Flights, "AA", "CHI", "NY")

  assert(snap1(Z.Reaches) == Set(
    R("AA", "CHI", "NY"),
    R("AA", "DAL", "CHI"),
    R("AA", "DAL", "NY"),
    R("AA", "SF", "CHI"),
    R("AA", "SF", "DAL"),
    R("AA", "SF", "NY"),
    R("UA", "CHI", "NY"),
    R("UA", "DEN", "CHI"),
    R("UA", "DEN", "DAL"),
    R("UA", "DEN", "NY"),
    R("UA", "SF", "CHI"),
    R("UA", "SF", "DAL"),
    R("UA", "SF", "DEN"),
    R("UA", "SF", "NY")
  ))

  assert(snap1(Z.OnlyUA) == Set(
    R("UA", "DEN", "CHI"),
    R("UA", "DEN", "DAL"),
    R("UA", "DEN", "NY"),
    R("UA", "SF", "DEN")
  ))

  assert(snap2(Z.Reaches) == snap1(Z.Reaches) ++ Set(
    R("UA", "NY", "BOS"),
    R("UA", "SF", "BOS"),
    R("UA", "CHI", "BOS"),
    R("UA", "DEN", "BOS")
  ))

  assert(snap2(Z.OnlyUA) == snap1(Z.OnlyUA) ++ Set(
    R("UA", "NY", "BOS"),
    R("UA", "SF", "BOS"),
    R("UA", "CHI", "BOS"),
    R("UA", "DEN", "BOS")
  ))

  assert(snap3(Z.Reaches) == snap2(Z.Reaches) ++ Set(
    R("AA", "NY", "BOS"),
    R("AA", "CHI", "BOS"),
    R("AA", "DAL", "BOS"),
    R("AA", "SF", "BOS")
  ))

  assert(snap3(Z.OnlyUA) == snap1(Z.OnlyUA) ++ Set(
    R("UA", "DEN", "BOS")
  ))
}
}}
