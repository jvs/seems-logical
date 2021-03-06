package test.seems.logical

import utest._
import seems.logical._

object R {
  def apply(items: Any*): Row = items.toVector
}

object DslTests extends TestSuite {

def rederive(book: Book): Book = {
  var result = Book(book.datasetDefs.toVector:_*)
  for (k <- book.tableDefs) {
    for (row <- book(k)) {
      result = result { INSERT INTO k VALUES (row:_*) }
    }
  }
  result
}

def assertSame(a: Book, b: Book) = {
  assert(a.datasetDefs == b.datasetDefs)
  for (k <- a.datasetDefs) {
    assert(a(k) == b(k))
  }
}

def assertConsistent(book: Book) = {
  assertSame(book, rederive(book))
}


val tests = Tests {
"Try updating the fields of a record." - {
  val a = Record("x" -> 1, "y" -> 2, "z" -> 3, "w" -> 4)
  val b = a("y" -> 20, "w" -> 400)

  assert(a[Int]("x") == 1)
  assert(a[Int]("y") == 2)
  assert(a[Int]("z") == 3)
  assert(a[Int]("w") == 4)

  assert(b[Int]("x") == 1)
  assert(b[Int]("y") == 20)
  assert(b[Int]("z") == 3)
  assert(b[Int]("w") == 400)
}

"Make sure DSL seems to create the expected things." - {
  val Scope = Table("id")
  val Package = Table("path")
  val PackageScope = Table("package", "scope")
  val Binding = Table("name", "treeId", "scope")

  val NumBindings = View(
    SELECT ("scope", COUNT("name") AS "numBindings")
    FROM Binding
    GROUP_BY "scope"
  )

  val Fullpath = View("treeId", "package", "name") {
    Binding("name", "treeId", "scope") and
    PackageScope("package", "scope")
  }

  assert(Scope.schema == Vector("id"))
  assert(Binding.schema == Vector("name", "treeId", "scope"))
  assert(NumBindings.schema == Vector("scope", "numBindings"))
}

"Try a simple view that just contains one 'or' operation." - {
  val Likes = Table("x", "y")
  val Friends = View("x", "y") { Likes("x", "y") or Likes("y", "x") }

  val start = Book(Friends, Likes)
  val snap1 = start { INSERT INTO Likes VALUES (1, 2, 3, 4, 5, 6) }
  val snap2 = snap1 { DELETE FROM Likes VALUES (3, 4) }
  assert(snap1(Likes) == Set(R(1, 2), R(3, 4), R(5, 6)))
  assert(snap2(Likes) == Set(R(1, 2), R(5, 6)))
  assert(snap1(Friends) == Set(
    R(1, 2), R(2, 1),
    R(3, 4), R(4, 3),
    R(5, 6), R(6, 5)
  ))
  assert(snap2(Friends) == Set(
    R(1, 2), R(2, 1),
    R(5, 6), R(6, 5)
  ))
  assertConsistent(snap1)
  assertConsistent(snap2)
}

"Try the same simple view as before, only this time make it recursive." - {
  val Likes = Table("x", "y")
  lazy val Friends: View = View("x", "y") { Likes("x", "y") or Friends("y", "x") }

  // Everything is the same as before:
  val start = Book(Friends, Likes)
  val snap1 = start { INSERT INTO Likes VALUES (1, 2, 3, 4, 5, 6) }
  val snap2 = snap1 { DELETE FROM Likes VALUES (3, 4) }
  assert(snap1(Likes) == Set(R(1, 2), R(3, 4), R(5, 6)))
  assert(snap2(Likes) == Set(R(1, 2), R(5, 6)))
  assert(snap1(Friends) == Set(
    R(1, 2), R(2, 1),
    R(3, 4), R(4, 3),
    R(5, 6), R(6, 5)
  ))
  assert(snap2(Friends) == Set(
    R(1, 2), R(2, 1),
    R(5, 6), R(6, 5)
  ))
  assertConsistent(snap1)
  assertConsistent(snap2)
}

"Try joining two simple tables." - {
  val Bar = Table("x", "y")
  val Baz = Table("z", "w")
  val Fiz = View("x", "yz", "w") requires {
    Bar("x", "yz") and Baz("yz", "w")
  }

  val start = Book(Bar, Baz, Fiz)
  val snap1 = start { INSERT INTO Bar VALUES (1, 2, 3, 4, 5, 6) }
  val snap2 = snap1 { INSERT INTO Baz VALUES (2, 3, 6, 7) }
  val snap3 = snap2
    { DELETE FROM Bar VALUES (5, 6) }
    { INSERT INTO Baz VALUES (4, 5) }

  assert(snap1(Bar) == Set(R(1, 2), R(3, 4), R(5, 6)))
  assert(snap1(Baz) == Set())
  assert(snap1(Fiz) == Set())

  assert(snap2(Bar) == Set(R(1, 2), R(3, 4), R(5, 6)))
  assert(snap2(Baz) == Set(R(2, 3), R(6, 7)))
  assert(snap2(Fiz) == Set(R(1, 2, 3), R(5, 6, 7)))

  assert(snap3(Bar) == Set(R(1, 2), R(3, 4)))
  assert(snap3(Baz) == Set(R(2, 3), R(4, 5), R(6, 7)))
  assert(snap3(Fiz) == Set(R(1, 2, 3), R(3, 4, 5)))

  assertConsistent(snap1)
  assertConsistent(snap2)
  assertConsistent(snap3)
}

"Try a simple view that drops a column and introduces duplicates." - {
  val Foo = Table("x", "y", "z")
  val Bar = View("x", "y") requires { Foo("x", "y", "z") }

  val snap1 = Book(Bar) {
    INSERT INTO Foo VALUES (1, 2, 3, 1, 2, 4, 2, 3, 4)
  }
  val snap2 = snap1 { DELETE FROM Foo VALUES (1, 2, 4) }
  val snap3 = snap2 { DELETE FROM Foo VALUES (1, 2, 3) }
  val snap4 = snap3
    { INSERT INTO Foo VALUES (2, 3, 5) }
    { DELETE FROM Foo VALUES (2, 3, 4, 2, 3, 5) }

  assert(snap1(Bar) == Set(R(1, 2), R(2, 3)))
  assert(snap2(Bar) == Set(R(1, 2), R(2, 3)))
  assert(snap3(Bar) == Set(R(2, 3)))
  assert(snap4(Bar) == Set())
  assertConsistent(snap1)
  assertConsistent(snap2)
  assertConsistent(snap3)
  assertConsistent(snap4)
}

"Try a simple view that redundantly adds two tables." - {
  val Foo = Table("x", "y")
  val Bar = View("x", "y") requires { Foo("x", "y") or Foo("x", "y") }

  val snap1 = Book(Bar) {
    INSERT INTO Foo VALUES (1, 2, 3, 4, 5, 6)
  }
  val snap2 = snap1 { DELETE FROM Foo VALUES (3, 4) }
  val snap3 = snap2 { DELETE FROM Foo VALUES (1, 2, 5, 6) }
  assert(snap1(Bar) == Set(R(1, 2), R(3, 4), R(5, 6)))
  assert(snap2(Bar) == Set(R(1, 2), R(5, 6)))
  assert(snap3(Bar) == Set())
  assertConsistent(snap1)
  assertConsistent(snap2)
  assertConsistent(snap3)
}

"Try a simple ancestor relation." - {
  val Father = Table("father", "child")
  val Mother = Table("mother", "child")

  val Parent = View("parent", "child") requires {
    Father("parent", "child") or Mother("parent", "child")
  }

  lazy val Ancestor: View = View("ancestor", "descendant") requires {
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

  val start = Book(Ancestor, Siblings)

  val snap1 = start {
    INSERT INTO Father VALUES (
      "Phillip", "Charles",
      "Charles", "William",
      "Charles", "Harry",
      "William", "George"
    )
  } THEN {
    INSERT INTO Mother VALUES (
      "Elizabeth II", "Charles",
      "Diana", "William",
      "Diana", "Harry",
      "Catherine", "George"
    )
  }
  val snap2 = snap1
    { INSERT INTO Father VALUES ("William", "Charlotte") }
    { INSERT INTO Mother VALUES ("Catherine", "Charlotte") }

  val snap3 = snap2
    { DELETE FROM Father VALUES ("William", "Charlotte") }
    { DELETE FROM Mother VALUES ("Catherine", "Charlotte") }

  val snap4 = snap3
    { INSERT INTO Father VALUES ("William", "Charlotte") }
    { INSERT INTO Mother VALUES ("Catherine", "Charlotte") }

  assert(snap1(Parent) == Set(
    R("Phillip", "Charles"),
    R("Charles", "William"),
    R("Charles", "Harry"),
    R("William", "George"),

    R("Elizabeth II", "Charles"),
    R("Diana", "William"),
    R("Diana", "Harry"),
    R("Catherine", "George")
  ))

  assert(snap1(Ancestor) == Set(
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

  assert(snap1(Siblings) == Set(
    R("Harry", "William"),
    R("William", "Harry")
  ))

  // snap2
  assert(snap2(Parent) == snap1(Parent) ++ Set(
    R("William", "Charlotte"),
    R("Catherine", "Charlotte")
  ))

  assert(snap2(Ancestor) == snap1(Ancestor) ++ Set(
    R("Phillip", "Charlotte"),
    R("Charles", "Charlotte"),
    R("William", "Charlotte"),

    R("Elizabeth II", "Charlotte"),
    R("Diana", "Charlotte"),
    R("Catherine", "Charlotte")
  ))

  assert(snap2(Siblings) == snap1(Siblings) ++ Set(
    R("George", "Charlotte"),
    R("Charlotte", "George")
  ))

  // snap3
  assert(snap3(Parent) == snap1(Parent))
  assert(snap3(Ancestor) == snap1(Ancestor))
  assert(snap3(Siblings) == snap1(Siblings))

  // snap4
  assert(snap4(Parent) == snap2(Parent))
  assert(snap4(Ancestor) == snap2(Ancestor))
  assert(snap4(Siblings) == snap2(Siblings))

  assertConsistent(snap1)
  assertConsistent(snap2)
  assertConsistent(snap3)
  assertConsistent(snap4)
}

"Try using a simple transform." - {
  val Bar = Table("x", "y")
  val Baz = View("x", "y") requires {
    Bar("x", "y") changing {
      record => Record(
        "x" -> (record[Int](1) + 1),
        "y" -> (record[Int](0) + 1)
      )
    }
  }

  val start = Book(Bar, Baz)
  val snap1 = start { INSERT INTO Bar VALUES (1, 2, 3, 4, 5, 6) }
  val snap2 = snap1 { DELETE FROM Bar VALUES (3, 4) }
  assert(snap1(Baz) == Set(R(7, 6), R(5, 4), R(3, 2)))
  assert(snap2(Baz) == Set(R(7, 6), R(3, 2)))
  assertConsistent(snap1)
  assertConsistent(snap2)
}

"Try using a simple transform that changes the order of the fields." - {
  val Bar = Table("x", "y")
  val Baz = View("x", "y") requires {
    Bar("x", "y") changing {
      record => Record(
        "y" -> (record[Int](0) + 1),
        "x" -> (record[Int](1) + 1)
      )
    }
  }

  // The rest is the same as before.
  val start = Book(Bar, Baz)
  val snap1 = start { INSERT INTO Bar VALUES (1, 2, 3, 4, 5, 6) }
  val snap2 = snap1 { DELETE FROM Bar VALUES (3, 4) }
  assert(snap1(Baz) == Set(R(7, 6), R(5, 4), R(3, 2)))
  assert(snap2(Baz) == Set(R(7, 6), R(3, 2)))
  assertConsistent(snap1)
  assertConsistent(snap2)
}

"Try using a simple transform that updates two of the fields." - {
  val Foo = Table("x", "y", "z", "w")
  val Bar = View("x", "y", "z", "w") requires {
    Foo("x", "y", "z", "w") changing { rec => rec(
      "y" -> rec[Int]("y") * 10,
      "w" -> rec[Int]("w") * 100
    )}
  }

  val start = Book(Bar)
  val snap1 = start { INSERT INTO Foo VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12) }
  val snap2 = snap1 { DELETE FROM Foo VALUES (5, 6, 7, 8) }
  assert(snap1(Bar) == Set(R(1, 20, 3, 400), R(5, 60, 7, 800), R(9, 100, 11, 1200)))
  assert(snap2(Bar) == Set(R(1, 20, 3, 400), R(9, 100, 11, 1200)))
  assertConsistent(snap1)
  assertConsistent(snap2)
}

"Try using a simple transform that adds a field." - {
  val Foo = Table("x", "y")
  val Bar = View {
    Foo("x", "y") changingTo ("x", "y", "z") using {
      rec => rec ++ Record("z" -> 0)
    }
  }

  val start = Book(Bar)
  val snap1 = start { INSERT INTO Foo VALUES (1, 2, 3, 4, 5, 6) }
  val snap2a = snap1 { DELETE FROM Foo VALUES (1, 2) }
  val snap2b = snap1 { DELETE FROM Foo VALUES (5, 6) }
  assert(snap1(Bar) == Set(R(1, 2, 0), R(3, 4, 0), R(5, 6, 0)))
  assert(snap2a(Bar) == Set(R(3, 4, 0), R(5, 6, 0)))
  assert(snap2b(Bar) == Set(R(1, 2, 0), R(3, 4, 0)))
  assertConsistent(snap1)
  assertConsistent(snap2a)
  assertConsistent(snap2b)
}

"Try using a transform that recursively generates many rows." - {
  val Bar = Table("x")

  lazy val Baz: View = View("x") requires {
    Bar("x") or (Baz("x") changing { rec =>
      val x = rec[Int]("x")
      if (x <= 0) rec else Record("x" -> (x - 1))
    })
  }

  val start = Book(Bar, Baz)
  val snap1 = start { INSERT INTO Bar VALUES 10 }
  val snap2 = snap1 { INSERT INTO Bar VALUES 9 }
  val snap3 = snap2 { DELETE FROM Bar VALUES 10 }
  val snap4 = snap2 { DELETE FROM Bar VALUES 9 }
  val snap5 = snap2 { DELETE FROM Bar VALUES (10, 9) }
  val snap6 = snap2 { DELETE FROM Bar VALUES (9, 10) }

  assert(snap1(Baz) == Set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10).map(i => R(i)))
  assert(snap2(Baz) == Set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10).map(i => R(i)))
  assert(snap3(Baz) == Set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9).map(i => R(i)))
  assert(snap4(Baz) == Set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10).map(i => R(i)))
  assert(snap5(Baz) == Set())
  assert(snap6(Baz) == Set())

  assertConsistent(snap1)
  assertConsistent(snap2)
  assertConsistent(snap3)
  assertConsistent(snap4)
  assertConsistent(snap5)
  assertConsistent(snap6)
}

"Try using a simple expansion." - {
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

  val start = Book(Bar, Baz)
  val snap1 = start { INSERT INTO Bar VALUES (1, 2, 3, 4, 5, 6, 7, 8) }
  val snap2 = snap1 { DELETE FROM Bar VALUES (5, 6, 7, 8) }
  assert(snap1(Baz) == Set(
    R(1, 2), R(1, 3), R(1, 4),
    R(5, 6), R(5, 7), R(5, 8)
  ))
  assert(snap2(Baz) == Set(R(1, 2), R(1, 3), R(1, 4)))
  assertConsistent(snap1)
  assertConsistent(snap2)
}

"Try using simple subtraction." - {
  val Flights = Table("airline", "from", "to")

  lazy val Reaches: View = View("airline", "from", "to") {
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

  val snap1 = Book(Flights, Reaches, Other, OnlyUA) {
    INSERT INTO Flights VALUES (
      "UA", "SF", "DEN",
      "AA", "SF", "DAL",
      "UA", "DEN", "CHI",
      "UA", "DEN", "DAL",
      "AA", "DAL", "CHI",
      "AA", "DAL", "NY",
      "AA", "CHI", "NY",
      "UA", "CHI", "NY"
    )
  }
  val snap2 = snap1 { INSERT INTO Flights VALUES ("UA", "NY", "BOS") }
  val snap3 = snap2 { INSERT INTO Flights VALUES ("AA", "NY", "BOS") }
  val snap4 = snap3 { DELETE FROM Flights VALUES ("AA", "CHI", "NY") }
  assertConsistent(snap1)
  assertConsistent(snap2)
  assertConsistent(snap3)
  assertConsistent(snap4)

  assert(snap1(Reaches) == Set(
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

  assert(snap1(OnlyUA) == Set(
    R("UA", "DEN", "CHI"),
    R("UA", "DEN", "DAL"),
    R("UA", "DEN", "NY"),
    R("UA", "SF", "DEN")
  ))

  assert(snap2(Reaches) == snap1(Reaches) ++ Set(
    R("UA", "NY", "BOS"),
    R("UA", "SF", "BOS"),
    R("UA", "CHI", "BOS"),
    R("UA", "DEN", "BOS")
  ))

  assert(snap2(OnlyUA) == snap1(OnlyUA) ++ Set(
    R("UA", "NY", "BOS"),
    R("UA", "SF", "BOS"),
    R("UA", "CHI", "BOS"),
    R("UA", "DEN", "BOS")
  ))

  assert(snap3(Reaches) == snap2(Reaches) ++ Set(
    R("AA", "NY", "BOS"),
    R("AA", "CHI", "BOS"),
    R("AA", "DAL", "BOS"),
    R("AA", "SF", "BOS")
  ))

  assert(snap3(OnlyUA) == snap1(OnlyUA) ++ Set(
    R("UA", "DEN", "BOS")
  ))

  assert(snap4(Reaches) == snap3(Reaches) -- Set(
    R("AA", "CHI", "NY"),
    R("AA", "CHI", "BOS")
  ))

  assert(snap4(OnlyUA) == snap3(OnlyUA) ++ Set(
    R("UA", "CHI", "NY"),
    R("UA", "CHI", "BOS")
  ))
}

"Try making the same row from two different tables reach the same view in the same transaction" - {
  val Maybe1 = Table("id", "name")
  val Maybe2 = Table("id", "name")
  val Banned = Table("id", "name")
  val Approved1 = View("id", "name") { Maybe1("id", "name") butNot Banned("id", "name") }
  val Approved2 = View("id", "name") { Maybe2("id", "name") butNot Banned("id", "name") }
  val Approved = View("id", "name") { Approved1("id", "name") or Approved2("id", "name") }

  val snap1 = Book(Approved)
    { INSERT INTO Banned VALUES (1, "A", 2, "B") }
    { INSERT INTO Maybe1 VALUES (1, "A", 3, "C") }
    { INSERT INTO Maybe2 VALUES (1, "A", 4, "D") }

  val snap2 = snap1 { DELETE FROM Banned VALUES (1, "A") }
  val snap3a = snap2 { DELETE FROM Maybe1 VALUES (1, "A") }
  val snap3b = snap2 { DELETE FROM Maybe2 VALUES (1, "A") }
  assertConsistent(snap1)
  assertConsistent(snap2)
  assertConsistent(snap3a)
  assertConsistent(snap3b)
  assert(snap1(Approved) == Set(R(3, "C"), R(4, "D")))
  assert(snap2(Approved) == Set(R(1, "A"), R(3, "C"), R(4, "D")))
  assert(snap3a(Approved) == Set(R(1, "A"), R(3, "C"), R(4, "D")))
  assert(snap3b(Approved) == Set(R(1, "A"), R(3, "C"), R(4, "D")))
}

"Try making a row recursively reach a view after the row's first transaction" - {
  val Root = Table("x")
  val Lock = Table("x")

  lazy val Catch: View = View("x") {
    Root("x") or (Catch("x") and Lock("x"))
  }

  val snap1 = Book(Catch) { INSERT INTO Root VALUES (1, 2, 3) }
  val snap2 = snap1 { INSERT INTO Lock VALUES (2, 3) }
  val snap3a = snap2 { DELETE FROM Root VALUES 2 }
  val snap3b = snap2 { DELETE FROM Lock VALUES 2 }

  // The point is that when "2" is removed from the "Root" table in "step3a",
  // it should also be removed from the "Catch" view. But this row doesn't look
  // obviously recusive to the "Catch" view, since it doesn't arrive multiple
  // times in the same transaction.

  assertConsistent(snap1)
  assertConsistent(snap2)
  assertConsistent(snap3a)
  assertConsistent(snap3b)

  assert(snap1(Root) == Set(R(1), R(2), R(3)))
  assert(snap1(Lock) == Set())
  assert(snap1(Catch) == Set(R(1), R(2), R(3)))

  assert(snap2(Root) == Set(R(1), R(2), R(3)))
  assert(snap2(Lock) == Set(R(2), R(3)))
  assert(snap2(Catch) == Set(R(1), R(2), R(3)))

  assert(snap3a(Root) == Set(R(1), R(3)))
  assert(snap3a(Lock) == Set(R(2), R(3)))
  assert(snap3a(Catch) == Set(R(1), R(3)))

  assert(snap3b(Root) == Set(R(1), R(2), R(3)))
  assert(snap3b(Lock) == Set(R(3)))
  assert(snap3b(Catch) == Set(R(1), R(2), R(3)))
}

"Try a simple view that just renames two columns." - {
  val AB = Table("a", "b")
  val BA = View("b", "a") { AB("a", "b") }

  val start = Book(BA)
  val snap1 = start { INSERT INTO AB VALUES (1, 2, 3, 4, 5, 6) }
  val snap2 = snap1 { DELETE FROM AB VALUES (3, 4) }
  assert(snap1(AB) == Set(R(1, 2), R(3, 4), R(5, 6)))
  assert(snap1(BA) == Set(R(2, 1), R(4, 3), R(6, 5)))
  assert(snap2(AB) == Set(R(1, 2), R(5, 6)))
  assert(snap2(BA) == Set(R(2, 1), R(6, 5)))
}

"Try a simple SELECT statement that just selects everyting" - {
  val ABC = Table("a", "b", "c")
  val CPY = View(SELECT ("*") FROM ABC)

  val start = Book(ABC, CPY)
  val snap1 = start { INSERT INTO ABC VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9) }
  val snap2 = snap1 { DELETE FROM ABC VALUES (4, 5, 6) }
  assert(snap1(ABC) == Set(R(1, 2, 3), R(4, 5, 6), R(7, 8, 9)))
  assert(snap1(CPY) == Set(R(1, 2, 3), R(4, 5, 6), R(7, 8, 9)))
  assert(snap2(ABC) == Set(R(1, 2, 3), R(7, 8, 9)))
  assert(snap2(CPY) == Set(R(1, 2, 3), R(7, 8, 9)))
}

"Make sure views can be mutually recusive" - {
  val Foo = Table("a", "b")
  lazy val Bar: View = View("a", "b") { Foo("b", "a") or Baz("a", "b") }
  lazy val Baz: View = View("a", "b") { Foo("a", "b") or Bar("b", "a") }
  val start = Book(Foo, Bar, Baz)
  val snap1 = start { INSERT INTO Foo VALUES (1, 2, 3, 4, 5, 6) }
  val snap2 = snap1 { DELETE FROM Foo VALUES (3, 4) }
  assert(snap1(Foo) == Set(R(1, 2), R(3, 4), R(5, 6)))
  assert(snap1(Bar) == Set(R(1, 2), R(3, 4), R(5, 6), R(2, 1), R(4, 3), R(6, 5)))
  assert(snap1(Baz) == Set(R(1, 2), R(3, 4), R(5, 6), R(2, 1), R(4, 3), R(6, 5)))
  assert(snap2(Foo) == Set(R(1, 2), R(5, 6)))
  assert(snap2(Bar) == Set(R(1, 2), R(5, 6), R(2, 1), R(6, 5)))
  assert(snap2(Baz) == Set(R(1, 2), R(5, 6), R(2, 1), R(6, 5)))
}

"Make sure objects can act like namespaces" - {
  object Foo {
    val Zim: View = View("a", "b") requires { Buz("a", "b") or Zim("b", "a") }
    val Bar = Table("a", "b")
    val Fiz = View("a", "b") requires { Bar("a", "b") or Buz("a", "b") }
    val Baz = View("b", "a") requires { Fiz("a", "b") }
    val Buz = Table("a", "b")
  }
  val start = Book(Foo.Bar, Foo.Fiz, Foo.Baz, Foo.Buz, Foo.Zim)
  val snap1 = start { INSERT INTO Foo.Bar VALUES (1, 2, 3, 4, 5, 6) }
  val snap2 = snap1 { DELETE FROM Foo.Bar VALUES (3, 4) }
  val snap3 = snap2 { INSERT INTO Foo.Buz VALUES (1, 2, 3, 4) }
  assert(snap1(Foo.Fiz) == Set(R(1, 2), R(3, 4), R(5, 6)))
  assert(snap1(Foo.Baz) == Set(R(2, 1), R(4, 3), R(6, 5)))
  assert(snap1(Foo.Zim) == Set())
  assert(snap2(Foo.Fiz) == Set(R(1, 2), R(5, 6)))
  assert(snap2(Foo.Baz) == Set(R(2, 1), R(6, 5)))
  assert(snap2(Foo.Zim) == Set())
  assert(snap3(Foo.Zim) == Set(R(1, 2), R(3, 4), R(2, 1), R(4, 3)))
}

"Reproduce compiler error seen in the jvc project" - {
  // SHOULD: Reduce this to a simpler test case.
  val ChildScope = Table("parent", "child")
  val Defines = Table("scope", "name", "target")
  val DefinesParameter = Table("scope", "name")
  val ImportsAll = Table("scope", "tree", "path")
  val Mentions = Table("scope", "tree", "name")
  val PackageScope = Table("path", "scope")
  val ProtocolScope = Table("outer", "tree", "inner")

  lazy val Ancestor: View = View("ancestor", "descendant") requires (
    ChildScope("ancestor", "descendant") or (
      Ancestor("ancestor", "relative") and
      Ancestor("relative", "descendant")
    )
  )

  lazy val Exports: View = View("path", "name", "target") requires (
    PackageScope("path", "scope")
    and DirectlyProvides("scope", "name", "target") where { rec => true }
  )

  lazy val ExposesProtocol: View = View("scope", "name", "target") requires (
    ProtocolScope("scope", "tree", "inner")
    and DirectlyProvides("inner", "name", "target") where { rec => true }
    butNot DefinesParameter("inner", "name")
    changing { rec => rec }
  )

  lazy val LocallyRefers: View = View("tree", "name", "scope", "target") requires (
    Mentions("scope", "tree", "name")
    and Provides("scope", "name", "target")
  )

  lazy val DirectlyProvides: View = View("scope", "name", "target") requires (
    Defines("scope", "name", "target")
    or ExposesProtocol("scope", "name", "target")
    or PointlessView("scope", "name", "target")
    or (
      PackageScope("path", "scope")
      and Exports("path", "name", "target")
    )
  )

  lazy val Shadows: View = View("scope", "name", "target") requires (
    DirectlyProvides("scope", "name", "target")
    and Ancestor("ancestor", "scope")
    and Provides("ancestor", "name", "other")
    changing { rec => rec }
  )

  lazy val Provides: View = View("scope", "name", "target") requires (
    Shadows("scope", "name", "target")
    or (
      DirectlyProvides("scope", "name", "target")
      butNot Shadows("scope", "name", "_1", "_2")
    )
  )

  lazy val Resolves: View = View("scope", "tree", "name", "target") requires (
    ImportsAll("scope", "tree", "path")
    and Exports("path", "name", "target")
    changing { rec => rec }
  )

  lazy val PointlessView: View = View("scope", "name", "target") requires (
    Resolves("scope", "_", "name", "target")
  )

  val start = Book(LocallyRefers, Provides)
  val snap1 = start
    { INSERT INTO PackageScope VALUES (Vector("foo"), 1) } THEN
    { INSERT INTO ChildScope VALUES (0, 1) } THEN
    { INSERT INTO Defines VALUES (1, "Bar", "<bar>") } THEN
    { INSERT INTO ChildScope VALUES (1, 2) } THEN
    { INSERT INTO ProtocolScope VALUES (1, 2, 2) } THEN
    { INSERT INTO Defines VALUES (2, "A", "<a>") } THEN
    { INSERT INTO DefinesParameter VALUES (2, "A") }

  assert(snap1(DirectlyProvides) == Set(R(1, "Bar", "<bar>"), R(2, "A", "<a>")))
  assertConsistent(snap1)

  val snap2 = start
    { INSERT INTO DefinesParameter VALUES (2, "A") } THEN
    { INSERT INTO Defines VALUES (2, "A", "<a>") } THEN
    { INSERT INTO ProtocolScope VALUES (1, 2, 2) } THEN
    { INSERT INTO ChildScope VALUES (1, 2) } THEN
    { INSERT INTO Defines VALUES (1, "Bar", "<bar>") } THEN
    { INSERT INTO ChildScope VALUES (0, 1) } THEN
    { INSERT INTO PackageScope VALUES (Vector("foo"), 1) }

  assertSame(snap1, snap2)
  assertConsistent(snap2)
}

"Try deleting a row involved in a collision on the negative side" - {
  val Positive = Table("name", "age")
  val Negative = Table("name", "count")
  val Allowed = View("name", "age") {
    Positive("name", "age") butNot Negative("name", "count")
  }

  val start = Book(Allowed)
  val snap1 = start { INSERT INTO Positive VALUES ("foo", 1, "bar", 2) }
  val snap2 = snap1 { INSERT INTO Negative VALUES ("bar", 3, "bar", 4) }
  val snap3 = snap2 { DELETE FROM Negative VALUES ("bar", 3) }

  assert(snap1(Allowed) == Set(R("foo", 1), R("bar", 2)))
  assert(snap2(Allowed) == Set(R("foo", 1)))
  assert(snap3(Allowed) == Set(R("foo", 1)))

  assertConsistent(snap1)
  assertConsistent(snap2)
  assertConsistent(snap3)
}

"Try overflowing the stack with speculative deletes" - {
  val Leader = Table("level")
  lazy val World: View = View("level") requires {
    Leader("level") or {
      World("level") expandingTo ("level") using { rec =>
        val level = rec[Int]("level")
        if (level > 0) {
          List(Record("level" -> level), Record("level" -> (level - 1)))
        } else {
          List(Record("level" -> level))
        }
      }
    }
  }

  // First, make sure the basics work as expected.
  val start = Book(World)
  val snap1 = start { INSERT INTO Leader VALUES (10, 3) }
  val snap2a = snap1 { DELETE FROM Leader VALUES (3) }
  val snap2b = snap1 { DELETE FROM Leader VALUES (10) }

  assert(start(World) == Set())
  assert(snap1(World) == Set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10).map(i => R(i)))
  assert(snap2a(World) == Set(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10).map(i => R(i)))
  assert(snap2b(World) == Set(0, 1, 2, 3).map(i => R(i)))

  // Now try kicking off a long chain of speculative deletes.
  val snapX = start { INSERT INTO Leader VALUES (5000, 1) }
  val snapY = snapX { DELETE FROM Leader VALUES (5000) }

  assert(snapY(World) == Set(R(0), R(1)))
}

"Make sure that columns named '_' are dropped" - {
  val Left = Table("color", "count", "status")
  val Right = Table("color", "count", "status")
  val Sides = View("color", "status1", "status2") {
    Left("color", "_", "status1") and Right("color", "_", "status2")
  }
  val start = Book(Sides)
  val snap = start {
    INSERT INTO Left VALUES ("red", "1", "found", "blue", 2, "found")
  } THEN {
    INSERT INTO Right VALUES ("red", "2", "running", "blue", 1, "waiting")
  }
  assert(snap(Sides) == Set(
    R("red", "found", "running"),
    R("blue", "found", "waiting")
  ))
}

"Try a simple aggregation with one group" - {
  val Foo = Table("a", "b", "c")
  val Agg = View(
    SELECT ("a", COUNT("*"), SET_OF("c"))
    FROM Foo
    GROUP_BY "a"
  )

  val start = Book(Foo, Agg)
  val snap1 = start {
    INSERT INTO Foo VALUES (
      "x", 1, 1,
      "y", 2, 1,
      "x", 3, 2,
      "y", 4, 2,
      "x", 5, 3,
      "y", 6, 2
    )
  }
  val snap2 = snap1 {
    DELETE FROM Foo VALUES (
      "x", 1, 1,
      "y", 6, 2
    )
  }
  val snap3 = snap2 {
    DELETE FROM Foo VALUES (
      "x", 3, 2,
      "y", 2, 1,
      "y", 4, 2
    )
  }

  assert(start(Foo) == Set())
  assert(start(Agg) == Set())
  assert(snap1(Agg) == Set(
    R("x", 3, Set(1, 2, 3)),
    R("y", 3, Set(1, 2))
  ))
  assert(snap2(Agg) == Set(
    R("x", 2, Set(2, 3)),
    R("y", 2, Set(1, 2))
  ))
  assert(snap3(Agg) == Set(R("x", 1, Set(3))))
}

"Try a simple aggregation with two groups" - {
  val Foo = Table("a", "b", "c", "d")
  val Agg = View(
    SELECT ("a", AVG("b"), "c", SUM("d"))
    FROM Foo
    GROUP_BY ("a", "c")
  )
  val start = Book(Foo, Agg)
  val snap1 = start {
    INSERT INTO Foo VALUES (
      "a", 1, true, 1,
      "a", 2, true, -1,
      "a", 3, true, 0,

      "a", 4, false, 10,
      "a", 5, false, 20,
      "a", 6, false, 30,

      "b", 10, true, 1,
      "b", 20, true, 1,

      "b", 10, false, 10,
      "b", -10, false, 0
    )
  }
  val snap2 = snap1 {
    DELETE FROM Foo VALUES (
      "a", 1, true, 1,
      "a", 5, false, 20,
      "a", 6, false, 30,
      "b", 20, true, 1,
      "b", 10, false, 10,
      "b", -10, false, 0
    )
  }

  assert(start(Foo) == Set())
  assert(start(Agg) == Set())
  assert(snap1(Agg) == Set(
    R("a", 2, true, 0),
    R("a", 5, false, 60),
    R("b", 15, true, 2),
    R("b", 0, false, 10),
  ))
  assert(snap2(Agg) == Set(
    R("a", 2.5, true, -1),
    R("a", 4, false, 10),
    R("b", 10, true, 1)
  ))
  assertConsistent(snap1)
  assertConsistent(snap2)
}

"Try MIN and MAX aggregate functions" - {
  val Foo = Table("a", "b")
  val Agg = View(
    SELECT ("a", MIN("b"), MAX("b"), SUM("b"), COUNT("b"))
    FROM Foo
    GROUP_BY "a"
  )
  val start = Book(Foo, Agg)
  val snap1 = start {
    INSERT INTO Foo VALUES (
      "x", 1,
      "x", 2,
      "x", 3,
      "x", 4,

      "y", -1,
      "y", -2,
      "y", -3,
      "y", -4,

      "z", -10,
      "z", 0,
      "z", 10
    )
  }
  val snap2 = snap1 {
    DELETE FROM Foo VALUES (
      "x", 1,
      "y", -3,
      "y", -4,
      "z", -10,
      "z", 0,
      "z", 10
    )
  }

  assert(start(Foo) == Set())
  assert(start(Agg) == Set())
  assert(snap1(Agg) == Set(
    R("x", 1, 4, 10, 4),
    R("y", -4, -1, -10, 4),
    R("z", -10, 10, 0, 3)
  ))
  assert(snap2(Agg) == Set(
    R("x", 2, 4, 9, 3),
    R("y", -2, -1, -3, 2)
  ))
  assertConsistent(snap1)
  assertConsistent(snap2)
}

"Try aggregate functions with no groups" - {
  val Foo = Table("a")
  val Agg = View(SELECT (MIN("a"), MAX("a")) FROM Foo)
  val start = Book(Foo, Agg)
  val snap1 = start { INSERT INTO Foo VALUES (1, 2, 3, 4, 5, 6) }
  val snap2 = snap1 { DELETE FROM Foo VALUES (1, 6) }
  val snap3 = snap2 { INSERT INTO Foo VALUES (0, 10) }
  val snap4 = snap3 { INSERT INTO Foo VALUES (1, 6) }
  assert(snap1(Agg) == Set(R(1, 6)))
  assert(snap2(Agg) == Set(R(2, 5)))
  assert(snap3(Agg) == Set(R(0, 10)))
  assert(snap4(Agg) == Set(R(0, 10)))
  assertConsistent(snap1)
  assertConsistent(snap2)
  assertConsistent(snap3)
  assertConsistent(snap4)
}

"Try using COUNT(*) with no groups" - {
  val Foo = Table("a", "b")
  val Agg = View(SELECT (COUNT("*")) FROM Foo)
  val start = Book(Foo, Agg)
  val snap1 = start { INSERT INTO Foo VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10) }
  val snap2 = snap1 { DELETE FROM Foo VALUES (1, 2, 3, 4) }
  assert(snap1(Agg) == Set(R(5)))
  assert(snap2(Agg) == Set(R(3)))
  assertConsistent(snap1)
  assertConsistent(snap2)
}

"Try simple TABLE_OF aggregate function" - {
  val Foo = Table("a", "b", "c")
  val Agg = View(SELECT ("a", TABLE_OF("b", "c") AS "t") FROM Foo GROUP_BY "a")
  val start = Book(Foo, Agg)
  val snap1 = start { INSERT INTO Foo VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12) }
  val snap2 = snap1 { DELETE FROM Foo VALUES (1, 2, 3, 7, 8, 9) }
  val snap3 = snap2 { INSERT INTO Foo VALUES (4, 50, 60, 10, 11, 13) }
  assert(snap1(Agg) == Set(
    R(1, Set(R(2, 3))), R(4, Set(R(5, 6))),
    R(7, Set(R(8, 9))), R(10, Set(R(11, 12))),
  ))
  assert(snap2(Agg) == Set(
    R(4, Set(R(5, 6))), R(10, Set(R(11, 12))),
  ))
  assert(snap3(Agg) == Set(
    R(4, Set(R(5, 6), R(50, 60))), R(10, Set(R(11, 12), R(11, 13))),
  ))
  assertConsistent(snap1)
  assertConsistent(snap2)
  assertConsistent(snap3)
}

"Try another TABLE_OF aggregate function" - {
  val Foo = Table("a", "b", "c", "d")
  val Agg = View(SELECT ("a", TABLE_OF("b", "c") AS "t") FROM Foo GROUP_BY "a")
  val start = Book(Foo, Agg)
  val snap1 = start { INSERT INTO Foo VALUES (
    1, 2, 3, 4,
    1, 2, 3, 5,
    2, 3, 4, 5,
    3, 4, 5, 6,
  ) }
  val snap2a = snap1 { DELETE FROM Foo VALUES (1, 2, 3, 4, 2, 3, 4, 5) }
  val snap2b = snap1 { DELETE FROM Foo VALUES (1, 2, 3, 5, 3, 4, 5, 6) }
  assert(snap1(Agg) == Set(
    R(1, Set(R(2, 3))), R(2, Set(R(3, 4))), R(3, Set(R(4, 5))),
  ))
  assert(snap2a(Agg) == Set(
    R(1, Set(R(2, 3))), R(3, Set(R(4, 5))),
  ))
  assert(snap2b(Agg) == Set(
    R(1, Set(R(2, 3))), R(2, Set(R(3, 4))),
  ))
  assertConsistent(snap1)
  assertConsistent(snap2a)
  assertConsistent(snap2b)
}
}}
