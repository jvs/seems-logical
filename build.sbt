name := "seems-logical"
version := "0.1-SNAPSHOT"
scalaVersion := "2.12.2"
scalacOptions := Seq("-unchecked", "-deprecation", "-feature")
libraryDependencies += "com.lihaoyi" %% "utest" % "0.6.0" % "test"
testFrameworks += new TestFramework("utest.runner.Framework")
