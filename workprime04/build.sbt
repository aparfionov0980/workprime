name := "workprime04"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "com.ibm.db2.jcc" % "db2jcc" % "db2jcc4",
  "com.ibm.stocator" % "stocator" % "1.0.35",
  "org.apache.spark" % "spark-core_2.11" % "2.4.3",
  "org.apache.spark" % "spark-sql_2.11" % "2.4.3",
  "org.scalatest" % "scalatest_2.11" % "3.0.8" % Test,
  "org.scalamock" % "scalamock_2.11" % "4.4.0" % Test
)

resolvers += "jitpack" at "https://jitpack.io"
libraryDependencies += "com.github.mrpowers" % "spark-fast-tests" % "v0.16.0" % "test"