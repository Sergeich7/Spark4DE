ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "features"
  )

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.7" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.7" % "provided"
