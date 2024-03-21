ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "data_mart"
  )

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.7" % "provided"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.7" % "provided"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3"
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "6.8.22"
libraryDependencies += "org.postgresql" % "postgresql" % "42.3.3"