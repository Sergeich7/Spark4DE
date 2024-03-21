ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "lab02"
  )

val sparkVersion = "2.4.7"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided
)
//libraryDependencies += "org.apache.spark" % "spark-core_2.11" % sparkVersion % "provided"
//libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % sparkVersion % "provided"
