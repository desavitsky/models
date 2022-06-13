name := "spark"

version := "0.1"

scalaVersion := "2.13.8"

val root = (project in file("."))
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.2.1",
      "org.apache.spark" %% "spark-sql" % "3.2.1",
      "org.apache.spark" %% "spark-streaming" % "3.2.1",
      "org.apache.spark" %% "spark-mllib" % "3.2.1"
    )
  )