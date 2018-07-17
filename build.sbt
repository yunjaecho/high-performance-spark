name := "high-performance-spark"

version := "0.1"

//sbtPlugin := true

scalaVersion := "2.11.12"

//autoScalaLibrary := false

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-hive-thriftserver" % "2.3.1" % "provided"
