name := "high-performance-spark"

version := "0.1"

//sbtPlugin := true

scalaVersion := "2.11.12"

//autoScalaLibrary := false

//unmanagedClasspath in (Compile, runMain) += baseDirectory.value / "resources"

resourceDirectory in Compile := baseDirectory.value /"src/main/resources"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.3.1"
libraryDependencies += "org.apache.spark" %% "spark-hive-thriftserver" % "2.3.1" % "provided"
// https://mvnrepository.com/artifact/mysql/mysql-connector-java
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.11"


// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0-SNAP10" % Test
