name := "Mijdbc1"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.11.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.0.0"

libraryDependencies += "postgresql" % "postgresql" % "9.1-901-1.jdbc4"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"
