name := "spark"

version := "0.1"

scalaVersion := "2.12.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.0.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.0.2"
libraryDependencies += "log4j" % "log4j" % "1.2.17"