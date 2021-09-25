name := "spark"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.1.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.1.2" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.1.2"
libraryDependencies += "log4j" % "log4j" % "1.2.17"
libraryDependencies += "dev.zio" %% "zio" % "2.0.0-M2"
libraryDependencies += "dev.zio" %% "zio-streams" % "2.0.0-M2"
libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.0.0"
libraryDependencies += "org.typelevel" %% "cats-core" % "2.3.0"
libraryDependencies += "org.typelevel" %% "cats-effect" % "2.5.3" withSources() withJavadoc()
libraryDependencies += "org.typelevel" %% "cats-collections-core" % "0.9.0"
libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.3.5"

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-unchecked",
  "-language:postfixOps",
  "-language:higherKinds",
  "-Ypartial-unification")
