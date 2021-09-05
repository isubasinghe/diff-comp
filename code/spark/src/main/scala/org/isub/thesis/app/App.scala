package org.isub.thesis.app

import scala.io.Source
import scala.util.Either
import zio.Console._
import org.apache.spark.graphx.{Edge, EdgeRDD, Graph, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.commons.lang3.exception.ExceptionUtils
import zio.ZIO
import zio.UIO
import zio.URIO



object GraphParser {
  def splitString(file: String): Either[String, (Int, Int)] = {
    val arr = file.split(" ").map(_.toInt)
    if (arr.length != 2) {
      val tup = (arr(0), arr(1))
      return Right(tup)
    }else {
      return Left("Need exactly 2 nodes per line")
    }
  }

  def parse(file: String) =  zio.IO {
    Source
    .fromFile(file)
    .getLines()
    .map(splitString)
  }
}

object App extends zio.App {
  
  def run(args: List[String]) = 
    appLogic.exitCode
  
  val appLogic = 
    for {
      _ <- GraphParser.parse("./data.txt")
      _ <- printLine("Hello World")
    } yield()

  
}
