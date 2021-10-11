package org.isub.thesis.app

import scala.io.Source
import scala.util.Either
import zio.Console._
import org.apache.spark.graphx.{Edge, EdgeRDD, Graph, VertexRDD}
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.commons.lang3.exception.ExceptionUtils
import zio.ZIO
import zio.UIO
import zio.URIO
import cats._
import scalaz._
import Scalaz._


object GraphParser {
  def splitString(file: String): Either[String, (Long, Long, Long)] = {
    val arr = file.split(" ").map(_.toLong)
    if (arr.length != 2) {
      val tup = (arr(0), arr(1), 1L)
      return Right(tup)
    }else {
      return Left("Need exactly 2 nodes per line")
    }
  }

  def parse(file: String, sc: SparkContext) =  zio.IO {
    // Either[Error, (NodeID, NodeID, Weight)]

    for {
      edges <- Source.fromFile(file).getLines().map(splitString).toList.sequence
      nodes = edges.flatMap(y => List( (y._1, y._1) , (y._2, y._2))).toSet.toList
      edgeRdd = sc.parallelize(edges.map(e => Edge(e._1, e._2, e._3)))
      vertexRDD = sc.parallelize(nodes)
      graph = Graph(vertexRDD, edgeRdd)
    } yield(graph)

  }
}

object Louvain {
  def iterate(graph: Graph[Long, Long]) = ()
}

object App extends zio.App {
  
  def run(args: List[String]) = 
    appLogic.exitCode

  val conf = new SparkConf().setAppName("").setMaster("")
  val sc = new SparkContext(conf)

  val appLogic =
    for {
      either_graph <- GraphParser.parse("./data.txt", sc)
      graph = either_graph.map(Louvain.iterate)
      _ <- printLine("Hello World")
    } yield()

  
}
