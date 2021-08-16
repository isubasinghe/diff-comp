package org.isub.thesis.app

import org.apache.spark.graphx.{Edge, EdgeRDD, Graph, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.commons.lang3.exception.ExceptionUtils

class VertexProperty()
case class UserProperty(val name: String) extends VertexProperty
case class ProductProperty(val name: String, val price: Double) extends VertexProperty

abstract class Graph[VD, ED] {
  val vertices: VertexRDD[VD]
  val edges: EdgeRDD[ED]
}


object App {
  def main(args: Array[String]): Unit = {
    val graph: Graph[String, String] = null
    println("Hello World")
  }
}
