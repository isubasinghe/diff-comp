package org.isub.thesis.app

import org.apache.spark.graphx.{Edge, EdgeRDD, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class VertexProperty()
case class UserProperty(val name: String) extends VertexProperty
case class ProductProperty(val name: String, val price: Double) extends VertexProperty

abstract class Graph[VD, ED] {
  val vertices: VertexRDD[VD]
  val edges: EdgeRDD[ED]
}


object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("clustering")
      .getOrCreate()

    val sc = spark.sparkContext

    val users: RDD[Edge[String]] = null

    val relationships: RDD[Edge[String]] = null

    spark.stop()

  }
}
