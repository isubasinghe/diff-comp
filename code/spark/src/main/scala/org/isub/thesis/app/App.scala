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
import org.apache.spark.broadcast.Broadcast

class VertexState extends Serializable {
  var community = -1L
  var communitySigmaTot = 0L
  var internalWeight = 0L
  var nodeWeight = 0L
  var changed = false

  override def toString(): String = {
    "{community:" + community + ",communitySigmaTot:"+communitySigmaTot+",internalWeight:"+internalWeight+",nodeWeight:"+nodeWeight+"}"
  }
}

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
      nodes = edges.flatMap(y => List( (y._1, y._2))).toSet.toList
      edgeRdd = sc.parallelize(edges.map(e => Edge(e._1, e._2, e._3)))
      vertexRDD = sc.parallelize(nodes)
      graph = Graph(vertexRDD, edgeRdd)
    } yield(graph)

  }
}

object Louvain {

  private def sendMsg(ec: EdgeContext[VertexState, Long, Map[(Long, Long), Long]]) = {
    val m1 = Map((ec.srcAttr.community,ec.srcAttr.communitySigmaTot)->ec.attr)
    val m2 = Map((ec.dstAttr.community, ec.dstAttr.communitySigmaTot)->ec.attr)
    ec.sendToSrc(m2)
    ec.sendToDst(m1)
  }

  private def mergeMsg(m1: Map[(Long, Long), Long], m2: Map[(Long, Long), Long]): Map[(Long, Long), Long] = {
    val newMap = scala.collection.mutable.HashMap[(Long, Long), Long]()
    m1.foreach({case (k,v)=>
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    m2.foreach({case (k,v)=> 
      if (newMap.contains(k)) newMap(k) = newMap(k) + v
      else newMap(k) = v
    })
    newMap.toMap
  }

  def toLouvainGraph(graph: Graph[Long, Long]): Graph[VertexState, Long] = {
    val nodeWeightMapFunc = (triplet: EdgeContext[Long, Long, Long]) => { 
      triplet.sendToSrc(triplet.attr)
      triplet.sendToDst(triplet.attr)
    }
    val nodeWeightReduceFunc = (e1: Long, e2: Long) => e1 + e2
    val nodeWeights = graph.aggregateMessages(nodeWeightMapFunc, nodeWeightReduceFunc)
    val louvainGraph = graph.outerJoinVertices(nodeWeights)((vid, data, weightOption) => {
      val weight:Long = weightOption.getOrElse(0)
      val state = new VertexState()
      state.community = vid
      state.changed = false
      state.communitySigmaTot = weight
      state.internalWeight = 0L
      state.nodeWeight = weight
      state
    }).partitionBy(PartitionStrategy.EdgePartition2D).groupEdges(_+_)
    louvainGraph 
  }

  def iterate(sc: SparkContext, graph: Graph[VertexState, Long]) = {
    var louvainGraph = graph.cache()
    val graphWeight = louvainGraph.vertices.values.map(vdata => vdata.internalWeight+vdata.nodeWeight).reduce(_+_)
    var totalGraphWeight = sc.broadcast(graphWeight)
    // VertexId[Map[(Long, Long), Long]]
    var msgRdd = louvainGraph.aggregateMessages(sendMsg, mergeMsg)
    var activeMsgs = msgRdd.count()
    var count = 0
    var even = true;
    val minProgress = 1L;
    var updated = 0L - minProgress;
    var updatedLastPhase = 0L;
    var stop = 0L
    do {
      val labelledVerts = louvainVertJoin(louvainGraph, msgRdd, totalGraphWeight, even)

      val communityUpdate = labelledVerts
        .map({case (vid, vdata) => (vdata.community, vdata.nodeWeight+vdata.internalWeight)})
        .reduceByKey(_+_).cache()

      val communityMapping = labelledVerts
        .map({case (vid, vdata)=> (vdata.community, vid)})
        .join(communityUpdate)
        .map({case (community, (vid, sigmaTot))=>(vid, (community, sigmaTot))})
        .cache()
      
      val updatedVerts = labelledVerts.join(communityMapping).map({case (vid, (vdata, communityTuple))=>{
        vdata.community = communityTuple._1
        vdata.communitySigmaTot = communityTuple._2
        (vid, vdata)
      }}).cache()

      updatedVerts.count()
      labelledVerts.unpersist(blocking=false)
      communityUpdate.unpersist(blocking=false)
      communityMapping.unpersist(blocking=false)

      val prevG = louvainGraph
      louvainGraph = louvainGraph.outerJoinVertices(updatedVerts)((vid, old, newOpt)=>newOpt.getOrElse(old))
      louvainGraph.cache()

      val oldMsgs = msgRdd
      msgRdd = louvainGraph.aggregateMessages(sendMsg, mergeMsg)
      activeMsgs = msgRdd.count()

      oldMsgs.unpersist(blocking=false)
      updatedVerts.unpersist(blocking=false)
      prevG.unpersist(blocking=false)
      
      if (even) updated = 0
      updated = updated + louvainGraph.vertices.filter(_._2.changed).count
      if(!even) {
        if (updated >= updatedLastPhase - minProgress) stop += 1
        updatedLastPhase = updated
      }  
      even = !even 
      count += 1
    } while(count != 1000)

  }

  def louvainVertJoin(louvainGraph: Graph[VertexState, Long], msgRdd: VertexRDD[Map[(Long, Long), Long]], totalEdgeWeight: Broadcast[Long], even: Boolean) = {
    louvainGraph.vertices.innerJoin(msgRdd)((vid, vdata, msgs)=> {
      var bestCommunity = vdata.community
      var startingCommunityId = bestCommunity
      var maxDeltaQ = BigDecimal(0.0)
      var bestSigmaTot = 0L
      msgs.foreach({ case((community, sigmaTotal), communityEdgeWeight)=>
        val deltaQ = q(startingCommunityId, community, sigmaTotal, communityEdgeWeight, vdata.nodeWeight, vdata.internalWeight, totalEdgeWeight.value)
        if(deltaQ > maxDeltaQ) {
          maxDeltaQ = deltaQ
          bestCommunity = community
          bestSigmaTot = sigmaTotal
        }
      })
      if( (vdata.community != bestCommunity) && ((even && vdata.community > bestCommunity) || (!even && vdata.community < bestCommunity)) ) {
          vdata.community = bestCommunity
          vdata.communitySigmaTot = bestSigmaTot
          vdata.changed = true
      }else {
        vdata.changed = false
      }
      vdata
    }) 
  }

  def q(currCommunityId: Long, testCommunityId:Long, testSigmaTot:Long, edgeWeightInCommunity: Long, nodeWeight:Long, internalWeight:Long, totalEdgeWeight: Long): BigDecimal = {
    val isCurrentCommunity = currCommunityId.equals(testCommunityId)
    val M = BigDecimal(totalEdgeWeight)
    val k_i_in_L = if (isCurrentCommunity) edgeWeightInCommunity+internalWeight else edgeWeightInCommunity
    val k_i_in = BigDecimal(k_i_in_L)
    val k_i = BigDecimal(nodeWeight+internalWeight)
    val sigma_tot = if (isCurrentCommunity) BigDecimal(testSigmaTot) - k_i else BigDecimal(testSigmaTot)
    var deltaQ = BigDecimal(0)
    if(!(isCurrentCommunity && sigma_tot.equals(0.0))) {
      deltaQ = k_i_in - (k_i*sigma_tot/M)
    }
    deltaQ
  }
}

object App extends zio.App {
  
  def run(args: List[String]) = 
    appLogic.exitCode

  val conf = new SparkConf().setAppName("").setMaster("")
  val sc = new SparkContext(conf)

  val appLogic =
    for {
      either_graph <- GraphParser.parse("./data.txt", sc)
      _ <- printLine("Hello World")
    } yield()

  
}
