package final_project

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession
import scala.util.Random
import org.apache.hadoop.fs.{FileSystem, Path}

object main {
  // quieten Spark logging
  Logger.getRootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  /** 
   * Parallelized‐Pivot clustering on an undirected, unweighted graph.
   * Vertices carry a Long clusterId; 0 means "not assigned yet".
   * Returns same Graph with each vertex's clusterId set to the pivot it joined.
   */
  def parallelPivotClustering(g_in: Graph[Int, Int]): Graph[Long, Int] = {
    // initialize all clusterIds to 0 (i.e. "unassigned")
    var g: Graph[Long, Int] =
      g_in.mapVertices { case (vid, _) => 0L }

    var nUnassigned = g.vertices.filter { case (_, cid) => cid == 0L }.count()
    var iteration = 0

    while (nUnassigned > 0) {
      iteration += 1

      // 1) give each unassigned vertex a fresh random rank; assigned ones get +∞
      val ranked: Graph[(Long, Double), Int] = g.mapVertices {
        case (vid, cid) =>
          val rank = if (cid == 0L) Random.nextDouble() else Double.PositiveInfinity
          (cid, rank)
      }

      // 2) for each unassigned vertex, find the minimum rank among its unassigned neighbors
      val neighborMinRank: VertexRDD[Double] = ranked.aggregateMessages[Double](
        triplet => {
          val (srcCid, srcRank) = triplet.srcAttr
          val (dstCid, dstRank) = triplet.dstAttr
          // only consider edges where both endpoints are still unassigned
          if (srcCid == 0L && dstCid == 0L) {
            triplet.sendToSrc(dstRank)
            triplet.sendToDst(srcRank)
          }
        },
        // take the smallest rank seen
        math.min
      )

      // 3) identify the pivot set S = { v : rank(v) < minNeighborRank(v) }
      val isPivot: VertexRDD[Boolean] = ranked.vertices.leftJoin(neighborMinRank) {
        case (vid, (cid, rank), Some(minNb)) => cid == 0L && rank < minNb
        case (vid, (cid, _),    None      ) => cid == 0L
      }

      // 4) prepare a triplet view that carries (oldCid, rank, isPivot) at each vertex
      val withPivotAttr: Graph[(Long, Double, Boolean), Int] =
        ranked.outerJoinVertices(isPivot) {
          case (vid, (cid, rank), Some(isP)) => (cid, rank, isP)
          case (vid, (cid, rank), None) => (cid, rank, false)  // Handle case where vertex wasn't in isPivot RDD
        }

      // 5a) collect messages from each pivot to itself -> cluster = pivotId
      val pivotAssignments: VertexRDD[Long] =
        ranked.vertices.innerJoin(isPivot) { (vid, attr, isP) =>
          val (cid, _) = attr
          if (isP) vid else cid
      }
      
      // 5b) collect messages from each pivot to its unassigned neighbors
      //    since S is an independent set, no neighbor has two pivots, so no need to break ties
      val neighborAssignments: VertexRDD[Long] = withPivotAttr.aggregateMessages[Long](
        triplet => {
          val (srcCid, _, srcIsP) = triplet.srcAttr
          val (dstCid, _, dstIsP) = triplet.dstAttr
          if (srcCid == 0L && dstCid == 0L && srcIsP) {
            triplet.sendToDst(triplet.srcId)
          }
          if (srcCid == 0L && dstCid == 0L && dstIsP) {
            triplet.sendToSrc(triplet.dstId)
          }
        },
        // if somehow two pivots did touch the same neighbor, pick the smaller ID
        (a, b) => math.min(a, b)
      )

      // 6) update cluster IDs in two steps:
      //    a) assign pivot‐self messages
      val afterPivots: Graph[Long, Int] = g.outerJoinVertices(pivotAssignments) {
        case (vid, oldCid, Some(pivotId)) if oldCid == 0L => pivotId
        case (vid, oldCid, _)                          => oldCid
      }

      //    b) assign neighbor messages
      g = afterPivots.outerJoinVertices(neighborAssignments) {
        case (vid, oldCid, Some(pivotId)) if oldCid == 0L => pivotId
        case (vid, oldCid, _)                            => oldCid
      }

      nUnassigned = g.vertices.filter { case (_, cid) => cid == 0L }.count()
      println(s"[ParallelPivot] iteration=$iteration, remaining unassigned=$nUnassigned")
    }

    g
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: final_project <input_edges.csv> <output_path.csv>")
      System.exit(1)
    }
    val inputPath = args(0)
    val outputPath = args(1)
    val tmpDir = outputPath + "_tmp"

    val conf = new SparkConf().setAppName("ParallelizedPivotClustering")
    val sc   = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    // 1) load edges
    val edges = sc.textFile(inputPath).map { line =>
      val Array(a,b) = line.split(",").map(_.toLong)
      Edge(a, b, 1)
    }
    val graph = Graph.fromEdges[Int,Int](
      edges,
      defaultValue        = 0,
      edgeStorageLevel    = StorageLevel.MEMORY_AND_DISK,
      vertexStorageLevel  = StorageLevel.MEMORY_AND_DISK
    )

    println("=== Starting Parallel Pivot Clustering ===")
    val clustered: Graph[Long,Int] = parallelPivotClustering(graph)

    // 2) write to a *temporary* folder, coalesced to one part
    import spark.implicits._
    clustered.vertices
      .toDF("vertexId","clusterId")
      .coalesce(1)
      .write
      .option("header","false")
      .mode("overwrite")
      .csv(tmpDir)

    // 3) move the part-*.csv up to finalCsv
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val tmpPath = new Path(tmpDir)
    val part = fs
      .listStatus(tmpPath)
      .map(_.getPath)
      .find(_.getName.startsWith("part-"))
      .getOrElse(throw new RuntimeException(s"No part file in $tmpDir"))
    fs.rename(part, new Path(outputPath))

    // 4) delete the temp directory
    fs.delete(tmpPath, true)

    println(s"=== Done! Clusters written to $outputPath ===")
    sc.stop()
  }
}
