package final_project

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession
import scala.util.Random
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD


object main {
  // quieten Spark logging
  Logger.getRootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)

  def parallelPivotOriginal(g_in: Graph[Int, Int]): Graph[Long, Int] = {
     val verticesRandom = g_in.vertices.map {
         case (id, attr) => (
           id, scala.util.Random.nextDouble()
         )
       }
    val verticesRanked: RDD[(VertexId, Long)] = verticesRandom
       .sortBy(_._2)
       .zipWithIndex()
       .map {
          case ((id, _), idx) => (id, idx+1)
        }.cache()

    val g_new = Graph(verticesRanked, g_in.edges)
    var g: Graph[Long, Int] =
      g_new.mapVertices { case (vid, _) => 0L }

    val rank: Graph[(Long, Double), Int] = g.outerJoinVertices(verticesRanked) {
      case (vid, cid, Some(rank)) =>
        val newRank = if (cid == 0L) rank.toDouble else Double.PositiveInfinity
        (cid, newRank)

      case (vid, cid, None) =>
        (cid, Double.PositiveInfinity)
    }

    var nUnassigned = g.vertices.filter { case (_, cid) => cid == 0L }.count()
    var iteration = 0

    while (nUnassigned > 0) {
      iteration += 1

      // 1) give each unassigned vertex a fresh random rank; assigned ones get +∞
      var ranked: Graph[(Long, Double), Int] = g.mapVertices {
        case (vid, cid) =>
          val rank = if (cid == 0L) Random.nextDouble() else Double.PositiveInfinity
          (cid, rank)
      }
      
      ranked = ranked.outerJoinVertices(verticesRanked) {
        case (vid, (cid, oldRank), Some(newRank)) =>
          (cid, if (cid == 0L) newRank.toDouble else Double.PositiveInfinity)

        case (vid, (cid, oldRank), None) =>
          (cid, Double.PositiveInfinity)
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

  def disagreements(g: Graph[Long,Int]): Long = g.triplets.filter(t => t.srcAttr != t.dstAttr).count()

  def signedCost(g: Graph[Long,Int]): Long = {
    val cuts = g.triplets.filter(t => t.attr > 0 && t.srcAttr != t.dstAttr).count()

    // edges that actually exist inside clusters
    val posInside = g.triplets.filter(t => t.srcAttr == t.dstAttr).count()

    // for each cluster, number of possible pairs  nC choose 2
    val clusterSizes = g.vertices.map(_._2).countByValue()  // Map[clusterId,Long]
    val totalPairsInside =
      clusterSizes.values.map(n => n*(n-1)/2).sum

    val missingInside = totalPairsInside - posInside
    cuts + missingInside
  }

  /** Local search to improve clustering */
  def localSearch(graph: Graph[Long, Int], sc: SparkContext, numIterations: Int = 10): Graph[Long, Int] = {
    var g = graph
    for (iter <- 1 to numIterations) {
      println(s"[LocalSearch] Starting iteration $iter")
      // println(s"[LocalSearch] cost before iter $iter = ${signedCost(g)}")

      // Store current vertex assignments before making changes
      val oldVertices = g.vertices.cache()

      val clusterSizes = g.vertices                        // (vid , cid)
                          .map { case (_, cid) => (cid, 1L) }
                          .reduceByKey(_ + _)
                          .collectAsMap()

      val neighborClusterCounts = g.aggregateMessages[Map[Long, Int]](
        ctx => {
          ctx.sendToSrc(Map(ctx.dstAttr -> 1))
          ctx.sendToDst(Map(ctx.srcAttr -> 1))
        },
        (m1, m2) => (m1.keySet ++ m2.keySet)
                      .map(k => k -> (m1.getOrElse(k,0) + m2.getOrElse(k,0)))
                      .toMap
      ).persist()

      val bcSizes = sc.broadcast(clusterSizes)  

      val moves = g.vertices.join(neighborClusterCounts).map {
        case (vid, (cid, neigh)) =>
          val a      = neigh.getOrElse(cid, 0)
          val choices = neigh - cid // every neighbour cluster D

          if (choices.isEmpty) {
            (vid, cid)
          } else {
            val (bestD, b) = choices.maxBy(_._2) // the D with largest b
            val nC  = bcSizes.value(cid)
            val nD  = bcSizes.value(bestD)

            val delta = nD - nC + 1 + 2*(a - b)
            if (delta < 0) (vid, bestD) else (vid, cid)
          }
      }
      g = Graph(moves, g.edges)
      
      // Compare old and new assignments to count changes
      val changesMade = oldVertices.join(moves)
        .filter { case (_, (oldCid, newCid)) => oldCid != newCid }
        .count()
      neighborClusterCounts.unpersist(blocking = false)
      oldVertices.unpersist()
      
      println(s"[LocalSearch] Iteration $iter completed, changes made: $changesMade")
      // println(s"[LocalSearch] cost  after iter $iter = ${signedCost(g)}")
      if (changesMade == 0) {
        println("[LocalSearch] No improvements found, stopping early")
        return g
      }
    }
    g
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      System.err.println("Usage: final_project method={original, random} <input_edges.csv> <output_path.csv> <n_iterations>")
      System.exit(1)
    }
    val method = args(0)
    val inputPath = args(1)
    val outputPath = args(2)
    val tmpDir = outputPath + "_tmp"

    val conf = if (method == "random") new SparkConf().setAppName("ParallelizedPivotClustering")
      else new SparkConf().setAppName("OriginalParallelizedPivotClustering")
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

    var clustered: Graph[Long, Int] = null

    val startTime = System.nanoTime()
    if (method == "random") {
      println("=== Starting Parallel Pivot Clustering ===")
      clustered = parallelPivotClustering(graph)
    } else if (method == "original") {
      println("=== Starting Original Parallel Pivot Clustering ===")
      clustered = parallelPivotOriginal(graph)
    } else {
      println("not a valid algorithm input")
      System.exit(1)
    }
    println("=== Starting Local Search ===")
    // Add checking to make sure 4th argument is an int (if necessary)
    clustered = localSearch(clustered, sc, args(3).toInt)
    val vertices = clustered.vertices.count()
    println()
    println("==========================================================")
    println(s"Number of vertices: $vertices")
    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e9d
    println(f"Clustering completed in $duration%.2f seconds")
    println("==========================================================")   
    println()

    val g2df = spark.createDataFrame(clustered.vertices)
    g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))

    println(s"=== Done! Clusters written to $outputPath ===")
    sc.stop()
  }
}
