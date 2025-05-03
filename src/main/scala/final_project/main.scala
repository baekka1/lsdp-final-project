package final_project

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SparkSession
import scala.util.Random
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.util.hashing.MurmurHash3
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast


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
   * Vertices carry a clusterId; 0 means "not assigned yet".
   * Returns same Graph with each vertex's clusterId set to the pivot it joined.
   */
  def parallelPivotClustering(g_in: Graph[Int, Int]): Graph[Long, Int] = {
    // 0) Partition + assign (clusterID, rank) once
    var g: Graph[(Long,Double), Int] = g_in
      .partitionBy(PartitionStrategy.EdgePartition2D)
      .mapVertices { case (vid, _) =>
        val rank = MurmurHash3.stringHash(vid.toString).toDouble
        (0L, rank)
      }
      .persist(StorageLevel.MEMORY_AND_DISK)

    var iter = 0
    var continue = true

    while (continue) {
      iter += 1

      // 1) min‐neighbor‐rank among unassigned
      val neighborMin: VertexRDD[Double] = g.aggregateMessages[Double](
        sendMsg = ctx => {
          val (sc, _) = ctx.srcAttr
          val (dc, _) = ctx.dstAttr
          if (sc == 0L && dc == 0L) {
            ctx.sendToSrc(ctx.dstAttr._2)
            ctx.sendToDst(ctx.srcAttr._2)
          }
        },
        mergeMsg = math.min
      )

      // 2) pivot flag per vertex
      val isPivot: VertexRDD[Boolean] = g.vertices.leftJoin(neighborMin) {
        case (_, (cid, rank), optMin) => cid == 0L && optMin.forall(rank < _)
      }

      // 3a) pivots assign themselves via innerJoin (no shuffle)
      val pivotSelf: VertexRDD[Long] = g.vertices
        .innerJoin(isPivot) {
          case (vid, (cid, _), isP) =>
            if (isP && cid == 0L) vid else 0L
        }
        .filter { case (_, cid) => cid != 0L }

      // 3b) inject pivot‐flag into the triplets
      val enriched: Graph[(Long,Double,Boolean), Int] = g.outerJoinVertices(isPivot) {
        case (_, (cid, rank), Some(flag)) => (cid, rank, flag)
        case (_, (cid, rank), None)       => (cid, rank, false)
      }

      // 3c) pivots assign neighbors
      val pivotNbrsRaw: RDD[(VertexId, Long)] = enriched.aggregateMessages[Long](
        sendMsg = ctx => {
          val (sc, _, sp) = ctx.srcAttr
          val (dc, _, dp) = ctx.dstAttr
          if (sp && dc == 0L) ctx.sendToDst(ctx.srcId)
          if (dp && sc == 0L) ctx.sendToSrc(ctx.dstId)
        },
        mergeMsg = math.min
      )

      // 4) collect all new assignments
      val allUpdatesRdd: RDD[(VertexId, Long)] =
        pivotSelf.union(pivotNbrsRaw)
                 .filter(_._2 != 0L)
                 .distinct()

      // if no new assignments, we're done
      if (allUpdatesRdd.isEmpty()) {
        continue = false
      } else {
        // wrap into VertexRDD so we can joinVertices
        val updates: VertexRDD[Long] = VertexRDD(allUpdatesRdd)

        println(s"[iter $iter] assigning ${updates.count()} vertices")

        // 5) apply only the delta via map‐side join
        g = g.joinVertices(updates) {
          case (_, (oldCid, rank), newCid) =>
            if (oldCid == 0L) (newCid, rank) else (oldCid, rank)
        }
      }
    }

    // 6) strip off the rank
    g.mapVertices { case (_, (cid, _)) => cid }
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
  def localSearch(
        graph: Graph[Long,Int],
        sc: SparkContext,
        numIter: Int = 10): Graph[Long,Int] = {

    var g = Graph(graph.vertices, graph.edges.cache())   // keep edges cached

    for (iter <- 1 to numIter) {
      println(s"[LS] iter $iter   cost = ${signedCost(g)}")

      // 1) tiny broadcast: cluster sizes
      val bcSize = sc.broadcast(
        g.vertices.map { case (_,cid) => (cid,1L) }
                  .reduceByKey(_+_)
                  .collectAsMap()
      )

      // 2) heavy neighbour map  (vid -> Map[cid -> deg])
      val neigh = g.aggregateMessages[Map[Long,Int]](
        ctx => {
          ctx.sendToSrc(Map(ctx.dstAttr -> 1))
          ctx.sendToDst(Map(ctx.srcAttr -> 1))
        },
        (m1,m2) => (m1.keySet ++ m2.keySet)
                    .map(k => k -> (m1.getOrElse(k,0)+m2.getOrElse(k,0)))
                    .toMap
      ).persist(StorageLevel.MEMORY_AND_DISK_SER)

      // 3) attach random priority to each vertex
      val gWithPrio = g.mapVertices { case (_,cid) =>
        (cid, scala.util.Random.nextDouble())
      }

      // 4) min neighbour priority
      val minPrio = gWithPrio.aggregateMessages[Double](
        t => {
          val (_,pSrc) = t.srcAttr ; val (_,pDst) = t.dstAttr
          t.sendToSrc(pDst) ; t.sendToDst(pSrc)
        },
        math.min
      )

      // 5) compute moves on the independent‑set vertices
      val candidates = gWithPrio.vertices // (vid → (cid,prio))
        .join(neigh) // add neighMap
        .leftOuterJoin(minPrio) // add minNbrPrio
        .flatMap { case (vid, (((cid, prio), neigh), minOpt)) =>
          val eligible = minOpt.forall(prio < _)
          if (!eligible) Nil
          else {
            val a = neigh.getOrElse(cid, 0)
            val opts = neigh - cid                                   // neighbours in *other* clusters
            val nC  = bcSize.value.getOrElse(cid, 0L)               // size of current cluster

            // --- best move is either to some neighbour cluster OR to a new singleton cluster
            var bestCid   = vid        // opening a fresh cluster → use own id
            var bestDelta = 1 - nC + 2*a    // delta for opening new cluster (nD = 0, b = 0)

            if (opts.nonEmpty) {
              val (otherCid, b) = opts.maxBy(_._2)                  // b = #pos‑edges to that cluster
              val nD = bcSize.value.getOrElse(otherCid, 0L)
              val delta = nD - nC + 1 + 2*(a - b)
              if (delta < bestDelta) { bestCid = otherCid ; bestDelta = delta }
            }

            if (bestDelta < 0) Some((vid, bestCid)) else Nil
          }
        }

      // 6) apply moves
      val newVerts = g.vertices.leftOuterJoin(candidates).mapValues {
        case (cid, Some(newCid)) => newCid
        case (cid, None)         => cid
      }

      val changed = g.vertices.join(newVerts)
                      .filter{case (_, (o,n)) => o!=n}.count()

      g = Graph(newVerts, g.edges)          // edges already cached
      neigh.unpersist(false)

      println(s"[LS]   moved vertices: $changed")
      if (changed == 0) return g
    }
    g
  }


  def localSearchOnSubgraph(
      subgraph: Graph[Long, Int],         // vertexId -> clusterId (all same to start)
      sc: SparkContext,
      clusterId: Long,
      numIter: Int = 10
  ): Graph[Long, Int] = {

    var g = Graph(subgraph.vertices, subgraph.edges.cache()) // keep edges cached

    for (iter <- 1 to numIter) {
      println(s"[LS-$clusterId] iter $iter   cost = ${signedCost(g)}")

      val bcSize = sc.broadcast(
        g.vertices.map { case (_, cid) => (cid, 1L) }
                  .reduceByKey(_ + _)
                  .collectAsMap()
      )

      val neigh = g.aggregateMessages[Map[Long, Int]](
        ctx => {
          ctx.sendToSrc(Map(ctx.dstAttr -> 1))
          ctx.sendToDst(Map(ctx.srcAttr -> 1))
        },
        (m1, m2) => (m1.keySet ++ m2.keySet)
          .map(k => k -> (m1.getOrElse(k, 0) + m2.getOrElse(k, 0)))
          .toMap
      ).persist(StorageLevel.MEMORY_AND_DISK_SER)

      val gWithPrio = g.mapVertices { case (_, cid) =>
        (cid, scala.util.Random.nextDouble())
      }

      val minPrio = gWithPrio.aggregateMessages[Double](
        t => {
          val (_, pSrc) = t.srcAttr ; val (_, pDst) = t.dstAttr
          t.sendToSrc(pDst) ; t.sendToDst(pSrc)
        },
        math.min
      )

      val candidates = gWithPrio.vertices
        .join(neigh)
        .leftOuterJoin(minPrio)
        .flatMap { case (vid, (((cid, prio), neigh), minOpt)) =>
          val eligible = minOpt.forall(prio < _)
          if (!eligible) Nil
          else {
            val a = neigh.getOrElse(cid, 0)
            val opts = neigh - cid
            val nC = bcSize.value.getOrElse(cid, 0L)

            var bestCid = vid                  // open new singleton cluster
            var bestDelta = 1 - nC + 2 * a     // default delta

            if (opts.nonEmpty) {
              val (otherCid, b) = opts.maxBy(_._2)
              val nD = bcSize.value.getOrElse(otherCid, 0L)
              val delta = nD - nC + 1 + 2 * (a - b)
              if (delta < bestDelta) {
                bestCid = otherCid
                bestDelta = delta
              }
            }

            if (bestDelta < 0) Some((vid, bestCid)) else None
          }
        }

      val newVerts = g.vertices.leftOuterJoin(candidates).mapValues {
        case (cid, Some(newCid)) => newCid
        case (cid, None)         => cid
      }

      val changed = g.vertices.join(newVerts)
        .filter { case (_, (oldCid, newCid)) => oldCid != newCid }
        .count()

      g = Graph(newVerts, g.edges)
      neigh.unpersist(false)

      println(s"[LS-$clusterId]   moved vertices: $changed")
      if (changed == 0) return g
    }

    g
  }


  def innerLocalSearch(
      graph: Graph[Long, Int],                       // (vid -> clusterId)
      sc: SparkContext,
      numIter: Int = 10
  ): Graph[Long, Int] = {

    // Group vertices by cluster ID
    val clusterToVertices: Map[Long, Set[VertexId]] =
      graph.vertices
           .map { case (vid, cid) => (cid, Set(vid)) }
           .reduceByKey(_ ++ _)
           .collect()
           .toMap

    // Initialize updatedVertices with the original graph's vertices
    var updatedVertices: RDD[(VertexId, Long)] = graph.vertices

    for ((clusterId, vertexSet) <- clusterToVertices) {

      val vertexSetBC = sc.broadcast(vertexSet)

      // Create subgraph induced by this cluster
      val subgraph = graph.subgraph(
        epred = et =>
          vertexSetBC.value.contains(et.srcId) &&
          vertexSetBC.value.contains(et.dstId),
        vpred = (vid, _) => vertexSetBC.value.contains(vid)
      ).cache()

      // Run local search loop on the subgraph
      val refinedSubgraph = localSearchOnSubgraph(subgraph, sc, clusterId, numIter)

      // Update refined cluster assignments
      val updatedCluster = refinedSubgraph.vertices.mapValues(_ => clusterId)

      // Merge back into overall vertex assignment
      val updatedMap = updatedVertices.leftOuterJoin(updatedCluster).mapValues {
        case (origCid, Some(newCid)) => newCid
        case (origCid, None)         => origCid
      }

      updatedVertices = updatedMap

      vertexSetBC.unpersist()
      subgraph.unpersist(blocking = false)
    }

    Graph(updatedVertices, graph.edges)  // Use original edges (assumed cached)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 4 || args.length > 5) {
      System.err.println(
        "Usage: final_project pivot={original,random} <input.csv> <output.csv> <n_LS_iters> [ls={regular,inner}]")
      System.exit(1)
    }

    val pivotAlg   = args(0)                   // original | random
    val inputPath  = args(1)
    val outputPath = args(2)
    val nLsIter    = args(3).toInt
    val lsMode     = if (args.length == 5) args(4) else "regular" // default

    val conf  = if (pivotAlg == "random")
                  new SparkConf().setAppName("ParallelizedPivotClustering")
                else
                  new SparkConf().setAppName("OriginalParallelizedPivotClustering")
    val sc    = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    // ---------------------------------------------------------------- pivot
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

    println(s"=== Starting $pivotAlg Parallel Pivot ===")
    val pivoted =
      if (pivotAlg == "random")   parallelPivotClustering(graph)
      else                        parallelPivotOriginal(graph)

    // -------------------------------------------------------------- LS phase
    val clustered =
      if (lsMode == "inner") {
        println("=== Starting InnerLocalSearch ===")
        innerLocalSearch(pivoted, sc, nLsIter)
      } else {
        println("=== Starting LocalSearch ===")
        localSearch(pivoted, sc, nLsIter)
      }

    // ---------------------------------------------------------------- stats
    val vertices = clustered.vertices.count()
    println()
    println("==========================================================")
    println(s"Number of vertices: $vertices")
    println(s"Signed‑cost after LS: ${signedCost(clustered)}")
    println("==========================================================")
    println()

    spark.createDataFrame(clustered.vertices)
         .coalesce(1)
         .write.format("csv")
         .mode("overwrite")
         .save(outputPath)

    println(s"=== Done!  Clusters written to $outputPath ===")
    sc.stop()
  }
}
