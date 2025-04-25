package final_project

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}
import scala.util.Random

object main {
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)
  def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
    var g = g_in.mapVertices((id,attr)=>0) // for if in MIS or not
    //var activeGraph = g_in.mapVertices((id,attr)=>true) // for if active or not
    //var n_active = activeGraph.vertices.filter(_._2 == true).count()
    var n_remaining_vertices = g.vertices.filter(_._2 == 0).count()
    var iterations = 0
    val startTime = System.currentTimeMillis()
    while (n_remaining_vertices > 0) {
      val iterStart = System.currentTimeMillis()
      iterations += 1

      val randomGraph = g.mapVertices { case (id, attr) =>
        if (attr == 0) Random.nextDouble() else -1.0
      }

      val neighborMax = randomGraph.aggregateMessages[Boolean](
        triplet => {
          if (triplet.srcAttr >= 0 && triplet.dstAttr >= 0) {
            if (triplet.srcAttr > triplet.dstAttr){
              triplet.sendToSrc(true)
              triplet.sendToDst(false)
            }
            if (triplet.dstAttr > triplet.srcAttr) {
              triplet.sendToDst(true)
              triplet.sendToSrc(false)
            }
          }
        }, (a,b) => a && b
      )

      // Add to MIS
      g = g.outerJoinVertices(neighborMax) {
        case (id, attr,Some(msg)) =>
          // add to MIS if largest vertex
          if (attr == 0 && msg) 1
          else attr
        case (id, attr, None) =>
          // case of isolated vertices
          if (attr == 0) 1
          else attr
      }

      // update neighbors
      val neighbors = g.aggregateMessages[Int] (
        triplet => {
          if (triplet.srcAttr == 1) triplet.sendToDst(-1)
          if (triplet.dstAttr == 1) triplet.sendToSrc(-1)
        },
        (a,b) => -1
      )

      g = g.outerJoinVertices(neighbors) {
        case (id, attr, Some(-1)) => -1
        case (id, attr, _) => attr
      }

      n_remaining_vertices = g.vertices.filter(_._2 == 0).count()
      val endTime = System.currentTimeMillis()
      val durationSeconds = (endTime - iterStart) / 1000
      System.out.println(s"Iteration: $iterations, Time: $durationSeconds, Active Vertices: $n_remaining_vertices")
    }
    //(g)


    val totalTime = (System.currentTimeMillis() - startTime) / 1000.0
    val isValid = verifyMIS(g)
    System.out.println(s"Total Iterations: $iterations, Time: $totalTime, isValid: $isValid")
    g
  }
  def verifyMIS(g_in: Graph[Int, Int]): Boolean = {
    // Checks if any adjacent vertices are in the MIS
    val adjacencyCheck = g_in.triplets.filter(triplet => triplet.srcAttr == 1 && triplet.dstAttr == 1).count() > 0
    if (adjacencyCheck) {
      return false
    }

    // Checks if for every vertex, it has at least one vertex in its neightborhood in the MIS
    val neighborMIS = g_in.aggregateMessages[Int](
      triplet => {
        if (triplet.srcAttr == 1) triplet.sendToDst(1)
        if (triplet.dstAttr == 1) triplet.sendToSrc(1)
      }, (a,b) => a + b
    )

    val nonMaximal = g_in.vertices.leftJoin(neighborMIS) {
        case (_, label, neighborCount) =>
          label == -1 && neighborCount.getOrElse(0) == 0
      }.filter { case (_, isNonMaximal) => isNonMaximal}
      .count() > 0

    !nonMaximal
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("final_project")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
    /* You can either use sc or spark */

    if(args.length == 0) {
      println("Usage: final_project input_graph_path output_graph_path")
      sys.exit(1)
    }
    val startTimeMillis = System.currentTimeMillis()
    val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
    val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
    val g2 = LubyMIS(g)

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
    println("==================================")
    println("Luby's algorithm completed in " + durationSeconds + "s.")
    println("==================================")

    val g2df = spark.createDataFrame(g2.vertices)
    g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
  }
}
