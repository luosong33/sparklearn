import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

object ComprehensiveExample {

<<<<<<< HEAD
  /* 官方最后一个案例 */
=======
>>>>>>> 2de32432abfd99b06a1e1057b7f0f615f14e319d
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
<<<<<<< HEAD
      .master("local[*]")
=======
>>>>>>> 2de32432abfd99b06a1e1057b7f0f615f14e319d
      .getOrCreate()
    val sc = spark.sparkContext

    // $example on$
    // Load my user data and parse into tuples of user id and attribute list
//    val users = (sc.textFile("data/users.txt")
<<<<<<< HEAD
    val users = (sc.textFile("D:\\workspace\\sparklearn\\data\\users.txt")
=======
    val users = (sc.textFile("file:///home/luosong/workspace/spark-dataanalysis/graphx/data/users.txt")
>>>>>>> 2de32432abfd99b06a1e1057b7f0f615f14e319d
      .map(line => line.split(",")).map( parts => (parts.head.toLong, parts.tail) ))

    // Parse the edge data which is already in userId -> userId format
//    val followerGraph = GraphLoader.edgeListFile(sc, "data/graphx/followers.txt")
<<<<<<< HEAD
    val followerGraph = GraphLoader.edgeListFile(sc, "D:\\workspace\\sparklearn\\data\\followers.txt")
=======
    val followerGraph = GraphLoader.edgeListFile(sc, "file:///home/luosong/workspace/spark-dataanalysis/graphx/data/graphx/followers.txt")
>>>>>>> 2de32432abfd99b06a1e1057b7f0f615f14e319d

    // Attach the user attributes
    val graph = followerGraph.outerJoinVertices(users) {
      case (uid, deg, Some(attrList)) => attrList
      // Some users may not have attributes so we set them as empty
      case (uid, deg, None) => Array.empty[String]
    }

    // Restrict the graph to users with usernames and names
    val subgraph = graph.subgraph(vpred = (vid, attr) => attr.size == 2)

    // Compute the PageRank
    val pagerankGraph = subgraph.pageRank(0.001)

    // Get the attributes of the top pagerank users
    val userInfoWithPageRank = subgraph.outerJoinVertices(pagerankGraph.vertices) {
      case (uid, attrList, Some(pr)) => (pr, attrList.toList)
      case (uid, attrList, None) => (0.0, attrList.toList)
    }

<<<<<<< HEAD
    println("------------------------start------------------------")
    println(userInfoWithPageRank.vertices.top(5)(Ordering.by(_._2._1)).mkString("\n"))
    println("------------------------end------------------------")
=======
    println(userInfoWithPageRank.vertices.top(5)(Ordering.by(_._2._1)).mkString("\n"))
>>>>>>> 2de32432abfd99b06a1e1057b7f0f615f14e319d
    // $example off$

    spark.stop()
  }
}
// scalastyle:on println
