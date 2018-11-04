import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/* 官方文档小例子 */
object OrgDome {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("OrgDome")
    val sc = new SparkContext(conf)
    /*val followerGraph = GraphLoader
      .edgeListFile(sc, "D:\\workspace\\spark-master\\examples\\data\\graphx\\followers.txt")*/
    constructsAgraph(sc)
    println("end")
  }

  def constructsAgraph(sc: SparkContext): Unit = {
    // Create an RDD for the vertices
    val users: RDD[(VertexId, (String, String))] =
      sc.parallelize(
        Array(
          (3L, ("rxin", "student")),
          (7L, ("jgonzal", "postdoc")),
          (5L, ("franklin", "prof")),
          (2L, ("istoica", "prof"))
        )
      )
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(
        Array(
          Edge(3L, 7L, "collab"),
          Edge(5L, 3L, "advisor"),
          Edge(2L, 5L, "colleague"),
          Edge(5L, 7L, "pi")
        )
      )
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
    // Count all users which are postdocs
    println(graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc" }.count)
    // Count all the edges where src > dst
    println(graph.edges.filter(e => e.srcId > e.dstId).count)
    println(graph.edges.filter { case Edge(src, dst, prop) => src > dst }.count)
    println("end constructsAgraph")
  }

}
