package yyyq.test

object TestScala {

  def main(args: Array[String]): Unit = {
    var streamingMap = scala.collection.mutable.Map[String, String]()  //  scala.collection.mutable.HashMap
    streamingMap+=("k3"->"3")
    streamingMap+=("k4"->"4")
    streamingMap+=("k5"->"5")
    streamingMap.foreach(m => println(m._1+":::"+m._2))

  }

}
