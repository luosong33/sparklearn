import org.apache.spark.{SparkConf, SparkContext}


object WordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("WordCount_")
    val sc = new SparkContext(conf)

    sc.textFile("/home/luosong/xxx.txt")
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)  // 关键代码三行
      .sortBy(_._2, false)
      .repartition(1)
      .saveAsTextFile("/home/luosong/out")

  }

}
