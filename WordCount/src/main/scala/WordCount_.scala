import org.apache.spark.{SparkConf, SparkContext}


object WordCount_ {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("WordCount_")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("/home/luosong/xxx.txt")
    val counts = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).
      reduceByKey(_ + _).repartition(1)
    counts.saveAsTextFile("/home/luosong/out")

  }

}
