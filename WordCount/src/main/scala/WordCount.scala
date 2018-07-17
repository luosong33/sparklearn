import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    val file_path_in = "file:///home/luosong/workspace/sparklearn/WordCount/data/users.txt"
    val file_path_out = "file:///home/luosong/workspace/sparklearn/WordCount/data/out"
//    var file_path_out = "hdfs://nn1:8020/bigdata/ls/wc/out"

    sc.textFile(file_path_in)
      .flatMap(_.split(" ")).map((_, 1))
      .reduceByKey(_ + _, 1).sortBy(_._2, false)
      .saveAsTextFile(file_path_out)

  }

}
