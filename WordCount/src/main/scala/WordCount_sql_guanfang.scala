import org.apache.spark.sql.SparkSession

object WordCount_sql_guanfang {

  // 官方用例
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    sc.textFile("/home/luosong/xxx.txt")
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _, 1)
      .sortBy(_._2, false)
      .saveAsTextFile("/home/luosong/out")

  }

}
