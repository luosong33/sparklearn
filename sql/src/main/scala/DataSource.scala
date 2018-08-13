import org.apache.spark.sql.SparkSession


object DataSource {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()

    read_write_parquet(spark)

    spark.stop()

  }

  private def read_write_parquet(spark: SparkSession) = {
    val employee = spark.read.json("D:\\workspace\\sparklearn\\data\\employees.json")
    employee.show()
    employee.write.parquet("D:\\workspace\\sparklearn\\data\\employee.parquet")
  }
}
