package yyyq.etl

import org.apache.spark.sql.SparkSession

object HbaseTaobaoBasic4Order {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("HbaseTaobaoBasic4Order")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
  }

}
