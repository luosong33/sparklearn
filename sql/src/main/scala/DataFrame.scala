import org.apache.spark.sql.SparkSession

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level, Logger}

/**
  * Created by genie on 2018/10/28.
  */
object DataFrame {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("Df")
      .getOrCreate()

    val json1:String = """{"a":null, "b": 23.1, "c": 1}"""
    val json2:String = """{"a":null, "b": "hello", "d": 1.2}"""
    println("json1:::" + json1)
    val seq: Seq[Char] = json1
    val input = Seq(json1, json2)
//    seq.foreach(x => println(x))

    import spark.implicits._
//    val ds = spark.createDataset(Seq(json1))
//    ds.show()
//    val df = spark.read.json(ds)
//    df.show
//    df.printSchema

    val sqlContext = spark.sqlContext
    val sc = spark.sparkContext
    import sqlContext.implicits._
    val value = sc.parallelize(input)
    println("over。。。")

    val name = "yourName"
    val age  = 12
    //字符串中$闭包自由变值
    var jsonStr = """{"userId":"1000000111000","event":"App_Start","time":"1490767690649","ip":"125.122.211.0","deviceId":"A249B296-F61C-4F6A-91B9-8AED70745B00","appVersion":"4.3.0","manufacturer":"Apple","model":"iPhone10,3","os":"iOS","osVersion":"11.4.1","screenHeight":"812","screenWidth":"375","wifi":"true","carrier":"中国电信","networkType":"WIFI","properties":"{}","day":"NULL","month":"10","year":"2018","traceId":"201810","screenName":"45寸"}"""
    val tesRDD = sc.makeRDD(jsonStr.stripMargin:: Nil)  // jsonStr转rdd
//    val tesRDD2DF = sqlContext.read.json(tesRDD)
    val tesRDD2DF = spark.read.json(tesRDD)
    tesRDD2DF.show()

    sc.stop()
  }


}
