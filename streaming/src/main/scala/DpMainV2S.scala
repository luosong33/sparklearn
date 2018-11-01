import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level, Logger}
import com.anve.datacenter.api.dp.DpEntryV2
import net.sf.json.JSONObject
import java.util

import org.apache.spark.rdd.RDD

import scala.collection.mutable


object DpMainV2S {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark.sql").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    var masterUrl = "local[2]" // yarn
    //    streaming方式
    val conf = new SparkConf()
      .setMaster(masterUrl)
      .setAppName("DpMainV2S")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "5")
    conf.set("spark.streaming.backpressure.enabled", "true")
    val ssc = new StreamingContext(conf, Seconds(5))


    val topics = Set("scalaSpark2kafka")
    //    val brokers = "172.20.10.54:9092,172.20.10.55:9092,172.20.10.56:9092"
    val brokers = "127.0.0.1:9092" // 本地
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      //      "serializer.class" -> "io.netty.handler.codec.bytes.DefaultDecoder",
      //      "fetch.message.max.bytes" -> "134217728", // 52428800 134217728
      "group.id" -> "DpMainV2S"
    )

    val messages = KafkaUtils.createDirectStream[String, String,
      StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    //    val messages = KafkaUtils.createDirectStream[Array[Byte], Array[Byte],
    //      DefaultDecoder, DefaultDecoder](ssc, kafkaParams, topics)
    System.out.println("。。。messages...")

    //  解压反序列化
    val events = messages.map(line => {
      //      var bytes: Array[Byte] = line._2
      //      bytes = GzipUtils.ungzip(bytes)
      //      val lists: util.List[DpEntryV2] = SeriaPojoUtil.deserializeList(bytes, classOf[DpEntryV2])
      val dataLog = line._2
      System.out.println("。。。events..." + dataLog)
      Tuple1(dataLog)
    })

    //  转df写入hdfs
    events.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val spark = SparkSession
          .builder
          //          .master("local[2]")
          //          .appName("DpMainV2S")
          .getOrCreate()
        val sc = spark.sparkContext
        val sqlContext = spark.sqlContext
        import spark.implicits._

        partitionOfRecords.foreach(pair => {
          /*val lists: util.List[DpEntryV2] = pair._1
          val slist = List[DpEntryV2]()
          import scala.collection.JavaConversions._
//          lists.forEach(dp => slist.::(dp))
          for (dp <- lists){
            slist.+:(dp)
          }*/

          //          异常org.apache.spark.SparkException: Task not serializable。解决：将spark、sqlContext、sc放在第二层创建
          val jsonStr = pair._1
          //          val jsonStr = """d"""
          val jstrRdd = sc.makeRDD(jsonStr.stripMargin :: Nil) // val ds = spark.createDataset(Seq(jsonStr))
          val df = sqlContext.read.json(jstrRdd)  // val df = jstrRdd.toDF() value 只有一行
          sqlContext

          //          val tesRDD = sc.parallelize(seq :: Nil)
          //          val tesRDD = sc.makeRDD(slist :: Nil)
          //          val df = sqlContext.createDataFrame(tesRDD)
          //          val df = tesRDD.toDF()
          //          val df = spark.createDataFrame(tesRDD)

          df.show()
          //          df.write.format("parquet")
          //            .option("checkpointLocation", "hdfs://i-b86ybfg5:8020/test/asong")
          //            .option("startingOffsets", "earliest")
          //            .option("path", "/data/dataplatform_pageview")
          //            .partitionBy("year", "month", "day")
          //          df.write.parquet("hdfs://i-b86ybfg5:8020/test/asong/employee1.parquet")
          //          df.write.parquet("/Users/gainie/Downloads/sparkfile/test1")  // 本地
          println("Over..." + jsonStr)
        })
      })
    })

    ssc.start() //  开启接收模式
    ssc.awaitTermination()
  }

}
