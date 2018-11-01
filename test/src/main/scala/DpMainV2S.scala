import org.apache.spark.SparkConf

object DpMainV2S {

  def main(args: Array[String]): Unit = {
    var masterUrl = "yarn"

    val conf = new SparkConf()
      .setMaster(masterUrl)
      .setAppName("DpMainV2S")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "5")
    conf.set("spark.streaming.backpressure.enabled", "true")
    val ssc = new StreamingContext(conf, Seconds(5))

    val topics = Set("MOXIE_CARRIER_ACCESS_SUMMIT")
    val brokers = "172.16.30.30:9092,172.16.30.12:9092,172.16.30.9:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "DpMainV2S",
      "fetch.message.max.bytes" -> "134217728" // 52428800 134217728
    )

    val kafkaStream = KafkaUtils.createDirectStream[String, String,
      StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val events = kafkaStream.map(line => {
      try {
        // 构建parquet文件
        
//        。。。
      } catch {
        case e: Exception => println(e)
      }
      new Tuple2()
    })

    events.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
//        写入hdfs
      })
    })

    ssc.start()  //  开启接收模式
    ssc.awaitTermination()
  }

}
