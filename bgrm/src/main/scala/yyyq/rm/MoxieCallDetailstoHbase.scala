package yyyq.rm

import java.util.ArrayList

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import yyyq.util.DateUtil

object MoxieCallDetailstoHbase {
  def main(args: Array[String]) {
    var masterUrl = "local[5]" //  local[8] yarn
    if (args.length > 0) {
      masterUrl = args(0)
    }
    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("moxie_basic_call_detail_to_hbase_his")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "4")
    conf.set("spark.streaming.backpressure.enabled", "true")
    //第一参数为SparkConf对象，第二个参数为批次时间
    val ssc = new StreamingContext(conf, Seconds(6))
    val topics = Set("MOXIE_CARRIER_ACCESS_SUMMIT_HISTORY") // _NEW
    val brokers = "192.168.15.195:9092,192.168.15.196:9092,192.168.15.197:9092,192.168.15.198:9092,192.168.15.199:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "moxie_basic_call_detail_to_hbase_his",
      "fetch.message.max.bytes" -> "134217728")

    // Create a direct stream
    //    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    var events = kafkaStream.map(line => {
      var putList = new ArrayList[Put]()
      try{
        var moxieLog = JSONObject.fromObject(line._2)
        var clientNo = moxieLog.getString("clientNo")
        var creditNo = moxieLog.getString("creditNo")
        var origDate = moxieLog.getString("origDate")
        var mobile = moxieLog.getString("mobile")
        var calls = moxieLog.getJSONArray("calls")
        for (i <- 0 until calls.size()) {
          var call = calls.getJSONObject(i); // 遍历 jsonarray 数组，把每一个对象转成 json 对象
          var items = call.getJSONArray("items")
          for (j <- 0 until items.size()) {
            var item = items.getJSONObject(j)
            var details_id = item.getString("details_id");
            var location = item.getString("location");
            var location_type = item.getString("location_type")
            var fee = item.getString("fee")
            var time = item.getString("time")
            var duration = item.getString("duration");
            var dial_type = item.getString("dial_type");
            var peer_number = item.getString("peer_number");
            var rowKey = origDate.substring(0,10).replace("-", "")+"|"+creditNo + "_" + mobile + "_" + peer_number + "_" + time;
            var nowDate =  DateUtil.nowString();
            var put = new Put(rowKey.getBytes());
            put.add(Bytes.toBytes("c"), Bytes.toBytes("time"), Bytes.toBytes(time));
            put.add(Bytes.toBytes("c"), Bytes.toBytes("location"), Bytes.toBytes(location));
            put.add(Bytes.toBytes("c"), Bytes.toBytes("fee"), Bytes.toBytes(fee));
            put.add(Bytes.toBytes("c"), Bytes.toBytes("details_id"), Bytes.toBytes(details_id));
            put.add(Bytes.toBytes("c"), Bytes.toBytes("peer_number"), Bytes.toBytes(peer_number));
            put.add(Bytes.toBytes("c"), Bytes.toBytes("location_type"), Bytes.toBytes(location_type));
            put.add(Bytes.toBytes("c"), Bytes.toBytes("duration"), Bytes.toBytes(duration));
            put.add(Bytes.toBytes("c"), Bytes.toBytes("dial_type"), Bytes.toBytes(dial_type));
            put.add(Bytes.toBytes("c"), Bytes.toBytes("mobile"), Bytes.toBytes(mobile));
            put.add(Bytes.toBytes("c"), Bytes.toBytes("voucherNo"), Bytes.toBytes(creditNo));
            put.add(Bytes.toBytes("c"), Bytes.toBytes("clientNo"), Bytes.toBytes(clientNo));
            put.add(Bytes.toBytes("c"), Bytes.toBytes("creditDate"), Bytes.toBytes(origDate));
            put.add(Bytes.toBytes("c"), Bytes.toBytes("nowDate"), Bytes.toBytes(nowDate));
            putList.add(put)
          }
        }
      }catch {
        case e: Exception => println()
      }
      putList
    })
    events.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val hconf = HBaseConfiguration.create()
        hconf.set("hbase.zookeeper.quorum", "192.168.15.195,192.168.15.196,192.168.15.197,192.168.15.198,192.168.15.199")
        hconf.set("hbase.zookeeper.property.clientPort", "2181")
        val table = new HTable(hconf, "moxie_basic_call_record_details");
        partitionOfRecords.foreach(pair => {
          table.put(pair)
        })
        table.close()
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}