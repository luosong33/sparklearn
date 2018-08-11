package yyyq.rm

import java.util.HashSet

import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import redis.clients.jedis.Jedis
import yyyq.util.PhoneUtils

object CalCallCountFeatuerStatisticsTransfer {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("calCallCountStatistics_local_his_20180305").setMaster("local[5]")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "4")
    val ssc = new StreamingContext(conf, Seconds(6))
    //_HISTORY
    val topics = Set("MOXIE_CARRIER_ACCESS_SUMMIT_HISTORY")  //  MOXIE_CARRIER_ACCESS_SUMMIT  MOXIE_CARRIER_ACCESS_SUMMIT_HISTORY
    val brokers = "192.168.15.195:9092,192.168.15.196:9092,192.168.15.197:9092,192.168.15.198:9092,192.168.15.199:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      /*"auto.offset.reset" -> "smallest",*/
      "group.id" -> "calCallCountStatistics_local_his_20180305",
      "fetch.message.max.bytes" -> "134217728")
    // Create a direct stream
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    kafkaStream.foreachRDD(rdd => {
      rdd.foreachPartition { partitionRecords =>
        {
          val hconf = HBaseConfiguration.create()
          hconf.set("hbase.zookeeper.quorum", "192.168.15.195,192.168.15.196,192.168.15.197,192.168.15.198,192.168.15.199")
          hconf.set("hbase.zookeeper.property.clientPort", "2181")
          val table = new HTable(hconf, "contact_person_call_count_featuer_result");
          partitionRecords.foreach(pair => {
            try {
              var isHasCall = 0
              var moxieLog = JSONObject.fromObject(pair._2)
              //获取客户号
              var clientNo = moxieLog.getString("clientNo")
              var emergencyContactInstance = RequestMobileBroadcast.getEmergencyContactMobileSet(clientNo)
              //获取授信号
              var creditNo = moxieLog.getString("creditNo")
              //通话详单jsonArray
              var calls = moxieLog.getJSONArray("calls")
              var peerNumberString = "";
              var phoneSet = new HashSet[String]();
              for (i <- 0 until calls.size()) {
                //得到一个通话详单call
                var call = calls.getJSONObject(i); // 遍历 jsonarray 数组，把每一个对象转成 json 对象
                //得到通话详单里面的item jsonArray
                var items = call.getJSONArray("items")
                //遍历items JsonArray
                for (j <- 0 until items.size()) {
                  //得到一个item
                  var item = items.getJSONObject(j)
                  //取出通话人
                  var peer_number = item.getString("peer_number");
                  if (PhoneUtils.isPhone(peer_number)) {
                    phoneSet.add(peer_number)
                  }
                  if (emergencyContactInstance.size() > 0 && emergencyContactInstance.contains(peer_number)) {
                    isHasCall = isHasCall + 1
                  }
                }
              }
              peerNumberString = PhoneUtils.setToString(phoneSet)
              var requestPersonCount = 0
              var phoneString = new StringBuilder
              var resultString = ""
              if (!"".equals(peerNumberString)) {
                var requestMobileInstance = RequestMobileBroadcast.getReqMobileSet(peerNumberString)
                requestPersonCount = requestMobileInstance.size()
                var a = requestMobileInstance.toArray();
                for (item <- requestMobileInstance.toArray()) phoneString ++= (item.toString() + ",")
                if (phoneString.toString().length() > 0&&phoneString.toString().length()<256) {
                  resultString = phoneString.toString().substring(0, phoneString.toString().length() - 1)
                }
                if (phoneString.toString().length()>255) {
                  resultString = phoneString.toString().substring(0, 255)
                }
              }
              /*var result = "{'userId':'" + clientNo +
                "','callReocodeMatchedYyUser':'" + requestPersonCount +
                "','ConPersonCallCountFeature':'" + resultString +
                "','type':'YINGYING_CARRIER_HIT_YY_DATA" +
                "','callReocodeMatchedEmergencyContact':'" + isHasCall + "'}"
              HTTPUtils.httpPostWithJson(JSONObject.fromObject(result), "http://riskdata-pool/dataDirect/operator");*/
              var put = new Put(Bytes.toBytes(clientNo + "|" + creditNo))
              put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("requestPersonCount"), Bytes.toBytes(String.valueOf(requestPersonCount)))
              put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("emergencyContactCallCount"), Bytes.toBytes(String.valueOf(isHasCall)))
              put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("clientNo"), Bytes.toBytes(clientNo))
              put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("creditNo"), Bytes.toBytes(creditNo))
              put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("ConPersonCallCountFeature"), Bytes.toBytes(resultString))
              table.put(put)
              val jedis = new Jedis("192.168.2.211", 8000);
              jedis.auth("yylc8888");
              jedis.set(clientNo + "|" + creditNo + "|" + "YINGYING_CARRIER_HIT_YY_DATA", "")
              jedis.expire(clientNo + "|" + creditNo + "|" + "YINGYING_CARRIER_HIT_YY_DATA", 60 * 60 * 24 * 7)
              jedis.close()
            } catch {
              case ex: Exception => ex.printStackTrace()

            }
          })
          table.close()
        }
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}