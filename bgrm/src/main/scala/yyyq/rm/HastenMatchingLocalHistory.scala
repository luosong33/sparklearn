package yyyq.rm

import java.sql.{DriverManager, ResultSet}

import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import yyyq.util.DateUtil

import scala.collection.JavaConversions._
import scala.collection.immutable.{ListMap, TreeMap}
import scala.collection.mutable.ArrayBuffer

object HastenMatchingLocalHistory {

  def main(args: Array[String]): Unit = {
    var masterUrl = "local[10]" //  local[20]  yarn
    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("HastenMatching_node4_History_0322") // HastenMatching_date_upgrade HastenMatching_date
    conf.set("spark.streaming.kafka.maxRatePerPartition", "5")
    conf.set("spark.streaming.backpressure.enabled", "true")
    val ssc = new StreamingContext(conf, Seconds(5))

    // Kafka configurations
    val topics = Set("MOXIE_CARRIER_ACCESS_SUMMIT_HISTORY") //  _HISTORY
    val brokers = "192.168.15.195:9092,192.168.15.196:9092,192.168.15.197:9092,192.168.15.198:9092,192.168.15.199:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "HastenMatching_node4_History_0322",
      "fetch.message.max.bytes" -> "134217728" // 52428800 134217728
    )

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val events = kafkaStream.map(line => {
      var clientNo = ""
      var creditNo = ""
      var phoneNum_10 = 0 // 个数
      var callNum_10 = 0 // 次数
      var phoneNum_20 = 0
      var callNum_20 = 0
      var phoneNum_30 = 0
      var callNum_30 = 0
      var phoneNum_90 = 0
      var callNum_90 = 0
      var phoneNum_180 = 0
      var callNum_180 = 0
      var origDate = ""
      var nowDate = ""
      var proportion30 = 0.00
      var top10Num = 0
      var phoneNum_90_fortyThousand = 0
      var phoneNum_180_fortyThousand = 0
      try {

        val pbset = conn() //  加载催收库催收机构号码  确认的三千多
        val pbset_ = conn_() //  加载催收库催收机构号码  四万
        var moxieLog = JSONObject.fromObject(line._2)
        //  解析数据
        val starttime = System.currentTimeMillis
        clientNo = moxieLog.getString("clientNo") // 获取客户号
        creditNo = moxieLog.getString("creditNo") // 获取授信号
        origDate = moxieLog.getString("origDate") // 获取原始时间
        var calls = moxieLog.getJSONArray("calls") // 通话详单jsonArray
        var streamingList = ArrayBuffer[String]() // 180天借款人通话记录
        var streamingMap = TreeMap[String, String]() //  180天借款人通话记录k,v

        //遍历通话详单jsonArray
        for (i <- 0 until calls.size()) {
          //得到一个通话详单call
          var call = calls.getJSONObject(i); // 遍历 jsonarray 数组，把每一个对象转成 json 对象
          var items = call.getJSONArray("items") // 得到通话详单里面的item jsonArray
          for (j <- 0 until items.size()) { // 遍历items JsonArray
            var item = items.getJSONObject(j) // 得到一个item
          var time = item.get("time").toString
            var peer_number = item.get("peer_number").toString()
            streamingList += peer_number
            streamingMap += (time -> peer_number)
          }
        }
        val lastString = moxieLog.getString("last_modify_time")  //  最新日期

        //  运营商top10撞中催收库个数
        var streamingMap_ = scala.collection.mutable.Map[String, Int]()
        streamingMap.foreach(m =>
          if (streamingMap_.contains(m._2)) {
            streamingMap_.put(m._2, streamingMap_.get(m._2).get + 1)
          } else {
            streamingMap_.put(m._2, 1)
          })
        var streamingMap10 = TreeMap[String, Int]()
        ListMap(streamingMap_.toSeq.sortWith(_._2 > _._2): _*).take(10).foreach(m => streamingMap10 += (m._1 -> m._2))
        for (i <- pbset_) {
          streamingMap10.foreach(x =>
            if (i.equals(x._1)) {
              top10Num += 1
            })
        }

        //  180天计算
        var frequency180Map = scala.collection.mutable.Map[String, Int]() //  借款人通话次数结果集合
        for (i <- pbset) {
          for (j <- streamingList) {
            if (i.equals(j)) {
              //  次数
              if (frequency180Map.contains(j)) {
                frequency180Map.put(j, frequency180Map.get(j).get + 1)
              } else {
                frequency180Map.put(j, 1)
                phoneNum_180 += 1 //  个数
              }
            }
          }
        }
        if (phoneNum_180 > 0) {
          frequency180Map.foreach(e => callNum_180 += e._2)
        }

        //  180模型4万
        var frequency180Map_4Thousand = scala.collection.mutable.Map[String, Int]() //  借款人通话次数结果集合
        for (i <- pbset_) {
          for (j <- streamingList) {
            if (i.equals(j)) {
              //  次数
              if (frequency180Map_4Thousand.contains(j)) {
                frequency180Map_4Thousand.put(j, frequency180Map_4Thousand.get(j).get + 1)
              } else {
                frequency180Map_4Thousand.put(j, 1)
                phoneNum_180_fortyThousand += 1 //  个数
              }
            }
          }
        }

        //  10天计算
        var streaming10List = ArrayBuffer[String]() // 10天借款人通话记录
        val strDate_10 = DateUtil.getFrontAfterDate(lastString, -9) //  10天前日期
        val strings_10 = DateUtil.getBetweenDates(strDate_10, lastString) //  10天日期
        //        var strings_10: util.List[String]  = scala.collection.mutable.ListBuffer[String]() = DateUtil.getBetweenDates(strDate_10,lastString)  //  10天日期
        for (x <- strings_10) { //  从总集合里模糊匹配出10天日期的联系号码
          streamingMap.foreach(y =>
            if (y._1.contains(x)) {
              streaming10List += y._2
            })
        }
        var frequency10Map = scala.collection.mutable.Map[String, Int]() //  10借款人通话次数结果集合
        //  匹配逻辑
        for (i <- pbset) {
          for (j <- streaming10List) {
            if (i.equals(j)) {
              //  次数
              if (frequency10Map.contains(j)) {
                frequency10Map.put(j, frequency10Map.get(j).get + 1)
              } else {
                frequency10Map.put(j, 1)
                phoneNum_10 += 1 //  个数
              }
            }
          }
        }
        if (phoneNum_10 > 0) {
          frequency10Map.foreach(e => callNum_10 += e._2)
        }

/*
        val endtime = System.currentTimeMillis
        System.out.println("===========================↓============================")
        System.out.println("===========================↓============================")
        System.out.println("===========================↓============================")
        System.out.println("===========================↓============================")
        System.out.println("===========================↓============================")
        System.out.println(DateUtil.nowString + " WordCount耗时为： " + (endtime - starttime) + "==" +clientNo + "|" + creditNo)
        System.out.println("===========================↑============================")
        System.out.println("===========================↑============================")
        System.out.println("===========================↑============================")
        System.out.println("===========================↑============================")
        System.out.println("===========================↑============================")*/

        //  20天计算
        var streaming20List = ArrayBuffer[String]() // 20天借款人通话记录
        val strDate_20 = DateUtil.getFrontAfterDate(lastString, -19)
        val strings_20 = DateUtil.getBetweenDates(strDate_20, lastString)
        for (x <- strings_20) {
          streamingMap.foreach(y =>
            if (y._1.contains(x)) {
              streaming20List += y._2
            })
        }
        var frequency20Map = scala.collection.mutable.Map[String, Int]() //  20天借款人通话次数结果集合
        //  匹配逻辑
        for (i <- pbset) {
          for (j <- streaming20List) {
            if (i.equals(j)) {
              //  次数
              if (frequency20Map.contains(j)) {
                frequency20Map.put(j, frequency20Map.get(j).get + 1)
              } else {
                frequency20Map.put(j, 1)
                phoneNum_20 += 1 //  个数
              }
            }
          }
        }
        if (phoneNum_20 > 0) {
          frequency20Map.foreach(e => callNum_20 += e._2)
        }

        //  30天计算
        var streaming30List = ArrayBuffer[String]() // 30天借款人通话记录
        val strDate_30 = DateUtil.getFrontAfterDate(lastString, -29)
        val strings_30 = DateUtil.getBetweenDates(strDate_30, lastString)
        for (x <- strings_30) {
          streamingMap.foreach(y =>
            if (y._1.contains(x)) {
              streaming30List += y._2
            })
        }
        var frequency30Map = scala.collection.mutable.Map[String, Int]() //  借款人通话次数结果集合
        //  匹配逻辑
        for (i <- pbset) {
          for (j <- streaming30List) {
            if (i.equals(j)) {
              //  次数
              if (frequency30Map.contains(j)) { //  存在值加1
                frequency30Map.put(j, frequency30Map.get(j).get + 1)
              } else {
                frequency30Map.put(j, 1)
                phoneNum_30 += 1 //  个数
              }
            }
          }
        }
        if (phoneNum_30 > 0) {
          frequency30Map.foreach(e => callNum_30 += e._2)
        }
        //  30天占比
        var frequency30Map_ = scala.collection.mutable.Map[String, Int]() //  借款人通话次数结果集合
        var phoneNum_30_ = 0
        for (i <- pbset_) {
          for (j <- streaming30List) {
            if (i.equals(j)) {
              //  次数
              if (frequency30Map_.contains(j)) { //  存在值加1
                frequency30Map_.put(j, frequency30Map_.get(j).get + 1)
              } else {
                frequency30Map_.put(j, 1)
                phoneNum_30_ += 1 //  个数
              }
            }
          }
        }
        val size = streaming30List.size
        proportion30 = phoneNum_30_.toDouble / size

        //  90天计算
        var streaming90List = ArrayBuffer[String]() // 10天借款人通话记录
        val strDate_90 = DateUtil.getFrontAfterDate(lastString, -89)
        val strings_90 = DateUtil.getBetweenDates(strDate_90, lastString)
        for (x <- strings_90) {
          streamingMap.foreach(y =>
            if (y._1.contains(x)) {
              streaming90List += y._2
            })
        }
        var frequency90Map = scala.collection.mutable.Map[String, Int]() //  借款人通话次数结果集合
        //  匹配逻辑
        for (i <- pbset) {
          for (j <- streaming90List) {
            if (i.equals(j)) {
              //  次数
              if (frequency90Map.contains(j)) {
                frequency90Map.put(j, frequency90Map.get(j).get + 1)
              } else {
                frequency90Map.put(j, 1)
                phoneNum_90 += 1 //  个数
              }
            }
          }
        }
        if (phoneNum_90 > 0) {
          frequency90Map.foreach(e => callNum_90 += e._2)
        }
        // 90天个数模型4万
        var frequency90Map_4Thousand = scala.collection.mutable.Map[String, Int]() //  借款人通话次数结果集合
        for (i <- pbset_) {
          for (j <- streaming90List) {
            if (i.equals(j)) {
              //  次数
              if (frequency90Map_4Thousand.contains(j)) { //  已经存在
                frequency90Map_4Thousand.put(j, frequency90Map_4Thousand.get(j).get + 1)
              } else {
                frequency90Map_4Thousand.put(j, 1) //  首次出现
                phoneNum_90_fortyThousand += 1 //  个数
              }
            }
          }
        }

        nowDate = DateUtil.nowString();
      } catch {
        case e: Exception => println(clientNo + e)
      }
      new Tuple2(clientNo + "|" + creditNo, phoneNum_10.toString + "|" + callNum_10.toString
        + "|" + phoneNum_20.toString + "|" + callNum_20.toString
        + "|" + phoneNum_30.toString + "|" + callNum_30.toString
        + "|" + phoneNum_90.toString + "|" + callNum_90.toString
        + "|" + phoneNum_180.toString + "|" + callNum_180.toString
        + "|" + origDate + "|" + nowDate
        + "|" + proportion30.formatted("%.4f")
        + "|" + top10Num
        + "|" + phoneNum_180_fortyThousand
        + "|" + phoneNum_90_fortyThousand
      )
    })

    events.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val hconf = HBaseConfiguration.create()
        hconf.set("hbase.zookeeper.quorum", "192.168.15.195,192.168.15.196,192.168.15.197,192.168.15.198,192.168.15.199")
        hconf.set("hbase.zookeeper.property.clientPort", "2181")
        val table = new HTable(hconf, "hasten_matching") //  hasten_matching_test
        val httpClient = HTTPUtils.getHttpClient
        partitionOfRecords.foreach(pair => {
          try {
            val strarr2 = pair._2.split("\\|")
            val put = new Put(Bytes.toBytes(pair._1))
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("phoneNum_10"), Bytes.toBytes(strarr2(0)))
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("callNum_10"), Bytes.toBytes(strarr2(1)))
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("phoneNum_20"), Bytes.toBytes(strarr2(2)))
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("callNum_20"), Bytes.toBytes(strarr2(3)))
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("phoneNum_30"), Bytes.toBytes(strarr2(4)))
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("callNum_30"), Bytes.toBytes(strarr2(5)))
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("phoneNum_90"), Bytes.toBytes(strarr2(6)))
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("callNum_90"), Bytes.toBytes(strarr2(7)))
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("phoneNum_180"), Bytes.toBytes(strarr2(8)))
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("callNum_180"), Bytes.toBytes(strarr2(9)))
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("origDate"), Bytes.toBytes(strarr2(10)))
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("nowDate"), Bytes.toBytes(strarr2(11)))
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("proportion30"), Bytes.toBytes(strarr2(12)))
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("top10Num"), Bytes.toBytes(strarr2(13)))
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("phoneNum_180_fortyThousand"), Bytes.toBytes(strarr2(14)))
            put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("phoneNum_90_fortyThousand"), Bytes.toBytes(strarr2(15)))
            table.put(put)

            /*val jSONObject = new JSONObject
            jSONObject.put("userId",pair._1.split("\\|")(0))
            jSONObject.put("matchedDanNumber10day",strarr2(0))
            jSONObject.put("matchedDanFrequency10day",strarr2(1))
            jSONObject.put("matchedDanNumber20day",strarr2(2))
            jSONObject.put("matchedDanFrequency20day",strarr2(3))
            jSONObject.put("matchedDanNumber30day",strarr2(4))
            jSONObject.put("matchedDanFrequency30day",strarr2(5))
            jSONObject.put("matchedDanNumber90day",strarr2(6))
            jSONObject.put("matchedDanFrequency90day",strarr2(7))
            jSONObject.put("matchedDanNumber180day",strarr2(8))
            jSONObject.put("matchedDanFrequency180day",strarr2(9))
            jSONObject.put("danNumberCallRate30",strarr2(12))
            jSONObject.put("callTopTenDanNumber",strarr2(13))
            jSONObject.put("call180FortyThousandDanNumber",strarr2(14))
            jSONObject.put("call90FortyThousandDanNumber",strarr2(15))
            jSONObject.put("type","YINGYING_MACHED_DUN_DATABASE")
            val str = jSONObject.toString
            HTTPUtils.httpPostWithJson(jSONObject, "http://riskdata-pool/dataDirect/operator", httpClient);*/

            val jedis = new Jedis("192.168.2.211", 8000)
            jedis.auth("yylc8888")
            jedis.set(pair._1 + "|" + "YINGYING_MACHED_DUN_DATABASE", "")
            jedis.expire(pair._1 + "|" + "YINGYING_MACHED_DUN_DATABASE", 604800)
            jedis.close()
          } catch {
            case e: Exception => println("error:" + e)
          }
        })
        //        connection.close()
        table.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def conn(): Set[String] = {
    val user = "root"
    val password = "rootROOT1."
    val host = "192.168.15.197"
    val database = "pb"
    val conn_str = "jdbc:mysql://" + host + ":3306/" + database + "?user=" + user + "&password=" + password
    //classOf[com.mysql.jdbc.Driver]
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    val conn = DriverManager.getConnection(conn_str)
    var set = Set("")
    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val rs = statement.executeQuery("SELECT 手机号 FROM pb_tel_risk_lib  WHERE isman = 1;")
      while (rs.next) {
        val name = rs.getString("手机号")
        set += name
      }
      return set
    }
    finally {
      conn.close
    }
  }

  def conn_(): Set[String] = {
    val user = "root"
    val password = "rootROOT1."
    val host = "192.168.15.197"
    val database = "pb"
    val conn_str = "jdbc:mysql://" + host + ":3306/" + database + "?user=" + user + "&password=" + password
    //classOf[com.mysql.jdbc.Driver]
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    val conn = DriverManager.getConnection(conn_str)
    var set = Set("")
    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
      val rs = statement.executeQuery("SELECT 手机号 FROM pb_tel_risk_lib")
      while (rs.next) {
        val name = rs.getString("手机号")
        set += name
      }
      return set
    }
    finally {
      conn.close
    }
  }



}