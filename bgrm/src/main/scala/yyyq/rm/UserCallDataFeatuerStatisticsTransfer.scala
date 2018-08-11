package yyyq.rm

import java.text.SimpleDateFormat

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

object UserCallDataFeatuerStatisticsTransfer {
  def getTimestamp(x: String, y: String): Long = {
    val format = new SimpleDateFormat(y)
    format.parse(x).getTime
  }

  def getDate(x: Long): String = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.format(x)
  }

  def main(args: Array[String]) {
    var masterUrl = "local[5]" //  local[8] yarn
    if (args.length > 0) {
      masterUrl = args(0)
    }
    // Create a StreamingContext with the given master URL
    val conf = new SparkConf().setMaster(masterUrl).setAppName("UserCall_local_his_20180305")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "4")
    conf.set("spark.streaming.backpressure.enabled", "true")
    //第一参数为SparkConf对象，第二个参数为批次时间
    val ssc = new StreamingContext(conf, Seconds(6))
    val topics = Set("MOXIE_CARRIER_ACCESS_SUMMIT_HISTORY") // MOXIE_CARRIER_ACCESS_SUMMIT  MOXIE_CARRIER_ACCESS_SUMMIT_HISTORY
    val brokers = "192.168.15.195:9092,192.168.15.196:9092,192.168.15.197:9092,192.168.15.198:9092,192.168.15.199:9092"
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers,
      "serializer.class" -> "kafka.serializer.StringEncoder",
      "group.id" -> "UserCall_local_his_20180305",
      "fetch.message.max.bytes" -> "134217728")

    // Create a direct stream
    //    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    var events = kafkaStream.map(line => {
      //30天内有大于等于15天连续八小时及以上小时未接电话的客户在本次授信中拒绝
      var workTimeThirtyDaySilent = false //这个规则是否拒绝，默认不拒绝
      //20天内有大于等于10天连续八小时及以上小时未接电话的客户在本次授信中拒绝
      var workTimeTweentyDaySilent = false //这个规则是否拒绝，默认不拒绝
      //10天内有大于等于6天连续八小时及以上小时未接电话的客户在本次授信中拒绝
      var workTimeTenDaySilent = false //这个规则是否拒绝，默认不拒绝
      var nightThirtyDaySilent = false //默认不拒绝
      var nightTweentyDaySilent = false
      var nightTenDaySilent = false
      var workTimeThirtyDayHourSilent = false
      var workTimeTweentyDayHourSilent = false
      var workTimeTenDayHourSilent = false
      var clientNo: String = ""
      var creditNo: String = ""
      var origDate = ""
      var nowDate = ""
      var slientDateCount = 0
      var durationRatio = 0.00
      var tenDialTypeRatio = 0.00
      var thirtyDialTypeRatio = 0.00
      try {
        var moxieLog = JSONObject.fromObject(line._2)
        //获取客户号
        clientNo = moxieLog.getString("clientNo")
        //获取授信号
        creditNo = moxieLog.getString("creditNo")
        origDate = moxieLog.getString("origDate") // 获取原始时间
        //通话详单jsonArray
        var calls = moxieLog.getJSONArray("calls")
        //数据获取时间
        var lastModifyTime = moxieLog.getString("last_modify_time");

        //hourMap[clientNo|creditNo|通话日期，List[通话小时]]
        var hourMap = scala.collection.mutable.Map[String, scala.collection.mutable.HashSet[Int]]()
        //hourCallCountMap[clientNo|creditNo|2017-09-26 12,通话次数]
        var hourCallCountMap = scala.collection.mutable.Map[String, Int]()
        //有通话的日期Set
        var callDateSet = scala.collection.mutable.HashSet[String]()
        var totalDuration = 0
        var nightTotalDuration = 0
        //十天被叫类型通话的所有人
        var tenDialedCallSet = scala.collection.mutable.HashSet[String]()
        //十天通过话的所有人
        var tenCallSet = scala.collection.mutable.HashSet[String]()
        var thirtyDialedCallSet = scala.collection.mutable.HashSet[String]()
        var thirtyCallSet = scala.collection.mutable.HashSet[String]()

        //遍历通话详单jsonArray
        for (i <- 0 until calls.size()) {
          //得到一个通话详单call
          var call = calls.getJSONObject(i); // 遍历 jsonarray 数组，把每一个对象转成 json 对象
          //得到通话详单里面的item jsonArray
          var items = call.getJSONArray("items")
          //遍历items JsonArray
          for (j <- 0 until items.size()) {
            //得到一个item
            var item = items.getJSONObject(j)
            //取出通话时间
            var time = item.getString("time")
            //取出通话时长
            var duration = item.getString("duration");
            //取出通话类型
            var dial_type = item.getString("dial_type");
            //取出通话人
            var peer_number = item.getString("peer_number");

            //hourKey 客户号|授信号|日期的年月（2017-09-26 12）
            var hourKey = clientNo + "|" + creditNo + "|" + time.substring(0, 13)
            //因为统计只需最近三十天的数据，所以只拉取最近30天的通话详单
            if (getTimestamp(lastModifyTime, "yyyy-MM-dd HH:mm:ss") - getTimestamp(time, "yyyy-MM-dd HH:mm:ss") <= 30 * 24 * 60 * 60 * 1000l) {
              //用hourCallCountMap存储[clientNo|creditNo|2017-09-26 12,通话次数]
              if (hourCallCountMap.contains(hourKey)) {
                hourCallCountMap.put(hourKey, hourCallCountMap.get(hourKey).get + 1)
              } else {
                hourCallCountMap.put(hourKey, 1)
              }
              totalDuration = totalDuration + Integer.valueOf(duration)

              thirtyCallSet.add(peer_number)
              if (dial_type.equals("DIALED")) {
                thirtyDialedCallSet.add(peer_number)
              }
              if (getTimestamp(lastModifyTime, "yyyy-MM-dd HH:mm:ss") - getTimestamp(time, "yy-MM-dd HH:mm:ss") <= 10 * 24 * 60 * 60 * 1000l) {
                tenCallSet.add(peer_number)
                if (dial_type.equals("DIALED")) {
                  tenDialedCallSet.add(peer_number)
                }
              }
              if (time.substring(11, 13).toInt < 8) {
                nightTotalDuration = nightTotalDuration + Integer.valueOf(duration)
              }
              //如果在白天有通话，就把日期放入callDateSet
              if (time.substring(11, 13).toInt > 7 && time.substring(11, 13).toInt < 24) {
                callDateSet.add(time.substring(0, 10))
              }
              //用hourMap存储 [clientNo|creditNo|2017-09-26,List[通话的小时]]
              var dateKey = clientNo + "|" + creditNo + "|" + time.substring(0, 10)
              if (hourMap.contains(dateKey)) {
                var tempL = hourMap.get(dateKey)
                tempL.get.add(time.substring(11, 13).toInt)
              } else {
                var tempList = scala.collection.mutable.HashSet[Int]()
                tempList.add(time.substring(11, 13).toInt)
                hourMap.put(dateKey, tempList)
              }
            }
          }
        }
        if (totalDuration > 0) {
          durationRatio = nightTotalDuration.toDouble / totalDuration
        }
        if (thirtyCallSet.size > 0) {
          thirtyDialTypeRatio = thirtyDialedCallSet.size.toDouble / thirtyCallSet.size
        }
        if (tenCallSet.size > 0) {
          tenDialTypeRatio = tenDialedCallSet.size.toDouble / tenCallSet.size
        }
        slientDateCount = 30 - callDateSet.size
        if (slientDateCount < 0) {
          slientDateCount = 0
        }
        //dateMap[clientNo|creditNo,list[连续未通话小于八小时的日期]]
        var dateMap = scala.collection.mutable.Map[String, scala.collection.mutable.HashSet[String]]()
        //dateHasCallMap[clientNo|creditNo,list[夜间连续3个小时及以上的日期]]
        var dateHasCallMap = scala.collection.mutable.Map[String, scala.collection.mutable.HashSet[String]]()
        //遍历hourMap[clientNo|creditNo|2017-09-18,Set[通话的小时]]
        for (i <- hourMap) {
          //把通话的小时转化为list并排序
          var hourList = i._2.toList.sorted
          //最大连续未通话小时数
          var maxHour = 0
          if (hourList.size > 0) {
            if (hourList(hourList.size - 1) > 7) {
              maxHour = 24 - hourList(hourList.size - 1)
              if (maxHour < hourList(0) - 7) {
                maxHour = hourList(0) - 7
              }
            }
          }
          //最大连续通话时间
          var maxCallHour = 0
          //连续通话时间
          var callHour = 0
          //通过循环得出最大连续未通话小时数maxHour
          for (i <- 1 until hourList.size) {
            //上一个通话的小时
            var preHour = hourList(i - 1)
            //当前通话的小时
            var curHour = hourList(i)
            //如果通话时间在8：00-23:59内

            if (preHour < 24 && curHour < 24 && preHour > 7 && curHour > 7) {
              //当前通话的小时减去上次通话的小时就是连续未通话小时数加1，如果大于maxHour，就赋值给maxHour
              if (curHour - preHour > maxHour) {
                maxHour = curHour - preHour
              }
            } //通话时间在0:00-7
            else if (preHour < 8 && curHour < 8 && preHour >= 0 && curHour >= 0) {
              if (curHour - preHour == 1) {
                callHour = callHour + 1
              } else {
                if (callHour > maxCallHour) {
                  maxCallHour = callHour
                }
                callHour = 0
              }
            }
          }
          //如果某天最大连续未通话小时数大于0，小于等于8，就把日期放入dataMap中
          if (maxHour > 0 && maxHour <= 8) {
            //连续八小时未通话日期callDate
            var callDate = i._1.split("\\|")(2);
            //key是clientNo|creditNo
            var key = i._1.split("\\|")(0) + "|" + i._1.split("\\|")(1)
            if (dateMap.contains(key)) {
              var tempL = dateMap.get(key)
              tempL.get.add(callDate)
            } else {
              var tempList = scala.collection.mutable.HashSet[String]()
              tempList.add(callDate)
              dateMap.put(key, tempList)
            }
          }
          //如果最大连续通话时间大于等于3，这个就是夜间连续通话三个小时以上的日期，存入dateHasCallMap
          if (maxCallHour >= 2) {
            var callDate = i._1.split("\\|")(2);
            var key = i._1.split("\\|")(0) + "|" + i._1.split("\\|")(1)
            if (dateHasCallMap.contains(key)) {
              var tempL = dateHasCallMap.get(key).get
              tempL.add(callDate)
            } else {
              var tempList = scala.collection.mutable.HashSet[String]()
              tempList.add(callDate)
              dateHasCallMap.put(key, tempList)
            }
          }
        }
        //dateSet clientNo|creditNo 的小于连续八小时未通话的日期集合
        var dateSet = dateMap.get(clientNo + "|" + creditNo).getOrElse(null)
        //最近30天连续未通话小于八小时的日期个数
        var thirtyCount = 0
        //最近20天连续未通话小于八小时的日期个数
        var tweentyCount = 0
        //最近10天连续未通话小于八小时的日期个数
        var tenCount = 0
        if (null != dateSet) {
          for (i <- dateSet) {
            if (getTimestamp(lastModifyTime, "yyyy-MM-dd HH:mm:ss") - getTimestamp(i, "yy-MM-dd") <= 30 * 24 * 60 * 60 * 1000l) {
              thirtyCount = thirtyCount + 1
            }
            if (getTimestamp(lastModifyTime, "yyyy-MM-dd HH:mm:ss") - getTimestamp(i, "yy-MM-dd") <= 20 * 24 * 60 * 60 * 1000l) {
              tweentyCount = tweentyCount + 1
            }
            if (getTimestamp(lastModifyTime, "yyyy-MM-dd HH:mm:ss") - getTimestamp(i, "yy-MM-dd") <= 10 * 24 * 60 * 60 * 1000l) {
              tenCount = tenCount + 1
            }
          }
        }
        if (thirtyCount < 15) {
          workTimeThirtyDaySilent = true
        }
        if (tweentyCount < 10) {
          workTimeTweentyDaySilent = true
        }
        if (tenCount < 5) {
          workTimeTenDaySilent = true
        }
        //夜间连续三小时有通话的日期集合
        var dateHasCallSet = dateHasCallMap.get(clientNo + "|" + creditNo).getOrElse(null)
        //30天内夜间连续三小时及以上有通话的日期数量
        var thirtyHasCallCount = 0
        //20天内夜间连续三小时及以上有通话的日期数量
        var tweentyHasCallCount = 0
        //10天内夜间连续3小时及以上有童话的额日期数量
        var tenHasCallCount = 0
        if (null != dateHasCallSet) {
          for (i <- dateHasCallSet) {
            if (getTimestamp(lastModifyTime, "yyyy-MM-dd HH:mm:ss") - getTimestamp(i, "yy-MM-dd") <= 30 * 24 * 60 * 60 * 1000l) {
              thirtyHasCallCount = thirtyHasCallCount + 1
            }
            if (getTimestamp(lastModifyTime, "yyyy-MM-dd HH:mm:ss") - getTimestamp(i, "yy-MM-dd") <= 20 * 24 * 60 * 60 * 1000l) {
              tweentyHasCallCount = tweentyHasCallCount + 1
            }
            if (getTimestamp(lastModifyTime, "yyyy-MM-dd HH:mm:ss") - getTimestamp(i, "yy-MM-dd") <= 10 * 24 * 60 * 60 * 1000l) {
              tenHasCallCount = tenHasCallCount + 1
            }
          }
        }
        //30天内夜间连续三小时通话的日期数量大于20天，本次用户的授信拒绝
        if (thirtyHasCallCount >= 20) {
          nightThirtyDaySilent = true
        }
        if (tweentyHasCallCount >= 10) {
          nightTweentyDaySilent = true
        }
        if (tenHasCallCount >= 7) {
          nightTenDaySilent = true
        }
        //thirtyCheck+"|"+tweentyCheck+"|"+tenCheck+"|"+thirtyHasCallCheck+"|"+tweentyHasCallCheck+"|"+tenHasCallCheck
        //tenHourM[clientNo|creditNo,set[小时]]
        var tenHourM = scala.collection.mutable.Map[String, scala.collection.mutable.HashSet[Int]]()
        var tweentyHourM = scala.collection.mutable.Map[String, scala.collection.mutable.HashSet[Int]]()
        var thirtyHourM = scala.collection.mutable.Map[String, scala.collection.mutable.HashSet[Int]]()
        //tenHourCM[clinetNo|creditNo|hour,通话次数]
        var tenHourCM = scala.collection.mutable.Map[String, Int]()
        var tweentyHourCM = scala.collection.mutable.Map[String, Int]()
        var thirtyHourCM = scala.collection.mutable.Map[String, Int]()
        //hourCallCountMap[clientNo|creditNo|2017-09-26 12,通话次数]
        for (i <- hourCallCountMap) {
          var k = i._1
          var c = i._2
          var tempKey = k.split("\\|")(0) + "|" + k.split("\\|")(1)
          var tempTime = k.split("\\|")(2)
          var tenKey = tempKey + "|" + tempTime.substring(11, 13)
          if (getTimestamp(lastModifyTime, "yyyy-MM-dd HH:mm:ss") - getTimestamp(tempTime, "yyyy-MM-dd HH") < 30 * 24 * 60 * 60 * 1000l) {
            if (thirtyHourCM.contains(tenKey)) {
              thirtyHourCM.put(tenKey, thirtyHourCM.get(tenKey).get + c)
            } else {
              thirtyHourCM.put(tenKey, c)
            }
            if (getTimestamp(lastModifyTime, "yyyy-MM-dd HH:mm:ss") - getTimestamp(tempTime, "yyyy-MM-dd HH") < 20 * 24 * 60 * 60 * 1000l) {
              if (tweentyHourCM.contains(tenKey)) {
                tweentyHourCM.put(tenKey, tweentyHourCM.get(tenKey).get + c)
              } else {
                tweentyHourCM.put(tenKey, c)
              }
              if (getTimestamp(lastModifyTime, "yyyy-MM-dd HH:mm:ss") - getTimestamp(tempTime, "yyyy-MM-dd HH") < 10 * 24 * 60 * 60 * 1000l) {
                if (tenHourCM.contains(tenKey)) {
                  tenHourCM.put(tenKey, tenHourCM.get(tenKey).get + c)
                } else {
                  tenHourCM.put(tenKey, c)
                }
              }
            }
          }
        }
        for (i <- tenHourCM) {
          var k = i._1
          var c = i._2
          var tempKey = k.split("\\|")(0) + "|" + k.split("\\|")(1)
          var tempH = k.split("\\|")(2)
          if (c > 1) {
            if (tenHourM.contains(tempKey)) {
              var tempL = tenHourM.get(tempKey).get
              tempL.add(tempH.toInt)
            } else {
              var tempList = scala.collection.mutable.HashSet[Int]()
              tempList.add(tempH.toInt)
              tenHourM.put(tempKey, tempList)
            }
          }
        }
        for (i <- tweentyHourCM) {
          var k = i._1
          var c = i._2
          var tempKey = k.split("\\|")(0) + "|" + k.split("\\|")(1)
          var tempH = k.split("\\|")(2)
          if (c > 2) {
            if (tweentyHourM.contains(tempKey)) {
              var tempL = tweentyHourM.get(tempKey).get
              tempL.add(tempH.toInt)
            } else {
              var tempList = scala.collection.mutable.HashSet[Int]()
              tempList.add(tempH.toInt)
              tweentyHourM.put(tempKey, tempList)
            }
          }
        }
        for (i <- thirtyHourCM) {
          var k = i._1
          var c = i._2
          var tempKey = k.split("\\|")(0) + "|" + k.split("\\|")(1)
          var tempH = k.split("\\|")(2)
          if (c > 3) {
            if (thirtyHourM.contains(tempKey)) {
              var tempL = thirtyHourM.get(tempKey).get
              tempL.add(tempH.toInt)
            } else {
              var tempList = scala.collection.mutable.HashSet[Int]()
              tempList.add(tempH.toInt)
              thirtyHourM.put(tempKey, tempList)
            }
          }
        }
        for (i <- tenHourM) {
          var hourList = i._2.toList.sorted
          var maxHour = 0
          if (hourList.size > 0) {
            if (hourList(hourList.size - 1) > 7) {
              maxHour = 24 - hourList(hourList.size - 1)
              if (maxHour < hourList(0) - 7) {
                maxHour = hourList(0) - 7
              }
            }
          }
          for (i <- 1 until hourList.size) {
            var preHour = hourList(i - 1)
            var curHour = hourList(i)
            if (preHour < 24 && curHour < 24 && preHour > 7 && curHour > 7) {
              if (curHour - preHour > maxHour) {
                maxHour = curHour - preHour
              }
            }
          }
          if (maxHour >= 7 || maxHour == 0) {
            workTimeTenDayHourSilent = true
          }
        }
        for (i <- tweentyHourM) {
          var hourList = i._2.toList.sorted
          var maxHour = 0
          if (hourList.size > 0) {
            if (hourList(hourList.size - 1) > 7) {
              maxHour = 24 - hourList(hourList.size - 1)
              if (maxHour < hourList(0) - 7) {
                maxHour = hourList(0) - 7
              }
            }
          }
          for (i <- 1 until hourList.size) {
            var preHour = hourList(i - 1)
            var curHour = hourList(i)
            if (preHour < 24 && curHour < 24 && preHour > 7 && curHour > 7) {
              if (curHour - preHour > maxHour) {
                maxHour = curHour - preHour
              }
            }
          }
          if (maxHour >= 11 || maxHour == 0) {
            workTimeTweentyDayHourSilent = true
          }
        }
        for (i <- thirtyHourM) {
          var hourList = i._2.toList.sorted
          var maxHour = 0
          if (hourList.size > 0) {
            if (hourList(hourList.size - 1) > 7) {
              maxHour = 24 - hourList(hourList.size - 1)
              if (maxHour < hourList(0) - 7) {
                maxHour = hourList(0) - 7
              }
            }
          }
          for (i <- 1 until hourList.size) {
            var preHour = hourList(i - 1)
            var curHour = hourList(i)
            if (preHour < 24 && curHour < 24 && preHour > 7 && curHour > 7) {
              if (curHour - preHour > maxHour) {
                maxHour = curHour - preHour
              }
            }
          }
          if (maxHour >= 7 || maxHour == 0) {
            workTimeThirtyDayHourSilent = true
          }
        }
      } catch {
        case e: Exception => println(clientNo)
      }
      new Tuple2(clientNo + "|" + creditNo, workTimeThirtyDaySilent + "|" + workTimeTweentyDaySilent
        + "|" + workTimeTenDaySilent + "|" + nightThirtyDaySilent
        + "|" + nightTweentyDaySilent + "|" + nightTenDaySilent
        + "|" + workTimeThirtyDayHourSilent + "|" + workTimeTweentyDayHourSilent
        + "|" + workTimeTenDayHourSilent
        + "|" + origDate + "|" + slientDateCount + "|" + durationRatio.formatted("%.4f") + "|" + thirtyDialTypeRatio.formatted("%.4f") + "|" + tenDialTypeRatio.formatted("%.4f"))
    })
    events.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val hconf = HBaseConfiguration.create()
        hconf.set("hbase.zookeeper.quorum", "192.168.15.195,192.168.15.196,192.168.15.197,192.168.15.198,192.168.15.199")
        hconf.set("hbase.zookeeper.property.clientPort", "2181")
        val table = new HTable(hconf, "call_featuer_result");
        partitionOfRecords.foreach(pair => {
          val put = new Put(Bytes.toBytes(pair._1))
          var thirtyCheck = pair._2.split("\\|")(0)
          var tweentyCheck = pair._2.split("\\|")(1)
          var tenCheck = pair._2.split("\\|")(2)
          var thirtyHasCallCheck = pair._2.split("\\|")(3)
          var tweentyHasCallCheck = pair._2.split("\\|")(4)
          var tenHasCallCheck = pair._2.split("\\|")(5)
          var thirtyHourCheck = pair._2.split("\\|")(6)
          var tweentyHourCheck = pair._2.split("\\|")(7)
          var tenHourCheck = pair._2.split("\\|")(8)
          var origDate = pair._2.split("\\|")(9)
          var workTimeSlientDateCount = pair._2.split("\\|")(10)
          var nightDurationRatio = pair._2.split("\\|")(11)
          var thirtyDialTypeRatio = pair._2.split("\\|")(12)
          var tenDialTypeRatio = pair._2.split("\\|")(13)
          var nowDate = DateUtil.nowString();
          /*var result = "{'userId':'" + pair._1.split("\\|")(0) +
            "','type':'YINGYING_CARRIER_REP_ACCESS_SUMMIT" +
            "','workTimeThirtyDaySilent':'" + thirtyCheck +
            "','workTimeTweentyDaySilent':'" + tweentyCheck +
            "','workTimeTenDaySilent':'" + tenCheck +
            "','nightThirtyDaySilent':'" + thirtyHasCallCheck +
            "','nightTweentyDaySilent':'" + tweentyHasCallCheck +
            "','nightTenDaySilent':'" + tenHasCallCheck +
            "','workTimeThirtyDayHourSilent':'" + thirtyHourCheck +
            "','workTimeTweentyDayHourSilent':'" + tweentyHourCheck +
            "','workTimeTenDayHourSilent':'" + tenHourCheck +
            "','dialCallRate10':'" + tenDialTypeRatio +
            "','dialCallRate30':'" + thirtyDialTypeRatio +
            "','workTimeNocall30':'" + workTimeSlientDateCount +
            "','nightTimeCallRate30':'" + nightDurationRatio + "'}"
          println(JSONObject.fromObject(result))
          HTTPUtils.httpPostWithJson(JSONObject.fromObject(result), "http://riskdata-pool/dataDirect/operator");*/
          put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("workTimeThirtyDaySilent"), Bytes.toBytes(thirtyCheck))
          put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("workTimeTweentyDaySilent"), Bytes.toBytes(tweentyCheck))
          put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("workTimeTenDaySilent"), Bytes.toBytes(tenCheck))
          put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("nightThirtyDaySilent"), Bytes.toBytes(thirtyHasCallCheck))
          put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("nightTweentyDaySilent"), Bytes.toBytes(tweentyHasCallCheck))
          put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("nightTenDaySilent"), Bytes.toBytes(tenHasCallCheck))
          put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("workTimeThirtyDayHourSilent"), Bytes.toBytes(thirtyHourCheck))
          put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("workTimeTweentyDayHourSilent"), Bytes.toBytes(tweentyHourCheck))
          put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("workTimeTenDayHourSilent"), Bytes.toBytes(tenHourCheck))
          put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("origDate"), Bytes.toBytes(origDate))
          put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("nowDate"), Bytes.toBytes(nowDate))
          put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("workTimeSlientDateCount"), Bytes.toBytes(workTimeSlientDateCount))
          put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("nightDurationRatio"), Bytes.toBytes(nightDurationRatio))
          put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("thirtyDialTypeRatio"), Bytes.toBytes(thirtyDialTypeRatio))
          put.addColumn(Bytes.toBytes("c"), Bytes.toBytes("tenDialTypeRatio"), Bytes.toBytes(tenDialTypeRatio))

          table.put(put)
          //权限认证
          //jedis.set(pair._1 + "|" + "YINGYING_CARRIER_REP_ACCESS_SUMMIT", "");
          val jedis = new Jedis("192.168.2.211", 8000);
          jedis.auth("yylc8888");
          jedis.set(pair._1 + "|" + "YINGYING_CARRIER_REP_ACCESS_SUMMIT", "")
          jedis.expire(pair._1 + "|" + "YINGYING_CARRIER_REP_ACCESS_SUMMIT", 60 * 60 * 24 * 7)
          jedis.close()
        })
        table.close()
      })
    })
    ssc.start()
    ssc.awaitTermination()
  }
}