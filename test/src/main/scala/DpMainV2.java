//import com.anve.datacenter.api.dp.DpEntryV2;
//import com.anve.dataplatform.collect.utils.DateUtils;
//import com.anve.dataplatform.collect.utils.GzipUtils;
//import com.anve.dataplatform.collect.utils.SeriaPojoUtil;
//import org.apache.commons.lang.exception.ExceptionUtils;
//import org.apache.spark.api.java.function.FilterFunction;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.apache.spark.sql.*;
//import org.apache.spark.sql.streaming.OutputMode;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.Collections;
//import java.util.Date;
//import java.util.Iterator;
//import java.util.List;
//
//
///**
// * Created by qqq5 on 2018/4/24.
// */
//public class DpMainV2 {
//
//    public static final Logger LOG = LoggerFactory.getLogger(DpMainV2.class);
//
//    public static void main(String[] args) throws Exception {
//
//        if (args == null || args.length == 0) {
//            args = new String[20];
//            args[0] = "dataplatform";
////            args[1] = "172.20.10.54:9092,172.20.10.55:9092,172.20.10.56:9092";
//            args[1] = "172.16.30.30:9092,172.16.30.12:9092,172.16.30.9:9092";
//            args[2] = "";
////            args[3] = "jdbc:mysql://172.16.10.117:8066/datacenter?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true";
//////            args[4] = "datacenter";
//////            args[5] = "wixnsCd8";
////
////            args[4] = "datacenter";
////            args[5] = "cR5ZrAOa";
//        }
//        System.out.println(args[0] + "  " + args[1] + "  " + args[2]);
//        String name = args[0];
//        String kafakServers = args[1];
//        String hdfs = args[2];
//        SparkSession sparkSession;
////        System.setProperty("hadoop.home.dir", "D:\\hadoop-common-bin-master\\2.7.1");
//        sparkSession = SparkSession
//                .builder()
//                .appName("dataplatform_client")
////                .master("local[*]")
//                .enableHiveSupport()
//                .getOrCreate();
//
//
//        Dataset<Row> df = sparkSession
//                .readStream()
//                .format("kafka")
//                .option("kafka.bootstrap.servers", kafakServers)
//                .option("subscribe", "dataplatform_client")
//                .option("startingOffsets", "earliest")
//                .option("failOnDataLoss", "false")
//                .load();
//
//        df.flatMap(new FlatMapFunction<Row, DpEntryV2>() {
//            @Override
//            public Iterator<DpEntryV2> call(Row row) throws Exception {
//                try {
//                    byte[] bytes = (byte[]) row.get(1);
//                    bytes = GzipUtils.ungzip(bytes);
//                    List<DpEntryV2> entries = SeriaPojoUtil.deserializeList(bytes, DpEntryV2.class);
//                    return entries.iterator();
//                } catch (Exception e) {
//                    System.out.println(ExceptionUtils.getFullStackTrace(e));
//                }
//                return Collections.emptyIterator();
//            }
//        }, Encoders.bean(DpEntryV2.class))
//        .filter((FilterFunction<DpEntryV2>) entry -> {
//            if (entry == null) {
//                return false;
//            }
//            System.out.println(DateUtils.getCurrTime() + " : " + entry.getUserId() + " : " + DateUtils.getCurrTime(new Date(entry.getTime())));
//            return true;
//        }).select(
//            new Column("userId"), new Column("event"),
//            new Column("time"), new Column("ip"),
//            new Column("deviceId"), new Column("appVersion"),
//            new Column("manufacturer"), new Column("model"),
//            new Column("os"), new Column("osVersion"),
//            new Column("screenHeight"), new Column("screenWidth"),
//            new Column("wifi"), new Column("carrier"),
//            new Column("networkType"), new Column("properties"),
//            new Column("day"), new Column("month"),
//            new Column("year"), new Column("traceId"), new Column("screenName")
//        )
//        .writeStream()
//        .format("parquet")
//        .option("checkpointLocation", hdfs + "/checkpoint/" + "dataplatform_client")
//        .option("startingOffsets", "earliest")
//        .outputMode(OutputMode.Append())
//        .option("path", hdfs + "/data/" + name)
////                .trigger(Trigger.ProcessingTime(5000))//10秒触发一次
//        .partitionBy("year", "month", "day")
//        .start();
//
//        System.out.println("Over...");
//        sparkSession.streams().awaitAnyTermination();
//        sparkSession.stop();
//
//    }
//
//
//}
