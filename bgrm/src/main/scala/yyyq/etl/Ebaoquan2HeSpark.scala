package yyyq.etl

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import yyyq.util.DateUtil

object Ebaoquan2HeSpark {

  def main(args: Array[String]): Unit = {
    val starttime = System.currentTimeMillis

    val conf = new SparkConf().setMaster("local[4]")
    val spark = SparkSession
      .builder()
      .appName("Ebaoquan2HeSpark")
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext

    // 创建hbase configuration
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hBaseConf.set("hbase.zookeeper.quorum", "192.168.15.195,192.168.15.196,192.168.15.197,192.168.15.198,192.168.15.199")
    hBaseConf.set(TableInputFormat.INPUT_TABLE, "yyyd_info") //  yyyd_info  ebaoquan_24h_formal
    hBaseConf.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, "120000");

    // 从数据源获取数据
    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    hbaseRDD.cache()

    val count = hbaseRDD.count()
    println("Students RDD Count:" + count)

    var i = 0
    hbaseRDD.foreach { case (_, result) =>
      val key = Bytes.toString(result.getRow)
      val client_no = Bytes.toString(result.getValue("c".getBytes, "client_no".getBytes))
      val party_type = Bytes.toString(result.getValue("c".getBytes, "party_type".getBytes))
      i += 1
      println(i + " Row key:" + key + " client_no:" + client_no + " party_type:" + party_type)
    }

//    hbaseRDD.

    sc.stop()
    val endtime = System.currentTimeMillis
    System.out.println(DateUtil.nowString + " Count 耗时为： " + (endtime - starttime))
  }

}
