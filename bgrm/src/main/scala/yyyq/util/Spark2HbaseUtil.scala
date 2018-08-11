package yyyq.util

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark2HbaseUtil {
  def sparkReadHbase(): Unit = {

    // 创建hbase configuration
    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hBaseConf.set("hbase.zookeeper.quorum", "192.168.15.195,192.168.15.196,192.168.15.197,192.168.15.198,192.168.15.199")
    hBaseConf.set(TableInputFormat.INPUT_TABLE, "emay_emr007")
    hBaseConf.set(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, "120000");

    val conf = new SparkConf().setMaster("local[5]")
    val spark = SparkSession.builder().appName("readHBase").config(conf).getOrCreate()
    val sc = spark.sparkContext

    // 从数据源获取数据
    val hbaseRDD = sc.newAPIHadoopRDD(hBaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

    val count = hbaseRDD.count()
    println("Students RDD Count:" + count)
  }

  def main(args: Array[String]): Unit = {
    sparkReadHbase()
  }

}
