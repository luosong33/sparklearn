package yyyq.tc

import org.apache.spark.{SparkConf, SparkContext}
import yyyq.util.DateUtil

object WordCount {

  def main(args: Array[String]): Unit = {
    val starttime = System.currentTimeMillis
    val conf = new SparkConf().setMaster("yarn")  //  设置运行核数，local[*] 表示自适应本机核数
      .setAppName("WordCount")                        //  设置应用程序名称
    val sc = new SparkContext(conf)
    sc.textFile("hdfs://192.168.15.195:8020/bigdata/ls/wc/*")    //  加载hadoop文件
//    sc.textFile("file:\\\\\\E:\\ls\\bgrm\\data\\gathering")    //  加载本地文件
//    sc.textFile("file:///E:/ls/bgrm/data/gathering")    //  加载本地文件
      .flatMap(_.split(" "))                          //  按空格切分单词   flatMap:map一一映射处理元素，再flatten弄平  常用于切分
      .map((_, 1))                                    //  对每个单词进行计数，
      .reduceByKey(_ + _,4)                           //  以key分组  4设置并行度
      .sortBy(_._2, false)                           //  标准排序，默认是true，也就是升序
      .repartition(1)                                //   设置一个结果文件
      .saveAsTextFile("hdfs://192.168.15.195:8020/bigdata/ls/wc/out")    //  输出到hdfs目录，必须不存在
//      .saveAsTextFile("file:///E:/ls/bgrm/data/out")    //  输出到本地目录，必须不存在
    sc.stop()

    val endtime = System.currentTimeMillis
    System.out.println(DateUtil.nowString + " WordCount耗时为： " + (endtime - starttime))

  }

}
