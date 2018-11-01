import org.apache.hadoop.util.Time
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel

/**
  * Created by genie on 2018/10/23.
  */
object Streaming2Parquet {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
      .setAppName("Event Stream Ingestion Pipeline")
      .setMaster("local[3]")
    val ssc = new StreamingContext(sparkConf, Seconds(120))

    val events = ssc.socketTextStream("localhost", 9999,
      StorageLevel.MEMORY_AND_DISK_SER)

    // Convert RDDs of the events DStream to DataFrame
    events.foreachRDD((event: RDD[String], time: Time) => {

      val sqlContext = SQLContextSingleton.getInstance(event.sparkContext)
      import sqlContext.implicits._
      val eventsDataFrame = event.map(w => CDCEvent(w)).toDF()
      eventsDataFrame.coalesce(1).write.mode(SaveMode.Append).parquet("/tmp/parquet2");
    })

    ssc.start()
    ssc.awaitTermination()
  }

  case class CDCEvent(word: String)

  object SQLContextSingleton {
    @transient private var instance: SQLContext = _

    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }

  }

}
