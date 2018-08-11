package yyyq.tc

import java.io.File
import java.nio.charset.CodingErrorAction
import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import net.sf.json.JSONObject
import org.apache.phoenix.query.QueryServices

import scala.collection.mutable.ArrayBuffer
import scala.io.{Codec, Source}
//import tools.GetConnection
import java.sql.SQLException

object data_process0 {
  def main(args: Array[String]): Unit = {
//    val file_path = new File("E:\\ls\\bgrm\\data\\file_last_number_0.txt")
    val file_path = new File("/bigdata/qjx/file_last_number_0.txt")

//    val count = 1
    var list_50 = ArrayBuffer[ArrayBuffer[String]]()

    for (one_file <- analysis_txt(file_path)){
      list_50 += one_file
      if (list_50.length == 10000){
        connect_phoenix(list_50)
        Thread.sleep(1000)
        list_50.clear()
      }
    }
    connect_phoenix(list_50)
  }

  def analysis_txt(path: File): ArrayBuffer[ArrayBuffer[String]] ={
    val decoder = Codec.UTF8.decoder.onMalformedInput(CodingErrorAction.IGNORE)
    val lines = Source.fromFile(path)(decoder).getLines()
//    val lines = Source.fromFile(path).getLines()
    val data_list = ArrayBuffer[ArrayBuffer[String]]()
    while (lines.hasNext){
//      println(lines.next())
      val json_data = JSONObject.fromObject(lines.next())
      var CERT_NO = json_data.getString("cert_no")
//      var CERT_NO = "330621199404258666"
      val result_data = json_data.getJSONArray("result")
      for (j <- 0 until result_data.size()) { // 遍历items JsonArray
        var item = result_data.getJSONObject(j) // 得到一个item
        var Etype = item.get("type").toString
        var Eproject = item.get("project").toString
        var EevidenceEid = item.get("evidenceEid").toString
        var Etime = item.get("time").toString
        var EevidenceId = item.get("evidenceId").toString

        var one = ArrayBuffer[String](EevidenceEid, CERT_NO, Etype, Eproject, Etime, EevidenceId)
//        println(one)
        data_list += one
      }
    }
    data_list
  }

  def connect_phoenix(list_50: ArrayBuffer[ArrayBuffer[String]]): Unit = {
    // Change to Your Database Config

    val connectionProperties = new Properties
    connectionProperties.setProperty(QueryServices.MAX_MUTATION_SIZE_ATTRIB, "10000000")
    val url = "jdbc:phoenix:192.168.15.195:2181/hbase"
    val conn = DriverManager.getConnection(url,connectionProperties)
    val statement = conn.createStatement()
    var sql = "upsert into \"e_qianbao_crawler_test\"(\"ID\", \"CERT_NO\", \"type\",\"project\", \"Etime\", \"evidenceId\",\"GMT_CREATED\",\"GMT_MODIFIED\") values(?,?,?,?,?,?,?,?)"
    try {
      val prep = conn.prepareStatement(sql)
      for (list_1 <- list_50){
        try{
          prep.setString(1, list_1(0).toString)
          prep.setString(2, list_1(1).toString)
          prep.setString(3, list_1(2).toString)
          prep.setString(4, list_1(3).toString)
          prep.setString(5, list_1(4).toString)
          prep.setString(6, list_1(5).toString)
          prep.setString(7, getNowDate())
          prep.setString(8, getNowDate())
          prep.addBatch()
        }catch {
          case e: SQLException =>
            e.printStackTrace()
            println("单条数据出错")
            println(list_1)
        }
        //        ESIGN_SIGNLOG
      }
      prep.executeBatch()
      conn.commit()
      println("已经存入10000条")

    } catch{
      case e:Exception =>e.printStackTrace()
        println("该批数据库插入出错")
    }
    finally {

      conn.close()
    }
  }

  def getNowDate():String={
    var now:Date = new Date()
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var hehe = dateFormat.format(now)
    hehe
  }

}
