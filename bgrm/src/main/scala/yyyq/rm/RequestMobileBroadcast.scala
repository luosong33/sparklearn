package yyyq.rm

import org.apache.spark.SparkContext
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.util.HashSet

import org.apache.spark.broadcast.Broadcast
import jpype.AESDecrypt
import yyyq.util.MysqlUtils

object RequestMobileBroadcast {
    @volatile private var instance: Broadcast[HashSet[String]] = null;
    private val requestMobileSet = new HashSet[String]()

    private val emergencyContactMobileSet = new HashSet[String]()
    def getReqMobileSet(mobileString:String): HashSet[String] = {
      var rmSet = new HashSet[String]()
      var conn=MysqlUtils.getBopsConn
      val sql = "select mobile from bops_credit_request where mobile in("+mobileString+")"
      val ps = conn.prepareStatement(sql)
      val rs = ps.executeQuery()

      while (rs.next()) {
        var requestMobile = rs.getString("mobile")
        rmSet.add(requestMobile)
      }
      if (null != rs) {
        rs.close()
      }
      if (null != ps) {
        ps.close()
      }
      if (null != conn) {
        conn.close()
      }
      rmSet
    }
    def getEmergencyContactMobileSet(clientNO:String): HashSet[String] = {
      var ecset = new HashSet[String]()
      var conn1=MysqlUtils.getBopsConn
      val sql = "SELECT MOBILE FROM  bops_client_contact  WHERE CLIENT_NO='"+clientNO+"' AND LINK_TYPE<>'SELF'"
      val ps = conn1.prepareStatement(sql)
      val rs = ps.executeQuery()

      while (rs.next()) {
        var requestMobile = new AESDecrypt().getDecrypt(rs.getString("mobile"))
        ecset.add(requestMobile)
      }
      if (null != rs) {
        rs.close()
      }
      if (null != ps) {
        ps.close()
      }
      if (null != conn1) {
        conn1.close()
      }
      ecset
    }
   /* def handle(sc: SparkContext, blocking: Boolean = false): Unit = {
      if (null != instance) {
        instance.unpersist(blocking)
      }
      instance = sc.broadcast(getReqMobileSet)
    }

    def getInstance(sc: SparkContext): Broadcast[HashSet[String]] = {
      if (instance == null) {
        synchronized {
          if (instance == null) {
            instance = sc.broadcast(getReqMobileSet)
          }
        }
      }
      instance
    }*/
    private def writeObject(out: ObjectOutputStream): Unit = {
      out.writeObject(instance)
    }

    private def readObject(in: ObjectInputStream): Unit = {
      instance = in.readObject().asInstanceOf[Broadcast[HashSet[String]]]
    }
  }