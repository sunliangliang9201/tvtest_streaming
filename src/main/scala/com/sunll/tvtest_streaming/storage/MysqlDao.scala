package com.sunll.tvtest_streaming.storage

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.sunll.tvtest_streaming.model.StreamingKeyConfig
import com.sunll.tvtest_streaming.utils.ConfigUtil

import scala.collection.mutable.ListBuffer

/**
  * mysql funcs object
  * @author sunliangliang 2018-09-15
  * @version 1.0
  */
object MysqlDao {

  val streamingSQL = "select streaming_key, app_name, driver_cores, formator, topics, group_id, table_name, fields, broker_list from realtime_streaming_config where streaming_key = ?"
  val configSQL = "select field,turn from tvtest_streaming_fields where enabled = 1 and streaming_key = ?"
  def findStreamingKeyConfig(streamingKey: String): StreamingKeyConfig = {
    val jdbcUrl = ConfigUtil.getConf.get.getString("cangku_host")
    val user = ConfigUtil.getConf.get.getString("cangku_username")
    val passwd = ConfigUtil.getConf.get.getString("cangku_password")
    val db = ConfigUtil.getConf.get.getString("cangku_datebase")
    val port = ConfigUtil.getConf.get.getString("cangku_port")
    var conn: Connection = null
    var ps: PreparedStatement = null
    var streamingKeyConfig: StreamingKeyConfig = null
    try {
      Class.forName("com.mysql.jdbc.Driver")
      conn = DriverManager.getConnection("jdbc:mysql://" + jdbcUrl + ":" + port + "/" + db, user, passwd)
      ps = conn.prepareStatement(streamingSQL)
      ps.setString(1, streamingKey)
      val res = ps.executeQuery()
      res.last()
      val cnt = res.getRow
      if (cnt == 1) {
        streamingKeyConfig = StreamingKeyConfig(
          res.getString("streaming_key"),
          res.getString("app_name"),
          res.getString("driver_cores"),
          res.getString("formator"),
          res.getString("topics"),
          res.getString("group_id"),
          res.getString("table_name"),
          res.getString("fields"),
          res.getString("broker_list")
        )
      } else {
        throw new Exception("mysql strming key set error...")
      }
    } catch {
      case e: Exception => println(e)
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
    streamingKeyConfig
  }

  def findStreamingKeyFileldsConfig(streamingKey: String): ListBuffer[(String, Int)] ={
    val fieldsList: ListBuffer[(String,Int)] = new ListBuffer[(String, Int)]()
    var conn: Connection = null
    var ps: PreparedStatement = null
    val jdbcUrl = ConfigUtil.getConf.get.getString("tvtest_host")
    val user = ConfigUtil.getConf.get.getString("tvtest_username")
    val passwd = ConfigUtil.getConf.get.getString("tvtest_password")
    val db = ConfigUtil.getConf.get.getString("tvtest_datebase")
    val port = ConfigUtil.getConf.get.getString("tvtest_port")
    try{
      Class.forName("com.mysql.jdbc.Driver")
      conn = DriverManager.getConnection("jdbc:mysql://" + jdbcUrl + ":" + port + "/" + db, user, passwd)
      ps = conn.prepareStatement(configSQL)
      ps.setString(1, streamingKey)
      val res = ps.executeQuery()
      while(res.next()){
        fieldsList.append((res.getString("field"), res.getInt("turn")))
      }
    }catch{
      case e:Exception => println(e)
    }finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
    fieldsList
  }
  def insertBatch(y: Iterator[ListBuffer[String]]): Unit ={
    var conn: Connection = null
    var ps: PreparedStatement = null
    val jdbcUrl = ConfigUtil.getConf.get.getString("tvtest_host")
    val user = ConfigUtil.getConf.get.getString("tvtest_username")
    val passwd = ConfigUtil.getConf.get.getString("tvtest_password")
    val db = ConfigUtil.getConf.get.getString("tvtest_datebase")
    val port = ConfigUtil.getConf.get.getString("tvtest_port")
    try{
      Class.forName("com.mysql.jdbc.Driver")
      conn = DriverManager.getConnection("jdbc:mysql://" + jdbcUrl + ":" + port + "/" + db, user, passwd)
      ps = conn.prepareStatement(configSQL)
      ps.setString(1, streamingKey)
      val res = ps.executeQuery()
      while(res.next()){
        fieldsList.append((res.getString("field"), res.getInt("turn")))
      }
    }catch{
      case e:Exception => println(e)
    }finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }
}
