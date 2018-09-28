package com.sunll.tvtest_streaming.storage

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.sunll.tvtest_streaming.model.StreamingKeyConfig
import com.sunll.tvtest_streaming.utils.ConfigUtil
import org.slf4j.LoggerFactory

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * mysql funcs object
  * @author sunliangliang 2018-09-15
  * @version 1.0
  */
object MysqlDao {

  val alterSQL = "alter table %s add %s varchar(255) default null"

  val streamingSQL = "select streaming_key, app_name, driver_cores, formator, topics, group_id, table_name, fields, broker_list from realtime_streaming_config where streaming_key = ?"

  val configSQL = "select field,turn from tvtest_streaming_fields where enabled = 1 and streaming_key = ? order by turn"

  val descSQL = "select COLUMN_NAME from information_schema.COLUMNS where table_name = ? and table_schema = 'tvtest_streaming';"

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * 获取最初的配置，即创建任务的主要配置如kafka、topic、zookeeper等
    * @param streamingKey 任务的唯一标示
    * @return 返回配置样例类对象
    */
  def findStreamingKeyConfig(streamingKey: String): StreamingKeyConfig = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    var streamingKeyConfig: StreamingKeyConfig = null
    try {
      conn = MysqlManager.getMysqlManager.getConnection
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
      case e: Exception => logger.error("findStreamingKeyConfig error..." + e)
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

  /**
    * 获取需要的字段
    * @param streamingKey 任务唯一标示
    * @return 返回这些需要的字段keys，并携带顺序turn
    */
  def findStreamingKeyFileldsConfig(streamingKey: String): ListBuffer[(String, Int)] ={
    val fieldsList: ListBuffer[(String,Int)] = new ListBuffer[(String, Int)]()
    var conn: Connection = null
    var ps: PreparedStatement = null

    try{
      conn = MysqlManager.getMysqlManager.getConnection
      ps = conn.prepareStatement(configSQL)
      ps.setString(1, streamingKey)
      val res = ps.executeQuery()
      while(res.next()){
        fieldsList.append((res.getString("field"), res.getInt("turn")))
      }
    }catch{
      case e:Exception => logger.error("findStreamingKeyFileldsConfig error..." + e)
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

  /**
    * 批量插入结果
    * @param y 每个DStream的每个ADD的每个partition，所以是迭代器
    * @param tableName 目标table
    */
  def insertBatch(y: Iterator[ListBuffer[String]], tableName:String, insertSQL: String, fields: ListBuffer[(String, Int)]): Unit ={
    var conn: Connection = null
    var ps: PreparedStatement = null
    try{
      conn = MysqlManager.getMysqlManager.getConnection
      conn.setAutoCommit(false)
      var arr = ArrayBuffer[String]()
      for(i <- 0 until fields.length){
        arr += fields(i)._1
      }
      ps = conn.prepareStatement(insertSQL.format(tableName, arr.mkString(",")))
      for(i <- y){
        for(j <- 1 to i.length){
          ps.setString(j, i(j-1))
        }
        ps.addBatch()
      }
      ps.executeBatch()
      conn.commit()
    }catch{
      case e:Exception => logger.error("insert into result error..." + e)
    }finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
  }

  /**
    *
    */
  def descDestinationTable(table: String): ArrayBuffer[String] ={
    var res = new ArrayBuffer[String]()
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
      ps = conn.prepareStatement(descSQL)
      ps.setString(1, table)
      val rows = ps.executeQuery()
      while(rows.next()){
        res += rows.getString("COLUMN_NAME")
      }
    }catch{
      case e:Exception => logger.error("desc table error...")
    }finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }
    res
  }

  /**
    * 修改目标表结构
    * @param tableName 目标表
    * @param field 需要添加的字段名
    * @return
    */
  def alterTable(tableName: String, field: String) = {
    var conn: Connection = null
    var ps: PreparedStatement = null

    try{
      conn = MysqlManager.getMysqlManager.getConnection
      ps = conn.prepareStatement(alterSQL.format(tableName, field))
      ps.execute()
    }catch{
      case e:Exception => logger.error("fail to alter result table..." + e)
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
