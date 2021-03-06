package com.sunll.tvtest_streaming_offset2.storage

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.sunll.tvtest_streaming_offset2.model.StreamingKeyConfig
import com.sunll.tvtest_streaming_offset2.utils.ConfigUtil
import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.OffsetRange
import org.slf4j.LoggerFactory
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}

/**
  * mysql funcs object
  * @author sunliangliang 2018-10-23
  * @version 3.0
  */
object MysqlDao {

  val alterSQL = "alter table %s add %s varchar(255) default null"

  val streamingSQL = "select streaming_key, app_name, driver_cores, formator, topics, group_id, table_name, fields, broker_list from realtime_streaming_config where streaming_key = ?"

  val configSQL = "select appkey,field,turn from tvtest_streaming_fields where enabled = 1 and streaming_key = ? order by turn"

  val descSQL = "select COLUMN_NAME from information_schema.COLUMNS where table_name = ? and table_schema = 'tvtest_streaming';"

  val offsetsSQL = "select topic_name,partition_num,offset from tvtest_streaming_offset where group_name = ? and enabled =1"

  val updateOffsetSQL = "update tvtest_streaming_offset set offset = ? where group_name = ? and topic_name = ? and partition_num = ?"

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
    * 获取需要的字段，根据appkey区分，防区map中
    * @param streamingKey 任务唯一标示
    * @return 返回这些需要的字段keys，并携带顺序turn
    */
  def findStreamingKeyFileldsConfig(streamingKey: String): Map[String, ListBuffer[(String, Int)]] ={
    val fieldsMap: Map[String, ListBuffer[(String, Int)]] = Map[String, ListBuffer[(String, Int)]]()
    var conn: Connection = null
    var ps: PreparedStatement = null

    try{
      conn = MysqlManager.getMysqlManager.getConnection
      ps = conn.prepareStatement(configSQL)
      ps.setString(1, streamingKey)
      val res = ps.executeQuery()
      while(res.next()){
        if(fieldsMap.contains(res.getString("appkey"))){
          fieldsMap(res.getString("appkey")).append((res.getString("field"), res.getInt("turn")))
        }else{
          fieldsMap(res.getString("appkey")) = ListBuffer()
          fieldsMap(res.getString("appkey")).append((res.getString("field"), res.getInt("turn")))
        }
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
    fieldsMap
  }

  /**
    * 批量插入结果，根据appkey插入不同的结果表
    * @param y 每个DStream的每个ADD的每个partition，所以是迭代器
    * @param tableName 目标table
    */
  def insertBatch(y: Iterator[(String, ListBuffer[String])], tableName:String, insertSQL: Map[String, String], fieldsMap: Map[String, ListBuffer[(String, Int)]]): Unit ={
    var conn: Connection = null
    var ps: PreparedStatement = null
    val tableMap = scala.collection.mutable.Map[String, PreparedStatement]()
    if(!y.isEmpty){
      try{
        conn = MysqlManager.getMysqlManager.getConnection
        conn.setAutoCommit(false)
        for(i <- y){
          var arr = ArrayBuffer[String]()
          for(j <- 0 until fieldsMap(i._1).length){
            arr += fieldsMap(i._1)(j)._1
          }
          if (i._1 != "-" &&  !tableMap.contains(i._1)){
            tableMap(i._1) = conn.prepareStatement(insertSQL(i._1).format(tableName + "." + i._1 + "_" + "stat", arr.mkString(",")))
            for(j <- 1 to i._2.length){
              tableMap(i._1).setString(j, i._2(j-1))
            }
            tableMap(i._1).addBatch()
          }else if(i._1 != "-"){
            for(j <- 1 to i._2.length){
              tableMap(i._1).setString(j, i._2(j-1))
            }
            tableMap(i._1).addBatch()
          }
        }
        for(i <- tableMap.values){
          i.executeBatch()
        }
        conn.commit()
      }catch{
        case e:Exception => logger.error("insert into result error..." + e)
      }finally {
        for(i <- tableMap.values){
          if(i != null)
            i.close()
        }
        if (conn != null) {
          conn.close()
        }
      }
    }
  }

  /**
    *用来动态修改结果表表结构的函数,暂时没有用到
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
      case e: Exception => e.printStackTrace()
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
    * 获取每个topic的每个partition的offset值，用来创建kafkaDStream
    * @param groupID 消费组
    * @return 创建kafkaDStream所需的Map
    */
  def getOffsetFromMysql(groupID: String): scala.collection.immutable.Map[TopicAndPartition, Long] ={
    var conn: Connection = null
    var ps: PreparedStatement = null
    var result: scala.collection.immutable.Map[TopicAndPartition, Long] = scala.collection.immutable.Map[TopicAndPartition, Long]()
    var topicAndPartition: TopicAndPartition = null
    try{
      conn = MysqlManager.getMysqlManager.getConnection
      ps = conn.prepareStatement(offsetsSQL)
      ps.setString(1, groupID)
      val rs = ps.executeQuery()
      while(rs.next()){
        topicAndPartition = TopicAndPartition(rs.getString("topic_name"),rs.getInt("partition_num"))
        result += (topicAndPartition -> rs.getLong("offset"))
      }
    }catch {
      case e: Exception => e.printStackTrace()
    }finally {
      if(ps != null){
        ps.close()
      }
      if(conn != null){
        conn.close()
      }
    }
    result
  }

  /**
    * 将最新的消费offset更新到mysql的offset配置表中
    * @param arr 从kafkaDStream中获取的topic+partition+offset数值
    */
  def updateOffset(groupID: String, arr: Array[OffsetRange]): Unit ={
    var conn: Connection = null
    var ps: PreparedStatement = null
    try{
      conn = MysqlManager.getMysqlManager.getConnection
      conn.setAutoCommit(false)
      ps = conn.prepareStatement(updateOffsetSQL)
      for(i <- arr){
        ps.setLong(1, i.untilOffset)
        ps.setString(2, groupID)
        ps.setString(3, i.topic)
        ps.setInt(4, i.partition)
        ps.addBatch()
      }
      ps.executeBatch()
      conn.commit()
    }catch {
      case e: Exception => e.printStackTrace()
    }finally {
      if(ps != null){
        ps.close()
      }
      if(conn != null){
        conn.close()
      }
    }
  }

  /**
    * 重载的更新mysql中offset的方法，跟updateOffset(groupID: String, arr: Array[OffsetRange])不同的是从redis中获取load近mysql
    * @param groupID 消费组
    * @param offsetsMap topic+partition+offset的消费信息
    */
  def updateOffset(groupID: String, offsetsMap: scala.collection.immutable.Map[TopicAndPartition, Long]): Unit ={
    val arr: ArrayBuffer[OffsetRange] = ArrayBuffer[OffsetRange]()
    var tmpOffsetRange: OffsetRange = null
    for(i <- offsetsMap.keySet){
      tmpOffsetRange = OffsetRange.create(i.topic, i.partition, 0L, offsetsMap(i))
      arr += tmpOffsetRange
    }
    updateOffset(groupID, arr.toArray)
  }
  /**
    * 测试
    * @param args
    */
  def main(args: Array[String]): Unit = {
    println(getOffsetFromMysql("dt-spark-streaming").values.toSet.contains(0))

  }
}
