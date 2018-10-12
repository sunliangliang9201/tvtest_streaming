package com.sunll.tvtest_streaming2.storage

import java.sql.{Connection, PreparedStatement}

import com.sunll.tvtest_streaming2.model.StreamingKeyConfig

/**
  * we use this to get config information and save result, after masterly, we will use redis to
  * instead mysql
  *
  * @author sunliangliang 2018-09-28 https://github.com/sunliangliang9201/tvtest_streaming
  * @version 2.0
  */
object MySQLDao {

  val streamingKeyConfigSQL = "select streaming_key, app_name, driver_cores, formator, topics, group_id, table_name, fields, broker_list from realtime_streaming_config where streaming_key = ?"

  def getStreamingConfig(key: String): StreamingKeyConfig = {
    var conn: Connection = null
    var ps:PreparedStatement = null
    var streamingKeyConfig: StreamingKeyConfig = null
    try{
      conn = MySQLPoolManager.getMySQLPool().getConnection()
      ps = conn.prepareStatement(streamingKeyConfigSQL)
      ps.setString(1, key)
      val res = ps.executeQuery()
      res.last()
      val count = res.getRow
      if(count != 0){
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
      }
    }catch {
      case e:Exception => e.printStackTrace()
        null
    }finally {
      if(ps != null){
        ps.close()
      }
      if(conn != null){
        conn.close()
      }
    }
    streamingKeyConfig
  }

  def main(args: Array[String]): Unit = {
    println(getStreamingConfig("TvTest2"))
  }
}
