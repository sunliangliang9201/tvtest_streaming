package com.sunll.tvtest_streaming.utils

import com.sunll.tvtest_streaming.storage.MysqlDao
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
/**
  * flush the config in mysql
  * @author sunliangliang 2018-09-15
  * @version 1.0
  */
object ReloadConfigManager {
  val logger = LoggerFactory.getLogger(this.getClass)
  var thread: Thread = null
  @volatile var fields: ListBuffer[(String, Int)] = new ListBuffer[(String, Int)]()
  @volatile var insertSQL = ""
  def init(flushTime: Int, streamingKey: String)={
    fields = MysqlDao.findStreamingKeyFileldsConfig(streamingKey)
    thread = new Thread(){
      override def run(): Unit = {
        while(true){
          reloadFields(streamingKey)
          reloadTableandInsertSQL(streamingKey)
          logger.info("success reload the fields config and reload result table and the insert sql " + fields)
          Thread.sleep(flushTime)
        }
      }
    }
    start_thread()
  }

  def getFields: ListBuffer[(String, Int)] = {
    this.fields
  }

  def start_thread(): Unit ={
    thread.setDaemon(true)
    thread.start()
  }

  private def reloadFields(streamingKey: String): Unit ={
    val tmpFieldsList = MysqlDao.findStreamingKeyFileldsConfig(streamingKey)
    fields = tmpFieldsList
  }
  private def reloadTableandInsertSQL(streamingKey: String): Unit ={
    insertSQL = "insert into %s(%s) values("
  }
}
