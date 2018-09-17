package com.sunll.tvtest_streaming.utils

import com.sunll.tvtest_streaming.model.StreamingKeyConfig
import com.sunll.tvtest_streaming.storage.MysqlDao
import org.slf4j.LoggerFactory
import util.control.Breaks._
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

  /**
    * 启动一个线程，每个固定时间舒心配置，设置为守护进程
    * @param flushTime 刷新间隔时间
    * @param streamingKey 任务唯一标示
    * @param streamingKeyConfig 全局配置
    */
  def init(flushTime: Int, streamingKey: String, streamingKeyConfig: StreamingKeyConfig)={
    fields = MysqlDao.findStreamingKeyFileldsConfig(streamingKey)
    thread = new Thread(){
      override def run(): Unit = {
        while(true){
          reloadFields(streamingKey)
          reloadTableandInsertSQL(streamingKey, streamingKeyConfig)
          logger.info("success reload the fields config and reload result table and the insert sql " + fields)
          Thread.sleep(flushTime)
        }
      }
    }
    start_thread()
  }

  /**
    * 保证fields变量的改变及时可见性和顺序性，但不保证修改安全性（无所谓，这里只有一个线程在改）
    * @return 新的fileds
    */
  def getFields: ListBuffer[(String, Int)] = {
    this.fields
  }

  /**
    * 设置守护进程并启动
    */
  def start_thread(): Unit ={
    thread.setDaemon(true)
    thread.start()
  }

  /**
    * 重新刷新字段的方法
    * @param streamingKey 任务唯一标示
    */
  private def reloadFields(streamingKey: String): Unit ={
    val tmpFieldsList = MysqlDao.findStreamingKeyFileldsConfig(streamingKey)
    fields = tmpFieldsList
  }

  /**
    * 刷新insert sql，并且根据字段改变而修改result table的表结构（尽量不修改）
    * @param streamingKey 任务唯一标示
    * @param streamingKeyConfig 全局配置
    */
  private def reloadTableandInsertSQL(streamingKey: String, streamingKeyConfig: StreamingKeyConfig): Unit ={
    var tmp = "insert into %s(%s) values("
    var tmpList = ListBuffer.fill(fields.length)("?")
    tmp += tmpList.mkString(",")
    tmp += ")"
    insertSQL = tmp
    val res_fields = MysqlDao.descDestinationTable(streamingKeyConfig.tableName)
    if(res_fields.length - 1 != fields.length){
      for(i <- 0 until fields.length){
        breakable{
          for(j <- 0 until res_fields.length){
            if(fields(i)._1.equals(res_fields(j))){
              break()
            }
          }
          MysqlDao.alterTable(streamingKeyConfig.tableName, fields(i)._1)
        }
      }
    }
  }

  /**
    * 测试main
    * @param args
    */
  def main(args: Array[String]): Unit = {

  }
}