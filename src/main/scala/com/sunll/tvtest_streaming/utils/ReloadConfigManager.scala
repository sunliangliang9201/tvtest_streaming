package com.sunll.tvtest_streaming.utils

import com.sunll.tvtest_streaming.storage.MysqlDao

import scala.collection.mutable.ListBuffer
/**
  * flush the config in mysql
  * @author sunliangliang 2018-09-15
  * @version 1.0
  */
object ReloadConfigManager {


  var thread: Thread = null
  @volatile var fields: ListBuffer[(String, Int)] = new ListBuffer[(String, Int)]()

  def init(flushTime: Int, streamingKey: String)={
    fields = MysqlDao.findStreamingKeyFileldsConfig(streamingKey)
    println("start")
    thread = new Thread(){
      override def run(): Unit = {
        while(true){
          reload(streamingKey)
          println(fields)
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

  private def reload(streamingKey:String): Unit ={
    val tmpFieldsList = MysqlDao.findStreamingKeyFileldsConfig(streamingKey)
    fields = tmpFieldsList
  }
}
