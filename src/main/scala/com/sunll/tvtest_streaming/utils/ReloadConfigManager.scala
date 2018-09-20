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
class ReloadConfigManager extends Serializable {
  //注意：序列化的话，里面的类变量必须是可序列化的
  @volatile var fields: ListBuffer[(String, Int)] = null

  @volatile var insertSQL: String = ""

  /**
    * 启动一个线程，每个固定时间舒心配置，设置为守护进程
    * @param flushTime 刷新间隔时间
    * @param streamingKey 任务唯一标示
    * @param streamingKeyConfig 全局配置
    */
  def init(flushTime: Int, streamingKey: String, streamingKeyConfig: StreamingKeyConfig)={
    fields = MysqlDao.findStreamingKeyFileldsConfig(streamingKey)
    val thread = new Thread(){
      override def run(): Unit = {
        while(true){
          println("上一次的fields" + fields)
          reloadFields(streamingKey)
          println("下一次的fields" + fields)
          reloadTableandInsertSQL(streamingKey, streamingKeyConfig)
          Thread.sleep(flushTime)
        }
      }
    }
    thread.setDaemon(true)
    thread.start()
    println("线程启动！！！！")
  }

  /**
    * 保证fields变量的改变及时可见性和顺序性，但不保证修改安全性（无所谓，这里只有一个线程在改）
    * @return 新的fileds
    */
  def getFields: ListBuffer[(String, Int)] = {
    this.fields
  }

  /**
    * 返回insertsql
    * @return insertsql
    */
  def getInsertSQL(): String ={
    this.insertSQL
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
    println("插入sql" + insertSQL)
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
