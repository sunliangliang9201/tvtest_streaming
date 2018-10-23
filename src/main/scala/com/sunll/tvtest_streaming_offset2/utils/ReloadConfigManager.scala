package com.sunll.tvtest_streaming_offset2.utils

import com.sunll.tvtest_streaming_offset2.model.StreamingKeyConfig
import com.sunll.tvtest_streaming_offset2.storage.{MysqlDao, RedisDao}

import scala.collection.mutable.{ListBuffer, Map}

/**
  * flush the config in mysql
  * @author sunliangliang 2018-10-23
  * @version 3.0
  */
class ReloadConfigManager extends Serializable {
  //注意：序列化的话，里面的类变量必须是可序列化的
  @volatile var fieldsMap: Map[String, ListBuffer[(String, Int)]] = null
  @volatile var insertSQL: Map[String, String] = Map[String, String]()

  /**
    * 启动一个线程，每个固定时间舒心配置，设置为守护进程
    * @param flushTime 刷新间隔时间
    * @param streamingKey 任务唯一标示
    * @param streamingKeyConfig 全局配置
    */
  def init(flushTime: Int, streamingKey: String, streamingKeyConfig: StreamingKeyConfig)={
    fieldsMap = MysqlDao.findStreamingKeyFileldsConfig(streamingKey)
    val thread = new Thread(){
      override def run(): Unit = {
        while(true){
          reloadFields(streamingKey)
          reloadTableandInsertSQL(streamingKey, streamingKeyConfig)
          MysqlDao.updateOffset(streamingKeyConfig.groupID, RedisDao.getOffsetFromRedis(streamingKeyConfig.groupID))
          Thread.sleep(flushTime)
        }
      }
    }
    thread.setDaemon(true)
    thread.start()
  }

  /**
    * 保证fields变量的改变及时可见性和顺序性，但不保证修改安全性（无所谓，这里只有一个线程在改）
    * @return 新的fileds
    */
  def getFields: Map[String, ListBuffer[(String, Int)]] = {
    this.fieldsMap
  }

  /**
    * 返回insertsql
    * @return insertsql
    */
  def getInsertSQL(): Map[String, String] ={
    this.insertSQL
  }

  /**
    * 重新刷新字段的方法
    * @param streamingKey 任务唯一标示
    */
  private def reloadFields(streamingKey: String): Unit ={
    val tmpFieldsMap = MysqlDao.findStreamingKeyFileldsConfig(streamingKey)
    fieldsMap = tmpFieldsMap
  }

  /**
    * 刷新insert sql，并且根据字段改变而修改result table的表结构（尽量不修改）
    * @param streamingKey 任务唯一标示
    * @param streamingKeyConfig 全局配置
    */
  private def reloadTableandInsertSQL(streamingKey: String, streamingKeyConfig: StreamingKeyConfig): Unit ={
    for(i <- fieldsMap.keySet){
      var tmp = "insert into %s(%s) values("
      var tmpList = ListBuffer.fill(fieldsMap(i).length)("?")
      tmp += tmpList.mkString(",")
      tmp += ")"
      insertSQL(i) = tmp
    }
    //动态改变表结果的功能删除
//    val res_fields = MysqlDao.descDestinationTable(streamingKeyConfig.tableName)
//    if(res_fields.length - 1 != fields.length){
//      for(i <- 0 until fields.length){
//        breakable{
//          for(j <- 0 until res_fields.length){
//            if(fields(i)._1.equals(res_fields(j))){
//              break()
//            }
//          }
//          MysqlDao.alterTable(streamingKeyConfig.tableName, fields(i)._1)
//        }
//      }
//    }
  }

  /**
    * 测试main
    * @param args
    */
  def main(args: Array[String]): Unit = {

  }
}
