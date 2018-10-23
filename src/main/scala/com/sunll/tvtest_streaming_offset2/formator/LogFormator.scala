package com.sunll.tvtest_streaming_offset.formator

import scala.collection.mutable.{ListBuffer, Map}

/**
  * the triat of formator, and the obstrct func format.
  * @author sunliangliang 2018-10-19
  * @version 2.0
  *
  */

trait LogFormator extends Serializable{
  /**
    * 待实现的方法
    * @param logStr 原始日志
    * @return 结果
    */
  def format(logStr: String, buff: Array[String], fields: Map[String, ListBuffer[(String, Int)]]): (String, ListBuffer[String])

}
