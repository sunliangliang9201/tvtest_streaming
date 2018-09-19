package com.sunll.tvtest_streaming.formator

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * the triat of formator, and the obstrct func format.
  * @author sunliangliang 2018-09-17
  * @version 1.0
  *
  */

trait LogFormator extends Serializable{
  /**
    * 待实现的方法
    * @param logStr 原始日志
    * @return 结果
    */
  def format(logStr: String, buff: Array[String]): ListBuffer[String]

}
