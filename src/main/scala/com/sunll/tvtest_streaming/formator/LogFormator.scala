package com.sunll.tvtest_streaming.formator

import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
  * the triat of formator, and the obstrct func format.
  * @author sunliangliang 2018-09-17
  * @version 1.0
  *
  */

trait LogFormator extends Serializable{

  def format(logStr: String, fields: ListBuffer[(String, Int)]): ListBuffer[String]
}
