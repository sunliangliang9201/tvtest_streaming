package com.sunll.tvtest_streaming.formator

import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

trait LogFormator extends Serializable{

  def format(logStr: String, fields: ListBuffer[(String, Int)]): ListBuffer[String]
}
