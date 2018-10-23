package com.sunll.tvtest_streaming_offset.utils

import com.typesafe.config.{Config, ConfigFactory}

/**
  * get configuration from config properties
  *
  * @author sunliangliang 2018-10-19
  * @version 2.0
  */
object ConfigUtil {

  val conf = ConfigFactory.load("application.properties")

  /**
    * 从配置文件中获取某个配置项
    * @return
    */
  def getConf: Option[Config] = {
    Some(conf)
  }
}
