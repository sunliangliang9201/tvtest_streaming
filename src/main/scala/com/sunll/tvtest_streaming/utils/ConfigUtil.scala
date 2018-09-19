package com.sunll.tvtest_streaming.utils

import com.typesafe.config.{ConfigFactory, Config}

/**
  * get configuration from config properties
  *
  * @author sunliangliang 2018-09-15
  * @version 1.0
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
