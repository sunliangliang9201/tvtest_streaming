package com.sunll.tvtest_streaming2.utils

import com.typesafe.config.{Config, ConfigFactory}

/**
  * desc
  *
  * @author sunliangliang 2018-09-28 https://github.com/sunliangliang9201/tvtest_streaming
  * @version 1.0
  */
object ConfigUtil {

  val conf = ConfigFactory.load("application.properties")

  /**
    * 获取配置文件中的信息
    * @return
    */
  def getConf(): Option[Config]={
    Some(conf)
  }
}
