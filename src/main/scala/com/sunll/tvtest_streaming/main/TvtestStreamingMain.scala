package com.sunll.tvtest_streaming.main

import org.slf4j.LoggerFactory

/**
  * tvtest_streaming start main object, start the project and get the config for paralize compute.
 *
  * @author sunliangliang 2018-09-10 https://github.com/sunliangliang9201/tvtest_streaming
  * @version 1.0
  *
  */
object TvtestStreamingMain {


  def main(args: Array[String]): Unit = {
    //初始化日志对象
    val logger = LoggerFactory.getLogger(this.getClass)
    val streamingKey = args(0)
    val streamingIntervalTime = args(1)
    val streamingKeyConfig =
  }
}
