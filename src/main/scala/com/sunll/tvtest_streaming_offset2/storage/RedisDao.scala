package com.sunll.tvtest_streaming_offset.storage

import com.sunll.tvtest_streaming_offset.utils.ConfigUtil
import redis.clients.jedis.Jedis

/**
  * redis has many uses, such as cache so much data that increat greatly
  *
  * @author sunliangliang 2018-10-23 https://github.com/sunliangliang9201/tvtest_streaming
  * @version 2.0
  */
object RedisDao {

  val redis_host = ConfigUtil.getConf.get.getString("redis_host")

  val redis_port = ConfigUtil.getConf.get.getInt("redis_port")

  val redis_pass = ConfigUtil.getConf.get.getString("redis_pass")

  def cacheOffset(): Unit ={
    val client = new Jedis(redis_host, redis_port)
    client.auth(redis_pass)
    println(client.ping())
  }

  /**
    * 测试
    * @param args
    */
  def main(args: Array[String]): Unit = {
    cacheOffset()
  }
}
