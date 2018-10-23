package com.sunll.tvtest_streaming_offset2.storage

import com.sunll.tvtest_streaming_offset2.utils.ConfigUtil
import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.OffsetRange
import redis.clients.jedis.Jedis

/**
  * redis has many uses, such as cache so much data that increat greatly
  *
  * @author sunliangliang 2018-10-23 https://github.com/sunliangliang9201/tvtest_streaming
  * @version 3.0
  */
object RedisDao {

  val redis_host = ConfigUtil.getConf.get.getString("redis_host")

  val redis_port = ConfigUtil.getConf.get.getInt("redis_port")

  val redis_pass = ConfigUtil.getConf.get.getString("redis_pass")

  /**
    * 缓存（更像是存储）消费组的topics的partitions的offsets
    * @param groupID 消费组
    * @param arr 包含topic、partitions、offset的array
    */
  def cacheOffset(groupID: String, arr: Array[OffsetRange]): Unit ={
    val client = new Jedis(redis_host, redis_port)
    client.auth(redis_pass)

    println(client.ping())
    client.close()
  }

  /**
    * 从redis中获取offset
    * @param groupID 消费组
    * @return 返回用于创建kafkaDStream的Map
    */
  def getOffsetFromRedis(groupID: String): Map[TopicAndPartition, Long] ={
    val client = new Jedis(redis_host, redis_port)
    client.auth(redis_pass)

    Map[TopicAndPartition, Long]()
  }

  /**
    * 测试
    * @param args
    */
  def main(args: Array[String]): Unit = {

  }
}
