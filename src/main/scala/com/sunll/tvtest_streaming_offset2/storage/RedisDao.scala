package com.sunll.tvtest_streaming_offset2.storage

import java.util

import com.sunll.tvtest_streaming_offset2.utils.{ConfigUtil, Constants}
import kafka.common.TopicAndPartition
import org.apache.spark.streaming.kafka.OffsetRange
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster, JedisPool}

import scala.collection.JavaConverters._


/**
  * redis has many uses, such as cache so much data that increat greatly
  *
  * @author sunliangliang 2018-10-23 https://github.com/sunliangliang9201/tvtest_streaming
  * @version 3.0
  */
object RedisDao {

  val redis_host1 = ConfigUtil.getConf.get.getString("redis_host1")
  val redis_host2 = ConfigUtil.getConf.get.getString("redis_host2")

  val redis_port0 = ConfigUtil.getConf.get.getInt("redis_port0")
  val redis_port1 = ConfigUtil.getConf.get.getInt("redis_port1")
  val redis_port2 = ConfigUtil.getConf.get.getInt("redis_port2")
  val redis_port3 = ConfigUtil.getConf.get.getInt("redis_port3")
  val redis_port4 = ConfigUtil.getConf.get.getInt("redis_port4")
  val redis_port5 = ConfigUtil.getConf.get.getInt("redis_port5")

  val redis_pass = ConfigUtil.getConf.get.getString("redis_pass")

  val redis_expire_time = ConfigUtil.getConf.get.getInt("redis_expire_time")

  val jedisCluster = {
    val nodesSet: util.HashSet[HostAndPort] = new util.HashSet[HostAndPort]()
    nodesSet.add(new HostAndPort(redis_host1, redis_port0))
    nodesSet.add(new HostAndPort(redis_host1, redis_port1))
    nodesSet.add(new HostAndPort(redis_host1, redis_port2))
    nodesSet.add(new HostAndPort(redis_host2, redis_port3))
    nodesSet.add(new HostAndPort(redis_host2, redis_port4))
    nodesSet.add(new HostAndPort(redis_host2, redis_port5))
    val clients = new JedisCluster(nodesSet)
    clients
  }
  /**
    * 缓存（更像是存储）消费组的topics的partitions的offsets
    * @param groupID 消费组
    * @param arr 包含topic、partitions、offset的array
    */
  def cacheOffset(groupID: String, arr: Array[OffsetRange]): Unit ={
    var key:String = null
    try{
      for(i <- arr){
        key = groupID + Constants.REDIS_SPLITER + i.topic + Constants.REDIS_SPLITER + i.partition
        if(!jedisCluster.exists(key)){
          jedisCluster.set(key, i.untilOffset.toString)
          jedisCluster.expire(key, redis_expire_time)
        }else{
          jedisCluster.set(key, i.untilOffset.toString)
        }
      }
    }catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 从redis中获取offset
    * @param groupID 消费组
    * @return 返回用于创建kafkaDStream的Map
    */
  def getOffsetFromRedis(groupID: String): Map[TopicAndPartition, Long] ={
    val keys: util.HashSet[String] = new util.HashSet[String]()
    var res: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()
    var tmp: TopicAndPartition = null
    var conn: Jedis = null
    var jp: JedisPool = null
    try{
      val clusterNodes = jedisCluster.getClusterNodes
      for(i <- clusterNodes.keySet().asScala){
        jp = clusterNodes.get(i)
        conn = jp.getResource()
        try{
          keys.addAll(conn.keys(groupID + "|" + "*"))
        }catch {
          case e: Exception => e.printStackTrace()
        }finally {
          conn.close()
        }
      }
      for(j: String <- keys.asScala){
        tmp = new TopicAndPartition(j.split("\\|")(1), j.split("\\|")(2).toInt)
        res += (tmp -> jedisCluster.get(j).toLong)
      }
    }catch {
      case e: Exception => e.printStackTrace()
    }
    res
  }

  /**
    * 测试
    * @param args
    */
  def main(args: Array[String]): Unit = {
    println(getOffsetFromRedis("dt-spark-streaming"))
  }
}
