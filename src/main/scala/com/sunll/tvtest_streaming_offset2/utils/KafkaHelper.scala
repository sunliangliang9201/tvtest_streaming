package com.sunll.tvtest_streaming_offset2.utils

import com.sunll.tvtest_streaming_offset2.storage.{MysqlDao, RedisDao}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * 用来根据程序参数以及mysql中配置来创建kafkaDStream
  * 需要注意的是：想一下实际生产中，如果不求exactly once的情况那么无需管这里的offset
  * 但是如果要求了exactly once的话，肯定要读取自己管理的offset，但是程序第一次运行的时候应该怎么办？是消费最新的还是从offset=0开始的？？
  * 如果我们程序不是第一次运行了，那么就太简单了因为我们管理的offset已经存储了程序上次停止时的offset了，这次启动到底是不是exactly once我们自己决定就好了。
  *
  * @author sunliangliang 2018-10-23 https://github.com/sunliangliang9201/tvtest_streaming
  * @version 3.0
  */
object KafkaHelper {

  /**
    * 如果选择了使用自己管理的offset启动kafkaDStream
    * @param groupID 消费组
    * @param ssc sparkContext
    * @param kafkaParams 参数
    * @param topicSet topics
    * @return kafkaDStream：InputDStream
    */
  def getKafkaDStreamFromOffset(groupID: String, ssc: StreamingContext, kafkaParams: Map[String, String], topicSet: Set[String]): InputDStream[(String, String)] = {
    var fromOffset: Map[TopicAndPartition, Long] = RedisDao.getOffsetFromRedis(groupID)
    if(fromOffset.isEmpty){
      println("**************from mysql************************")
      fromOffset = MysqlDao.getOffsetFromMysql(groupID)
    }else{
      println("***************from redis***********************")
    }
    val mesageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffset, mesageHandler)
  }
}
