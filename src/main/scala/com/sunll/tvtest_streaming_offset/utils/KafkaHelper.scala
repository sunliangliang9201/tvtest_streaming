package com.sunll.tvtest_streaming_offset.utils

import com.sunll.tvtest_streaming_offset.storage.MysqlDao
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * desc
  *
  * @author sunliangliang 2018-09-28 https://github.com/sunliangliang9201/tvtest_streaming
  * @version 2.0
  */
object KafkaHelper {

  def getKafkaDStreamFromOffset(groupID: String, ssc: StreamingContext, kafkaParams: Map[String, String], topicSet: Set[String]): InputDStream[(String, String)] = {
    val fromOffset = MysqlDao.getOffset(groupID)
    val mesageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffset, mesageHandler)
  }
}
