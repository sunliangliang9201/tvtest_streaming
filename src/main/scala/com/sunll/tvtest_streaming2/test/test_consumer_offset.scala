package com.sunll.tvtest_stereaming2.test

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * desc
  *
  * @author sunliangliang 2018-09-28 https://github.com/sunliangliang9201/tvtest_streaming
  * @version 1.0
  */
object test_consumer_offset {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestConsumerOffset").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(4))
    val kafkaParams = Map("metadata.broker.list" -> "103.26.158.194:9092,103.26.158.195:9092,103.26.158.196:9092,103.26.158.197:9092,103.26.158.198:9092,103.26.158.199:9092,103.26.158.200:9092",
    "group.id" -> "hello",
    "auto.offset.reset" -> "largest")
    val topics = Set("tvtest.sunliangliang")
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc,kafkaParams,topics).map(_._2)
    kafkaStream.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
