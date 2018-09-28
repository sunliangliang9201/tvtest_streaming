package com.sunll.tvtest_streaming.test

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 测试kafka的Receiver消费方式，通过StorageLevel.MEMORY_AND_DISK_SER这个参数来搞定at least once问题，但是很小可能存在重复读的问题
  *
  */
object testsc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> "103.26.158.194:9092,103.26.158.195:9092,103.26.158.196:9092,103.26.158.197:9092,103.26.158.198:9092,103.26.158.199:9092,103.26.158.200:9092",
      "group.id" -> "test_kafka",
      "zookeeper.connect" -> "103.26.158.64:2181,103.26.158.65:2181,103.26.158.66:2181",
      "auto.offset.reset" -> "largest")
    val kafkaStreams = (1 to 3).map(_ => {
      KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Map("tvtest.sunliangliang" -> 1), StorageLevel.MEMORY_AND_DISK_SER)
    })
    ssc.union(kafkaStreams).repartition(1).print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
