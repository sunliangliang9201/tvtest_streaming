package com.sunll.offsets.main

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 虽然Direct的方式可以保证精确一次的消费数据，但是如果出现问题如中断程序，断电等问题，就都得用到checkpoint，但是checkpoint
  * 自身也存在局限，所以最好是自己管理offsets
  */
object TvTestOffsetMain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(4))
    val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> "103.26.158.194:9092,103.26.158.195:9092,103.26.158.196:9092,103.26.158.197:9092,103.26.158.198:9092,103.26.158.199:9092,103.26.158.200:9092",
      "group.id" -> "test_kafka",
      "zookeeper.connect" -> "103.26.158.64:2181,103.26.158.65:2181,103.26.158.66:2181",
      "auto.offset.reset" -> "largest")
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("tvtest.sunliangliang"))
    kafkaStream.foreachRDD(rdd => {
      rdd.map(_._2)
    })
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
