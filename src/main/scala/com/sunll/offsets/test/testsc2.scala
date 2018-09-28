package com.sunll.offsets.test

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 测试checkpoint，可以做到任务中断不丢失数据（很小可能丢失，会存在小可能的重复）
  */
object testsc2 {
  def main(args: Array[String]): Unit = {
    val checkpoint = "e:/checkpoint"
    val ssc = StreamingContext.getOrCreate(checkpoint, createContext)
    ssc.start()
    ssc.awaitTermination()
  }

  def createContext(): StreamingContext ={
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(4))
    val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> "103.26.158.194:9092,103.26.158.195:9092,103.26.158.196:9092,103.26.158.197:9092,103.26.158.198:9092,103.26.158.199:9092,103.26.158.200:9092",
      "group.id" -> "test_kafka",
      "zookeeper.connect" -> "103.26.158.64:2181,103.26.158.65:2181,103.26.158.66:2181",
      "auto.offset.reset" -> "largest")
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("tvtest.sunliangliang"))
    ssc.checkpoint("e:/checkpoint")
    kafkaStream.foreachRDD(rdd => rdd.checkpoint())
    kafkaStream.map(_._2).print()
    ssc
  }
}