import kafka.serializer.StringDecoder

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}

/**
  * 测试kafka的Direct消费方式，通过将读取zookeeper中保存的offset值来读取数据，如果没有保存就读取largest最新的数据
  * 每次处理一批数据后都将当前的offset值存入zookeeper，防止程序中断时导致丢失数据
  * 这种Direct的消费方式保证了exactly once！ 当然了所有保证都不能百分百。
  */
object testsc2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> "103.26.158.194:9092,103.26.158.195:9092,103.26.158.196:9092,103.26.158.197:9092,103.26.158.198:9092,103.26.158.199:9092,103.26.158.200:9092",
      "group.id" -> "test_kafka2",
      "zookeeper.connect" -> "103.26.158.64:2181,103.26.158.65:2181,103.26.158.66:2181",
      "auto.offset.reset" -> "largest")
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("tvtest.sunliangliang")).map(_._2)
    kafkaStream.print()
    ssc.start()
    ssc.awaitTermination()
}
}