import kafka.serializer.StringDecoder
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils

object testsc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> "103.26.158.194:9092,103.26.158.195:9092,103.26.158.196:9092,103.26.158.197:9092,103.26.158.198:9092,103.26.158.199:9092,103.26.158.200:9092",
      "group.id" -> "test_kafka",
      "zookeeper.connect" -> "103.26.158.64:2181,103.26.158.65:2181,103.26.158.66:2181",
      "auto.offset.reset" -> "largest")
    val kafkaStreams = (1 to 3).map(_ => {
      KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Map("tvtest.sunliangliang" -> 1), StorageLevel.MEMORY_AND_DISK_SER).map(_._2)
    })
    ssc.union(kafkaStreams).repartition(1).print()
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
