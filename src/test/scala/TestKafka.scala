import com.sunll.tvtest_streaming.formator.LogFormator
import com.sunll.tvtest_streaming.storage.MysqlDao
import com.sunll.tvtest_streaming.utils.{ConfigUtil, Constants, ReloadConfigManager}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * desc
  *
  * @author sunliangliang 2019-02-22 https://github.com/sunliangliang9201/tv_realtime_display
  * @version 1.0
  */
object TestKafka {

  def main(args: Array[String]): Unit = {
    val streamingKey = "test_sunliangliang"
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(10))
    val sc = ssc.sparkContext
    val textFileRdd = sc.textFile("e:/ip_area_isp.txt")
    var ipAreaIspCache: Array[String]  = textFileRdd.filter(x => {
      x.stripMargin != null && x.stripMargin != ""
    }).collect()
    //更新mysql中result字段配置
    //kafka配置
    val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> "103.26.158.182:9092,103.26.158.183:9092,103.26.158.184:9092",
      "group.id" -> "sunliangliang",
      //"zookeeper.connect" -> ConfigUtil.getConf.get.getString("zookeeper_list"),
      "auto.offset.reset" -> "largest")
    val kafakaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("sunliangliang"))
    kafakaDStream.foreachRDD(x => x.foreachPartition(y => {
      for (i <- y){
        println(i)
      }
    }))
    ssc.start()
    ssc.awaitTermination()
    sc.stop()
  }
}
