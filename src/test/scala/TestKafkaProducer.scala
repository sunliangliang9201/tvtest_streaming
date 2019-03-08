import java.util.{Collections, Properties}

import com.alibaba.fastjson.JSONObject

import scala.collection.JavaConversions._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

/**
  * 测试kafka客户端版本与kafka集群版本的兼容性，两种情况：
  * 第一：低版本的client0.9访问1.1.1版本的kakfa
  * 第二：高版本的client访问低版本kafka集群
  *
  * @author sunliangliang 2019-02-26 https://github.com/sunliangliang9201/tv_realtime_display
  * @version 1.0
  */
object TestKafkaProducer {

  def main(args: Array[String]): Unit = {
    producer()
    //consumer()
  }

  def consumer(): Unit ={
    val props = new Properties()
    props.put("bootstrap.servers", "103.26.158.194:9092,103.26.158.195:9092,103.26.158.196:9092,103.26.158.197:9092,103.26.158.198:9092,103.26.158.199:9092,103.26.158.200:9092")
    //props.put("bootstrap.servers", "103.26.158.182:9092,103.26.158.183:9092,103.26.158.184:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "sunliangliang")
    props.put("auto.offset.reset","earliest")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "1000")
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList("test_sunliangliang"))
    //consumer.subscribe(Collections.singletonList("sunliangliang"))
    while(true){
      val records = consumer.poll(100)
      for(record <- records){
        println(record.key + "--" + record.value())
      }
    }
  }

  def producer(): Unit ={
    //val brokers_list = "103.26.158.194:9092,103.26.158.195:9092,103.26.158.196:9092,103.26.158.197:9092,103.26.158.198:9092,103.26.158.199:9092,103.26.158.200:9092"
    val brokers_list = "103.26.158.182:9092,103.26.158.183:9092,103.26.158.184:9092"
    //val topic = "test_sunliangliang"
    val topic = "sunliangliang"
    val properties = new Properties()
    properties.put("group.id", "sunliangliang")
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers_list)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    val producer = new KafkaProducer[String, String](properties)
    var num = 0
    for(i <- 1 to 100){
      val json = new JSONObject()
      json.put("name", "sunliangliang"+i)
      json.put("addr", "26"+i)
      producer.send(new ProducerRecord(topic, json.toString))
    }
    producer.close()
  }
}
