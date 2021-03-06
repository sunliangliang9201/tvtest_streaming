package com.sunll.tvtest_streaming_offset.main

import com.sunll.tvtest_streaming_offset.formator.LogFormator
import com.sunll.tvtest_streaming_offset.storage.MysqlDao
import com.sunll.tvtest_streaming_offset.utils.{ConfigUtil, Constants, KafkaHelper, ReloadConfigManager}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory


/**
  * tvtest_streaming start main object, start the project and get the config for paralize compute.
  *
  * the different is we manage the offsets myself in mysql.
  * @author sunliangliang 2018-10-19 https://github.com/sunliangliang9201/tvtest_streaming
  * @version 2.0
  * tpoic tvtest.sunliangliang
  * consumer-group group-bftv-tvtest
  * result country,province,isp,ltype,uuid,imei,mac,mtype,gid,mos,ver,unet,itime,value,mins,dt,hour
  *
  */
object TvtestStreamingMain {

  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(this.getClass)
    val streamingKey = args(0)
    //val streamingKey = "TvTest"
    val streamingIntervalTime = 30
    //是否采用自己管理的offset作为创建kafkaDStream
    val flag = Integer.valueOf(args(1))
    //val flag = 1
    val streamingKeyConfig = MysqlDao.findStreamingKeyConfig(streamingKey)
    if(null == streamingKeyConfig){
      logger.error("No streaming config found...")
      System.exit(-1)
    }
    logger.info("success load the config" + streamingKeyConfig)
    val conf = new SparkConf().setAppName(streamingKeyConfig.appName).set("spark.driver.cores", streamingKeyConfig.driverCores)
    //val conf = new SparkConf().setAppName(streamingKeyConfig.appName).setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(streamingIntervalTime))
    val sc = ssc.sparkContext
    val textFileRdd = sc.textFile("hdfs://192.168.5.31:9000/test/sunliangliang/ip_area_isp.txt")
    //val textFileRdd = sc.textFile("e:/ip_area_isp.txt")
    var ipAreaIspCache: Array[String]  = textFileRdd.filter(x => {
      x.stripMargin != null && x.stripMargin != ""
    }).collect()
    //更新mysql中result字段配置
    val reloadConfig = new ReloadConfigManager
    reloadConfig.init(60 * 1000 * 3, streamingKey, streamingKeyConfig)
    //kafka配置
    val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> streamingKeyConfig.brolerList,
                                                "group.id" -> streamingKeyConfig.groupID,
                                                "zookeeper.connect" -> ConfigUtil.getConf.get.getString("zookeeper_list"),
                                                "auto.offset.reset" -> ConfigUtil.getConf.get.getString("auto_offset_reset")
                                                )
    val topicSet = streamingKeyConfig.topics.split(",").toSet
    println("topic is" + topicSet.toList)
    logger.info("success to load kafkaParams " + kafkaParams)
    logger.info("success to load topics " + topicSet)
    var kafkaDStream: InputDStream[(String, String)] = null
    //是否启用保证exactly once消费
    if(flag == 0){
      kafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
    }else{
      kafkaDStream = KafkaHelper.getKafkaDStreamFromOffset(streamingKeyConfig.groupID, ssc, kafkaParams, topicSet)
    }
    //通过清洗类清洗日志所有字段
    val logFormator = Class.forName(Constants.FORMATOR_PACACKE_PREFIX2 + streamingKeyConfig.formator).newInstance().asInstanceOf[LogFormator]
    //清洗入库并更新offset
    var offsetRange = Array[OffsetRange]()
    kafkaDStream.transform(all => {
      offsetRange = all.asInstanceOf[HasOffsetRanges].offsetRanges
      all
    }).map(x => {
      logFormator.format(x._2, ipAreaIspCache, reloadConfig.getFields)
    }).foreachRDD(x => {
      //这里的shuffle效率问题有待商榷
      //x.repartition(8)
      x.foreachPartition(y => MysqlDao.insertBatch(y, streamingKeyConfig.tableName, reloadConfig.getInsertSQL(), reloadConfig.getFields))
      MysqlDao.updateOffset(streamingKeyConfig.groupID, offsetRange)
    })
    ssc.start()
    ssc.awaitTermination()
    sc.stop()
  }
}