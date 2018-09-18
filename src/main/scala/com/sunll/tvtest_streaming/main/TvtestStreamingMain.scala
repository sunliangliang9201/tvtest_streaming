package com.sunll.tvtest_streaming.main

import com.sunll.tvtest_streaming.formator.LogFormator
import com.sunll.tvtest_streaming.storage.MysqlDao
import com.sunll.tvtest_streaming.utils.{ConfigUtil, Constants, ReloadConfigManager}
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory


/**
  * tvtest_streaming start main object, start the project and get the config for paralize compute.
  * @author sunliangliang 2018-09-10 https://github.com/sunliangliang9201/tvtest_streaming
  * @version 1.0
  * tpoic tvtest.sunliangliang
  * consumer-group group-bftv-tvtest
  * result country,province,isp,ltype,uuid,imei,mac,mtype,gid,mos,ver,unet,itime,value,mins,dt,hour
  *
  */
object TvtestStreamingMain {


  def main(args: Array[String]): Unit = {
    //初始化日志对象
    val logger = LoggerFactory.getLogger(this.getClass)
    //获取mysql配置
    val streamingKey = args(0)
//    val streamingIntervalTime = Integer.parseInt(args(1))
    //val streamingKey = "TvTest"
    val streamingIntervalTime = 15
    val streamingKeyConfig = MysqlDao.findStreamingKeyConfig(streamingKey)
    if(null == streamingKeyConfig){
      logger.error("No streaming config found...")
      System.exit(-1)
    }
    logger.info("success load the config" + streamingKeyConfig)
    val conf = new SparkConf().setAppName(streamingKeyConfig.appName).set("spark.driver.cores", streamingKeyConfig.driverCores).setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(streamingIntervalTime))
    //更新mysql中result字段配置
    ReloadConfigManager.init(60*1000, streamingKey, streamingKeyConfig)
    //kafka配置
    val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> streamingKeyConfig.brolerList,
                                                "group.id" -> streamingKeyConfig.groupID,
                                                "zookeeper.connect" -> ConfigUtil.getConf.get.getString("zookeeper_list"),
                                                "auto.offset.reset" -> ConfigUtil.getConf.get.getString("auto_offset_reset"))
    val topicSet = streamingKeyConfig.topics.split(",").toSet
    logger.info("success to load kafkaParams " + kafkaParams)
    logger.info("success to load topics " + topicSet)
    val kafakaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
    //关键点：通过清洗类清洗日志所有字段
    val logFormator = Class.forName(Constants.FORMATOR_PACACKE_PREFIX + streamingKeyConfig.formator).newInstance().asInstanceOf[LogFormator]
    //清洗入库
    kafakaDStream.map(x => {
      logFormator.format(x._2)
    }).foreachRDD(x => x.foreachPartition(y => MysqlDao.insertBatch(y, streamingKeyConfig.tableName)))
    ssc.start()
    ssc.awaitTermination()
  }
}




















