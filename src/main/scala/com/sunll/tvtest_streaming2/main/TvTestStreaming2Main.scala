package com.sunll.tvtest_streaming2.main

import com.sunll.tvtest_streaming2.storage.MySQLDao
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * tvtest_streaming2 start main object, start the project and get the config for paralize compute.
  * @author sunliangliang 2018-09-28 https://github.com/sunliangliang9201/tvtest_streaming
  * @version 2.0
  * tpoic tvtest.sunliangliang
  * result country,province,isp,ltype,uuid,imei,mac,mtype,gid,mos,ver,unet,itime,value,mins,dt,hour
  *
  * ######but where is the difference? streaming2 manager the offsets owen, save the topics -- partitions -- offsets in mysql
  * ######or redis. so we do not use checkpoint to guarantee the exactly once!!!!!
  */
object TvTestStreaming2Main {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Offset").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(4))
    val streamingKey = "TvTest2"
    val streamingKeyConfig = MySQLDao.getStreamingConfig(streamingKey)
    if(streamingKeyConfig == null){
      println("no streaming key find")
      System.exit(1)
    }

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
