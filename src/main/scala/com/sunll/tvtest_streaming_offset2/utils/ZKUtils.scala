package com.sunll.tvtest_streaming_offset2.utils

import org.I0Itec.zkclient.ZkClient
import scala.collection.mutable.Map

/**
  * 需要动态更新topic的partition的话就用，不需要不用
  *
  * @author sunliangliang 2018-10-23 https://github.com/sunliangliang9201/tvtest_streaming
  * @version 3.0
  */
object ZKUtils {

  /**
    * 获取topic的分区数，目的是动态更新分区数
    * @param topics
    * @param zk
    * @return
    */
  def getPartitionsNum(topics: Set[String],zk: String): Map[String, Int] ={
    val res = Map[String, Int]()
    var client: ZkClient = null
    try{
      client = new ZkClient(zk)
      val basePath = "/brokers/topics"
      var realPath = ""
      for(i <- topics){
        realPath = s"$basePath/$i/partitions"
        val count = client.countChildren(realPath)
        res(i) = count
      }
    }
    res
  }

  /**
    * 测试zkclient
    * @param args
    */
  def main(args: Array[String]): Unit = {
    getPartitionsNum(Set("bf.bftv.tv_test11","bf.bftv.tv_test"), ConfigUtil.getConf.get.getString("zookeeper_list"))
  }
}
