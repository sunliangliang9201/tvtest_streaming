package com.sunll.tvtest_streaming2.utils

import java.util.concurrent.CountDownLatch

import org.apache.zookeeper.Watcher.Event
import org.apache.zookeeper._

/**
  * get and refresh the offsets of partitions of topics.
  *
  * @author sunliangliang 2018-09-28 https://github.com/sunliangliang9201/tvtest_streaming
  * @version 2.0
  */
object ZKClient {

  val zkQuorm: String = ConfigUtil.getConf().get.getString("zookeeper_list")
  val basePath: String = "/conusmers"
  def exist(topics: String, groupID: String): Boolean ={
    val zk = new ZooKeeper(zkQuorm,5000,ZKWatcher)
    val path = basePath + "/" + groupID
    if(zk.exists(path, false) == null){
      //zk.create(path, "0".getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
      return false
    }
  true
  }
}

object ZKWatcher extends Watcher{

  protected var countDownLatch: CountDownLatch = new CountDownLatch(1)

  override def process(watchedEvent: WatchedEvent): Unit = {
    if(watchedEvent.getState eq Event.KeeperState.SyncConnected){
      countDownLatch.countDown()
    }
  }
}