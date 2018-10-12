package com.sunll.tvtest_streaming2.storage

import java.sql.Connection

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.sunll.tvtest_streaming2.utils.ConfigUtil

/**
  * mysql connections pool
  *
  * @author sunliangliang 2018-09-28 https://github.com/sunliangliang9201/tvtest_streaming
  * @version 2.0
  */
class MySQLPool {

  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)
  val jdbcUrl = ConfigUtil.getConf.get.getString("tvtest_host")
  val user = ConfigUtil.getConf.get.getString("tvtest_username")
  val passwd = ConfigUtil.getConf.get.getString("tvtest_password")
  val db = ConfigUtil.getConf.get.getString("tvtest_datebase")
  val port = ConfigUtil.getConf.get.getString("tvtest_port")
  try {
    cpds.setJdbcUrl("jdbc:mysql://" + jdbcUrl + ":" + port + "/" + db)
    cpds.setDriverClass("com.mysql.jdbc.Driver")
    cpds.setUser(user)
    cpds.setPassword(passwd)
    cpds.setMaxPoolSize(200)
    cpds.setMinPoolSize(3)
    cpds.setAcquireIncrement(5)
    cpds.setMaxStatements(180)
  } catch {
    case e: Exception => e.printStackTrace()
  }

  def getConnection(): Connection={
    try{
      return cpds.getConnection
    }catch {
      case e: Exception => e.printStackTrace()
        null
    }
  }
}
/**
  * 外界获取mysqlpool的单例对象的object
  */
object MySQLPoolManager{
  var mySQLPoolManager: MySQLPool = null
  def getMySQLPool(): MySQLPool={
    synchronized{
      if(mySQLPoolManager == null){
        mySQLPoolManager = new MySQLPool()
      }
    }
    mySQLPoolManager
  }
}
