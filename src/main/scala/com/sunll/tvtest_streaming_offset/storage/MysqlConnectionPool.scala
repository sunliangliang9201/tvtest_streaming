package com.sunll.tvtest_streaming_offset.storage

import java.sql.Connection

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.sunll.tvtest_streaming_offset.utils.ConfigUtil

/**
  * mysql connections pool util
  *
  * @author sunliangliang 2018-10-19
  * @version 2.0
  */
class MysqlConnectionPool extends Serializable {
  private val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)
  val jdbcUrl = ConfigUtil.getConf.get.getString("tvtest_host")
  val user = ConfigUtil.getConf.get.getString("tvtest_username")
  val passwd = ConfigUtil.getConf.get.getString("tvtest_password")
  val db = ConfigUtil.getConf.get.getString("tvtest_datebase")
  val port = ConfigUtil.getConf.get.getString("tvtest_port")
  try {
    cpds.setJdbcUrl("jdbc:mysql://" + jdbcUrl + ":" + port + "/" + db + "?" + "useSSL=false")
    cpds.setDriverClass("com.mysql.jdbc.Driver")
    cpds.setUser(user)
    cpds.setPassword(passwd)
    cpds.setMaxPoolSize(10)
    cpds.setMinPoolSize(1)
    cpds.setAcquireIncrement(1)
    cpds.setMaxStatements(500)
    cpds.setInitialPoolSize(1)
  } catch {
    case e: Exception => e.printStackTrace()
  }

  /**
    * 获取连接
    * @return 连接
    */
  def getConnection: Connection = {
    try {
      return cpds.getConnection();
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        null
    }
  }
}

/**
  * 实例对象
  */
object MysqlManager {
  var mysqlManager: MysqlConnectionPool = _
  def getMysqlManager: MysqlConnectionPool = {
    synchronized {
      if (mysqlManager == null) {
        mysqlManager = new MysqlConnectionPool
      }
    }
    mysqlManager
  }
}
