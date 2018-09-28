package com.sunll.tvtest_streaming2.model

/**
  * save the global config for this app.
  *
  * @author sunliangliang 2018-09-28 https://github.com/sunliangliang9201/tvtest_streaming
  * @version 2.0
  */
case class StreamingKeyConfig(streamingKey: String, appName: String, driverCores: String, formator: String,
                              topics: String, groupID: String, tableName: String, fields: String,
                              brokerList: String){
}
