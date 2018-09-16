package com.sunll.tvtest_streaming.model

/**
  * case class for appication config
  * @param streamingKey
  * @param appName
  * @param driverCores
  * @param formator
  * @param topics
  * @param groupID
  * @param tableName
  * @param fields
  * @param brolerList
  */
case class StreamingKeyConfig(streamingKey:String, appName:String, driverCores:String,
                              formator:String, topics:String, groupID:String, tableName:String,
                              fields:String, brolerList:String) {

}
