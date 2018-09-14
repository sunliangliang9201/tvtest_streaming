package com.sunll.tvtest_streaming.model

case class StreamingKeyConfig(streamingKey:String, appName:String, driverCores:String,
                              formator:String, topics:String, groupID:String, tableName:String,
                              fields:String, brolerList:String) {

}
