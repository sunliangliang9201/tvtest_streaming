package com.sunll.tvtest_streaming_offset2.model

/**
  * case class for appication config
  * @param streamingKey 任务唯一标示
  * @param appName 任务名
  * @param driverCores 核心数
  * @param formator 日志格式化类名
  * @param topics 消费的topics
  * @param groupID 消费组
  * @param tableName 目标结果表
  * @param fields 所需字段，这个不用了，有一个单独的tvtest_straeming_fields表来获取所需字段
  * @param brolerList kafka brokers
  */
case class StreamingKeyConfig(streamingKey:String, appName:String, driverCores:String,
                              formator:String, topics:String, groupID:String, tableName:String,
                              fields:String, brolerList:String) {
}
