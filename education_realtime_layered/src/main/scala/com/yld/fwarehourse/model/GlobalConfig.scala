package com.atguigu.education.model

//fixme: hbase 相关
object GlobalConfig {

  val HBASE_ZOOKEEPER_QUORUM = "hadoop101,hadoop102,hadoop103"
  val HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "2181"

  val BOOTSTRAP_SERVERS = "hadoop101:9092,hadoop102:9092,hadoop103:9092"
  val ACKS = "-1"


}
