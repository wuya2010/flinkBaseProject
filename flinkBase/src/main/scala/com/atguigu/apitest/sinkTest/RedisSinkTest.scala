package com.atguigu.apitest.sinkTest

import com.atguigu.apitest.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest.sinkTest
  * Version: 1.0
  *
  * Created by wushengran on 2019/10/19 16:14
  */
object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val config = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()

    val inputStream = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    val dataStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })

    dataStream.addSink(new RedisSink[SensorReading]( config, new MyRedisMapper() ))

    dataStream.print()

    env.execute("redis sink test")
  }
}

class MyRedisMapper() extends RedisMapper[SensorReading]{

  // 定义保存数据到redis的命令，HSET sensor_temperature sendor_id temperature
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription( RedisCommand.HSET, "sensor_temperature" )
  }

  override def getValueFromData(data: SensorReading): String = {
    data.temperature.toString
  }

  override def getKeyFromData(data: SensorReading): String = {
    data.id
  }
}