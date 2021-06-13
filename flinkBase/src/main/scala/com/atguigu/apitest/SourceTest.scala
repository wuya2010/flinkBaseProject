package com.atguigu.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2019/10/19 10:18
  */

// 测试source

// 定义温度传感器样例类
case class SensorReading( id: String, timestamp: Long, temperature: Double )

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1. 从集合读取数据
    val stream1 = env.fromCollection( List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 15.4),
      SensorReading("sensor_7", 1547718202, 6.7),
      SensorReading("sensor_10", 1547718205, 38.1)
    ) )
//    env.fromElements(1, 0.34, "data")
    // 2. 从文件读取数据
    val stream2 = env.readTextFile("D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    // 3. 从kafka读取数据
    // 定义相关的配置
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))

    // 4. 自定义source
    val stream4 = env.addSource( new MySensorSource() )

    // print输出
    stream4.print().setParallelism(1)

    env.execute("source test")

  }
}

class MySensorSource() extends SourceFunction[SensorReading]{
  // 定义一个标识位，表示数据源是否继续运行
  var running: Boolean = true

  override def cancel(): Unit = {
    running = false
  }

  // 随机生成自定义的传感器数据
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 初始化一个随机数发生器
    val rand = new Random()

    // 初始化10个传感器数据，随机生成
    var curTemp = 1.to(10).map(
      i => ( "sensor_" + i, 60 + rand.nextGaussian() * 20 )
    )
    // 无限循环，在初始温度值基础上随机波动，产生随机的数据流
    while( running ){
      // 对10个数据更新温度值
      curTemp = curTemp.map(
        data => ( data._1, data._2 + rand.nextGaussian() )
      )
      // 获取当前的时间戳，包装成样例类
      val curTime = System.currentTimeMillis()
      curTemp.foreach(
        data => ctx.collect( SensorReading(data._1, curTime, data._2) )
      )
      // 间隔500ms
      Thread.sleep(500)
    }
  }
}
