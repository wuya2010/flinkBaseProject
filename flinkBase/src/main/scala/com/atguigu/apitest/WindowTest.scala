package com.atguigu.apitest

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved 
  *
  * Project: FlinkTutorial
  * Package: com.atguigu.apitest
  * Version: 1.0
  *
  * Created by wushengran on 2019/10/21 10:19
  */
object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(300L)
    env.enableCheckpointing(60000L)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(90000L)
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(10000L)
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
//    env.setStateBackend(new RocksDBStateBackend(""))

    val inputStream = env.readTextFile("F:\\03. Codes\\03-实时分析\\02-Flink\\01-IDEA\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

//    val inputStream = env.socketTextStream("localhost", 7777)


    val dataStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })
      //fixme: 生成 watermark 的 3 种方法:
      //.assignAscendingTimestamps(_.timestamp * 1000L)
      .assignTimestampsAndWatermarks( new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.milliseconds(2500)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
    } )
//      .assignTimestampsAndWatermarks( new MyAssigner() )

    val outputTag = new OutputTag[SensorReading]("side")  //侧输出流



    // 每个传感器每隔15秒输出这段时间内的最小值
    val minTempPerWindowStream = dataStream
      .keyBy(_.id)
//      .timeWindow(Time.seconds(15))  //fixme: 与 window 的区别是什么?
        .window(SlidingEventTimeWindows.of(Time.minutes(1),Time.seconds(10),Time.hours(-8)))
        .allowedLateness(Time.seconds(10))  //允许延迟
      .sideOutputLateData(outputTag) //侧输出
      .minBy("temperature")
    //      .reduce( (x, y) => SensorReading( x.id, y.timestamp, x.temperature.min(y.temperature) ) )

//    对于窗口时间是没有详细的说明的?

    //    获得3种种的方式
    dataStream.print("data")
    minTempPerWindowStream.print("min")
    minTempPerWindowStream.getSideOutput(outputTag).print("side")

    env.execute("window test")
  }
}

class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading]{
  // 定义一个最大延迟时间
  val bound: Long = 1000L
  // 定义当前最大的时间戳
  var maxTs: Long = Long.MinValue

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs - bound)
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    maxTs = maxTs.max(element.timestamp * 1000)
    element.timestamp * 1000L
  }
}

class MyAssigner2 extends AssignerWithPunctuatedWatermarks[SensorReading]{
  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    if(lastElement.id == "sensor_1"){
      new Watermark(extractedTimestamp)
    } else{
      null
    }
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    element.timestamp * 1000L
  }
}