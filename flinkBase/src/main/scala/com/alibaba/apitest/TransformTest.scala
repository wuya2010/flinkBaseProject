package com.alibaba.apitest

import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

// 转换算子测试, 了解的流的状态

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputStream = env.readTextFile("E:\\WORKS\\Mine\\flinkBaseProject\\flinkBase\\src\\main\\resources\\sensor.txt")

//    val inputStream = env.socketTextStream("localhost", 7777)
    // 1. 简单转换和滚动聚合算子测试
    val dataStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })

    val aggStream = dataStream
      .keyBy("id")
//        .timeWindow(Time.seconds(10))
      //      .max("temperature")
//      .reduce((x, y) => SensorReading(x.id, x.timestamp + 1, y.temperature + 10))
      .min("temperature")

    aggStream.print("min")





    // 2. 分流算子测试
    val splitStream = dataStream
      .split( sensorData => {
        // 根据温度值高低划分不同的流
        if( sensorData.temperature > 30 ) Seq("high") else Seq("low")
      } )

    val lowTempStream = splitStream.select("low")
    val highTempStream = splitStream.select("high")
    val allTempStream = splitStream.select("high", "low")

    // 3. 合并两条流
    val warningStream = highTempStream.map( data => (data.id, data.temperature) )
    //fixme: 连接 2 个 不同类型的流  different type
    val connectedStreams: ConnectedStreams[(String, Double), SensorReading] = warningStream.connect(lowTempStream)

    //fixme:将2个流合并的流拆分
    val coMapStream = connectedStreams.map(
      warningData => (warningData._1, warningData._2, "high temperature warning"),
      lowData => (lowData.id, "healthy")
    )




    val unionStream = highTempStream.union(lowTempStream, allTempStream)

    //测试打印
//    unionStream.print()

    // 4. UDF测试
//    dataStream.filter( new MyFilter() ).print("filter")
//    dataStream.filter( _.id.startsWith("sensor_1") )


    // 打印输出
//    lowTempStream.print("low")
//    highTempStream.print("high")
//    allTempStream.print("all")

    env.execute("transform test")
  }
}

class MyFilter() extends FilterFunction[SensorReading]{
  override def filter(value: SensorReading): Boolean = {
    value.id.startsWith("sensor_1")
  }
}

class MyMapper() extends RichMapFunction[SensorReading, Int]{
  override def map(value: SensorReading): Int = {
    0
  }

  override def open(parameters: Configuration): Unit = super.open(parameters)
}