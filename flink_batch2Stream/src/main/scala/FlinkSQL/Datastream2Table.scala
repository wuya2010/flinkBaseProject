package FlinkSQL

import flink_source.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
  * @author kylinWang
  * @data 2021/3/14 9:38
  *
  */
object Datastream2Table  {

  def main(args: Array[String]): Unit = {

    //fixme： 将 DataStream 转换成 表
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //SteamTableEnvironment
    val TableEnv = StreamTableEnvironment.create(env)

    val inputStream: DataStream[String] = env.readTextFile("E:\\01_myselfProject\\Base\\spark_flink_project\\flink_streaming\\src\\main\\resources\\sensor.txt")
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",").map(x=>x.trim)
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    dataStream.print("data")

    //fixme: 流转Table
    val resultTable = TableEnv.fromDataStream(dataStream)

    //fromDataStream 的几种方式 : 1. 基于位置的对应： 2. 基于名称的对应：
    //TableEnv.fromDataStream(dataStream,)

    //创建临时视图 1.基于dataStream创建临时视图； 2. 基于Table创建临时视图
    TableEnv.createTemporaryView("sensorView", dataStream) //,"id,temperature")  // Field reference expression or alias on field expression expected.
    TableEnv.createTemporaryView("sensorView2",resultTable)

    //在Table API中，可以认为View和Table是等价的

    resultTable.printSchema()
//    root
//    |-- id: STRING
//    |-- timestamp: BIGINT
//    |-- temperature: DOUBLE

    //输出到外部表
    TableEnv.connect(
      new FileSystem().path("out.txt")
    )
        .withFormat(new Csv())
        .withSchema(new Schema()
          //定义表结构
          .field("id", DataTypes.STRING())
          .field("timestamp", DataTypes.BIGINT())
          .field("temperature", DataTypes.DOUBLE())
        )
        .createTemporaryTable("outputTable")



    //fixme: 将表输出
    resultTable.insertInto("outputTable")



    env.execute("test")
  }
}
