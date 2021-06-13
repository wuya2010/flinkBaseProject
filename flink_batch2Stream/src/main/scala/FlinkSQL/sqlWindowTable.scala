package FlinkSQL

import flink_source.SensorReading
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Table, Tumble}
import org.apache.flink.table.api.scala._

/**
  * @author kylinWang
  * @data 2021/3/14 16:38
  *
  */
object sqlWindowTable {

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
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1L)) {
          override def extractTimestamp(element: SensorReading): Long =element.timestamp*1000L
        }
      )

    val resultTable = TableEnv.fromDataStream(dataStream)

    //Group Window
    //Tumble ; Hop ; Session ;
    //Tumble_* , Hop_* , Session_*
    //Tumble_start ; Tumble_end ; Tumble_rowtime ; Tumble_proctime


    //Over Window : 所有聚合必须在同一窗口上定义 ;ORDER BY必须在单一的时间属性上指定
    // 必须是相同的分区、排序和范围


    val windowTable  = resultTable
      .window(Tumble over 10.seconds on 'timestamp as 'tw)
      .groupBy('id,'tw)
      .select('id,'id.count)

    val sqlDataTable: Table = resultTable
      .select('id, 'temperature, 'timestamp as 'ts)

    val resultSqlTable: Table = TableEnv
      .sqlQuery("select id, count(id) from "
        + sqlDataTable
        + " group by id,tumble(ts,interval '10' second)")

    // 把 Table转化成数据流, 新增
    val resultDstream: DataStream[(Boolean, (String, Long))] = resultSqlTable
      .toRetractStream[(String, Long)]

    resultDstream.filter(_._1).print()
    env.execute()





  }
}
