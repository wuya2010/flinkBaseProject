package FlinkSQL

import flink_source.SensorReading
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
  * @author kylinWang
  * @data 2021/3/14 10:36
  *
  */
object Table2Datastream {

  def main(args: Array[String]): Unit = {

    //fixme： 将 DataStream 转换成 表
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //SteamTableEnvironment
    val TableEnv = StreamTableEnvironment.create(env)

//    val inputStream: DataStream[String] = env.readTextFile("E:\\01_myselfProject\\Base\\spark_flink_project\\flink_streaming\\src\\main\\resources\\sensor.csv")
//    val dataStream: DataStream[SensorReading] = inputStream
//      .map(data => {
//        val dataArray = data.split(",").map(x=>x.trim)
//        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
//      })


    val inputStream = env.socketTextStream("192.168.25.229", 7777)


    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1L)) {
          override def extractTimestamp(element: SensorReading): Long =element.timestamp*1000L
        }
      )

    val resultTable: Table = TableEnv.fromDataStream(dataStream)

 //追加模式， 指定返回值类型
   val getStream: DataStream[Row] =  TableEnv.toAppendStream[Row](resultTable)
//    getStream.print("getStream")



//撤回模式, 多了一个boolean fixme: 这种方式识别不了 TableEnv.toRetractStream[(String,Long,Double)](resultTable)
    import org.apache.flink.table.api.scala._
    val getStream2 =  TableEnv.toRetractStream[(String,Long,Double)](resultTable)
//    False： 老数据
//    True: 新数据

    //查看执行情况
    TableEnv.explain(resultTable)
    getStream2.print("getStream")



    env.execute("get stream")
  }
}
