package FlinkSQL

import flink_source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{GroupWindow, Over, Slide}

/**
  * @author kylinWang
  * @data 2021/3/14 16:11
  *
  */
object windowApiTable {

  def main(args: Array[String]): Unit = {

    //fixme： 将 DataStream 转换成 表
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //SteamTableEnvironment
    val TableEnv = StreamTableEnvironment.create(env)

    val inputStream: DataStream[String] = env.readTextFile("E:\\01_myselfProject\\Base\\spark_flink_project\\flink_streaming\\src\\main\\resources\\sensor.csv")
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",").map(x=>x.trim)
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    val resultTable = TableEnv.fromDataStream(dataStream)

    //fixme:下面我们就来看看Table API和SQL中，怎么利用时间字段做窗口操作,主要两种窗口：Group Windows和Over Windows

    //1. 分组窗口 Group Windows
    // over：定义窗口长度
    // on：用来分组
    //    *   table
    //      *     .window(Over partitionBy 'c orderBy 'rowTime preceding 10.seconds as 'ow)
    //    *     .select('c, 'b.count over 'ow, 'e.sum over 'ow)
    //    * }


    // fixme: 窗口的语法过时
//    resultTable
//    .window(Over partitionBy 'c orderBy 'rowTime preceding 10.seconds as 'ow)
//    .select('c , 'b.count over 'ow , 'e.sum over 'ow)



  //3. 滑动窗口
//    over：定义窗口长度
//    every：定义滑动步长
//    on：用来分组（按时间间隔）或者排序（按行数）的时间字段
//    as：别名，必须出现在后面的groupBy中
resultTable
  // Event-time Window
//  .window(Slide over 10.minutes every 5.minutes on 'rowtime as 'w)
  //process-time window
//      .window(Slide over 10.minutes every 5.minutes on 'proctime as 'w)
   //row_count window

    // fixme: 窗口的语法过时
//      .window(Slide over 10.rows every 5.rows on 'proctime as 'w)



   //Over Windows
    //1. 无界 over

    //2. 有界 over



  }

}
