package FlinkSQL

import flink_source.SensorReading
import org.apache.flink.streaming.api.scala._
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

    val inputStream: DataStream[String] = env.readTextFile("E:\\01_myselfProject\\Base\\spark_flink_project\\flink_streaming\\src\\main\\resources\\sensor.txt")
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",").map(x=>x.trim)
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    val resultTable = TableEnv.fromDataStream(dataStream)

 //追加模式， 指定返回值类型
   val getStream =  TableEnv.toAppendStream[Row](resultTable)
//撤回模式, 多了一个boolean
    val getStream2 =  TableEnv.toRetractStream[(String,Long,Double)](resultTable)
//    False： 老数据
//    True: 新数据

    //查看执行情况
    TableEnv.explain(resultTable)

    getStream2.print("table")


    env.execute("get stream")
  }
}
