package FlinkSQL

import flink_source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
//import org.apache.flink.table.api.scala._  //fixme: 导入依赖

/**
  * @author kylinWang
  * @data 2021/3/11 22:21
  *
  */
object FlinkTableDemo {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

//    val inputStream: DataStream[String] = env.readTextFile("E:\\WORKS\\Mine\\flinkBaseProject\\flink_batch2Stream\\src\\main\\resources\\sensor.csv")

    val inputStream = env.socketTextStream("192.168.25.229", 7777)

    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })

//    dataStream.print("stream")

    val tableEnv = StreamTableEnvironment.create(env)
    //流转换为tables
    val dataTable = tableEnv.fromDataStream(dataStream)

    //调用Table Api
    // id: String, timestamp: Long, temperature: Double
    val resultTable = dataTable.select("id,timestamp")

    resultTable.printSchema()
    //    root
    //    |-- id: STRING
    //    |-- timestamp: BIGINT


    // fixme: 过时写法
//    val resultSream = resultTable.toAppendStream[(String,Long)]
//    resultSream.print("result")




    //展示
    env.execute("test")

  }
}
