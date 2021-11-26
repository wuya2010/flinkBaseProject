package FlinkSQL

import flink_source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions._
import org.apache.flink.types.Row

/**
  * @author kylinWang
  * @data 2021/3/14 17:08
  *
  */
object func2 {

  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val settings = EnvironmentSettings
      .newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create( env, settings )


    // 定义好 DataStream
    val inputStream: DataStream[String] = env.readTextFile("sensor.csv")
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val sensorTable = tableEnv.fromDataStream(dataStream,"id","timestamp")


//    *   class MySplitUDTF extends TableFunction<String> {
//      *     public void eval(String str) {
//        *       str.split("#").forEach(this::collect);
//        *     }
//      *   }
//    *
//    *   TableFunction<String> split = new MySplitUDTF();
//    *   tableEnv.registerFunction("split", split);
//    *

    val split = new Split(",") //定义分隔符

    // Table API中调用，需要用joinLateral
    val resultTable = sensorTable
      .joinLateral(split('id) as ('word, 'length))   // as对输出行的字段重命名
      .select('id, 'word, 'length)

    // 或者用leftOuterJoinLateral
    val resultTable2 = sensorTable
      .leftOuterJoinLateral(split('id) as ('word, 'length))
      .select('id, 'word, 'length)


    //sql 方式
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("split", split)

    val resultSqlTable = tableEnv.sqlQuery(
      """
        |select id, word, length
        |from
        |sensor, LATERAL TABLE(split(id)) AS newsensor(word, length)
      """.stripMargin)

    // 或者用左连接的方式
    val resultSqlTable2 = tableEnv.sqlQuery(
      """
        |SELECT id, word, length
        |FROM
        |sensor
        |  LEFT JOIN
        |  LATERAL TABLE(split(id)) AS newsensor(word, length)
        |  ON TRUE
      """.stripMargin
    )



    // 转换成流打印输出
    resultTable.toAppendStream[Row].print("1")
    resultTable2.toAppendStream[Row].print("2")


  }
}


// 自定义TableFunction
class Split(separator: String) extends TableFunction[(String, Int)]{
  def eval(str: String): Unit = {
    str.split(separator).foreach(
      word => collect((word, word.length))
    )
  }
}



