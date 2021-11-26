package FlinkSQL

import flink_source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.AggregateFunction
import org.apache.flink.types.Row

/**
  * @author kylinWang
  * @data 2021/3/14 17:17
  *
  */
object Func3 {

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




    //聚合函数
    // createAccumulator ； accumulate ； getValue ; retract ; merge ; resetAccumulator
    val avgTemp = new AvgTemp()

    // Table API的调用
    val resultTable = sensorTable.groupBy('id)
      .aggregate(avgTemp('temperature) as 'avgTemp)
      .select('id, 'avgTemp)

    // SQL的实现
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("avgTemp", avgTemp)
    val resultSqlTable = tableEnv.sqlQuery(
      """
        |SELECT
        |id, avgTemp(temperature)
        |FROM
        |sensor
        |GROUP BY id
      """.stripMargin)

    // 转换成流打印输出
    resultTable.toRetractStream[(String, Double)].print("agg temp")
    resultSqlTable.toRetractStream[Row].print("agg temp sql")

  }

}


// 定义AggregateFunction的Accumulator
class AvgTempAcc {
  var sum: Double = 0.0
  var count: Int = 0
}

class AvgTemp extends AggregateFunction[Double, AvgTempAcc] {
  override def getValue(accumulator: AvgTempAcc): Double =
    accumulator.sum / accumulator.count

  override def createAccumulator(): AvgTempAcc = new AvgTempAcc

  def accumulate(accumulator: AvgTempAcc, temp: Double): Unit ={
    accumulator.sum += temp
    accumulator.count += 1
  }
}