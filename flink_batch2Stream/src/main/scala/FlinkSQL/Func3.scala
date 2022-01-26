package FlinkSQL

import flink_source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
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


    val inputStream = env.socketTextStream("192.168.25.229", 7777)

    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val sensorTable = tableEnv.fromDataStream(dataStream,'id,'temperature)



    //聚合函数 ==》自定义聚合函数  sensor_1, 1547718199, 30.8
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
    /**
     * 每来一条数据，对状态进行修改
     * agg temp> (true,(sensor_1,34.55))
     * agg temp> (false,(sensor_1,34.55))
     * agg temp> (true,(sensor_1,33.8))
     * agg temp> (false,(sensor_1,33.8))
     */
//    resultTable.toRetractStream[(String, Double)].print("agg temp")

    // 过时写法
//    resultSqlTable.toRetractStream[Row].print("agg temp sql")

    env.execute()

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