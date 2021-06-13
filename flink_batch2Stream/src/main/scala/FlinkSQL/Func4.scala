package FlinkSQL

import flink_source.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableAggregateFunction
import org.apache.flink.util.Collector

/**
  * @author kylinWang
  * @data 2021/3/14 17:20
  *
  */
object Func4 {

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
    val inputStream: DataStream[String] = env.readTextFile("sensor.txt")
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    val sensorTable = tableEnv.fromDataStream(dataStream,"id","timestamp")


    //创建表聚合函数
    val top2Temp = new Top2Temp()

    // Table API的调用
    val resultTable = sensorTable.groupBy('id)
      .flatAggregate( top2Temp('temperature) as ('temp, 'rank) )
      .select('id, 'temp, 'rank)

    // 转换成流打印输出
    resultTable.toRetractStream[(String, Double, Int)].print("agg temp")
//    resultSqlTable.toRetractStream[Row].print("agg temp sql")

    env.execute("get func")

  }

}


// 先定义一个 Accumulator
class Top2TempAcc{
  var highestTemp: Double = Int.MinValue
  var secondHighestTemp: Double = Int.MinValue
}

// 自定义 TableAggregateFunction
class Top2Temp extends TableAggregateFunction[(Double, Int), Top2TempAcc]{

  override def createAccumulator(): Top2TempAcc = new Top2TempAcc

  def accumulate(acc: Top2TempAcc, temp: Double): Unit ={
    if( temp > acc.highestTemp ){
      acc.secondHighestTemp = acc.highestTemp
      acc.highestTemp = temp
    } else if( temp > acc.secondHighestTemp ){
      acc.secondHighestTemp = temp
    }
  }

  def emitValue(acc: Top2TempAcc, out: Collector[(Double, Int)]): Unit ={
    out.collect(acc.highestTemp, 1)
    out.collect(acc.secondHighestTemp, 2)
  }
}