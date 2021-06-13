package FlinkSQL

import flink_source.SensorReading
import org.apache.calcite.interpreter.Row
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._
import org.apache.flink.table.functions.ScalarFunction



/**
  * @author kylinWang
  * @data 2021/3/14 16:52
  *
  */
object Func1 {
  def main(args: Array[String]): Unit = {

    //TableApi:  ===

    //Sql:  OR / A IS FALSE / Not BOOLEAN
    //TableApi :  ||  / isFalse | !


    // charLength , upperCase , toDate , toTimestamp , currentTime
    // NUMERIC.days , NUMERIC.minutes , count , sum

    //Count(*) , SUM  , rank , row_number


    //自定义 DDF / 标量函数 / 表函数 / 聚合函数 / 表聚合函数 /


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

    // 将 DataStream转换为 Table，并指定时间字段
    val sensorTable = tableEnv.fromDataStream(dataStream, 'id, 'timestamp.rowtime, 'temperature)

    // Table API中使用
    val hashCode = new HashCode(10)

    val resultTable = sensorTable
      .select( 'id, hashCode('id) )

    // SQL 中使用
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("hashCode",hashCode)
    val resultSqlTable = tableEnv.sqlQuery("select id, hashCode(id) from sensor")

    // 转换成流，打印输出
    resultTable.toAppendStream[Row].print("table")
    resultSqlTable.toAppendStream[Row].print("sql")

    env.execute()

  }
}

/**
  * 自定义函数
  * @param factor
  */
class HashCode( factor: Int ) extends ScalarFunction {
  def eval( s: String ): Int = {
    s.hashCode * factor
  }
}
