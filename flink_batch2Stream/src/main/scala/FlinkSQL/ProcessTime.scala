package FlinkSQL

import flink_source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors._

/**
  * @author kylinWang
  * @data 2021/3/14 10:53
  *
  */
object ProcessTime {
  def main(args: Array[String]): Unit = {

    //fixme： 将 DataStream 转换成 表
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //SteamTableEnvironment
    val TableEnv = StreamTableEnvironment.create(env)

    val inputStream: DataStream[String] = env.readTextFile("E:\\WORKS\\Mine\\flinkBaseProject\\flink_batch2Stream\\src\\main\\resources\\sensor.txt")
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",").map(x=>x.trim)
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })


    //fixme: 指定process Time 的三种方式
    // 1. 在DataStream转化时直接指定；
    // 2. 在定义Table Schema时指定；
    // 3. 在创建表的DDL中指定

    //方式1：
//    val streaTable = TableEnv.fromDataStream(dataStream, 'id, 'timestamp, 'temperature, 'pt.proctime)

    //方式2：
    TableEnv.connect(
      new FileSystem().path("sensor.csv"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
        //定义process fixme: DataTypes.TIMESTAMP
        .field("pt", DataTypes.TIMESTAMP(3))
        .proctime()    // 指定 pt字段为处理时间
      ) // 定义表结构
      .createTemporaryTable("inputTable") // 创建临时表

      //"E:\\WORKS\\Mine\\flinkBaseProject\\flink_batch2Stream\\src\\main\\resources\\sensor.csv"





    //DDL指定 : pt AS PROCTIME()
//    val sinkDDL: String =
//      """
//        |create table dataTable (
//        |  id varchar(20) not null,
//        |  ts bigint,
//        |  temperature double,
//        |  pt AS PROCTIME()
//        |) with (
//        |  'connector.type' = 'filesystem',
//        |  'connector.path' = 'file:///E:\WORKS\Mine\flinkBaseProject\flink_batch2Stream\src\main\resources\sensor.csv',
//        |  'format.type' = 'csv'
//        |)
//      """.stripMargin
//
//    TableEnv.sqlUpdate(sinkDDL) // 执行 DDL




//    val table = TableEnv.sqlQuery(
//      """
//        |select * from streaTable
//        |""".stripMargin)
//
//    table.toRetractStream

    env.execute("get window")

  }

}
