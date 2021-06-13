package FlinkSQL

import flink_source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Rowtime, Schema}

/**
  * @author kylinWang
  * @data 2021/3/14 15:52
  *
  */
object EventTime {
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
      //定义 watermark 的生成方式
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // fixme: 根据每个记录中包含的时间生成结果,这样即使在有乱序事件或者延迟事件时，也可以获得正确的结果
    //1. 根据 DataStream 转化成 Table 时指定
    val resultTable =  TableEnv.fromDataStream(dataStream,"id","timestamp".rowtime,"temperature")

    //2. 直接追加字段
    val resultTable2 = TableEnv.fromDataStream(dataStream,"id","temperature","timestamp","rt".rowtime)

    //3. 定义TableSchema指定
    TableEnv.connect(
      new FileSystem().path("result.txt")
    )
    .withFormat(new Csv())
    .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temperature", DataTypes.BIGINT())
        //指定 eventTime : 将前面定义的字段指定为事件时间属性。
        .rowtime(
          new Rowtime()
           .timestampsFromField("timestamp") // 从字段中提取时间戳
        .watermarksPeriodicBounded(1000)   // watermark延迟1秒
        )
      .field("timestamp", DataTypes.DOUBLE())
    )
      .createTemporaryTable("inputTable")


    //4. 创建ddl指定
    // 这里FROM_UNIXTIME是系统内置的时间函数，用来将一个整数（秒数）转换成“YYYY-MM-DD hh:mm:ss”格式
    // TO_TIMESTAMP将其转换成Timestamp
    val sinkDDL: String =
      """
        |create table dataTable (
        |  id varchar(20) not null,
        |  ts bigint,
        |  temperature double,
        |  rt AS TO_TIMESTAMP( FROM_UNIXTIME(ts) ),
        |  watermark for rt as rt - interval '1' second
        |) with (
        |  'connector.type' = 'filesystem',
        |  'connector.path' = 'file:///D:\\..\\sensor.txt',
        |  'format.type' = 'csv'
        |)
      """.stripMargin
    TableEnv.sqlUpdate(sinkDDL) // 执行 DDL










  }
}
