package FlinkSQL

import flink_source.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors._

/**
  * @author kylinWang
  * @data 2021/3/11 22:48
  *
  */
object FlinkTableApi {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val streamSetting = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode() //fixme: StreamingMode
      .build()

    //SteamTableEnvironment
    val TableEnv = StreamTableEnvironment.create(env,streamSetting)

    //blink 版本的批处理环境
    val batchSetting = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode() //fixme: BatchMode
      .build()
    val batchEnv = TableEnvironment.create(batchSetting)


    val inputStream = env.socketTextStream("192.168.25.229", 7777)
    val dataStream = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })




    //连接到文件系统
    val tempTable  = TableEnv
      //streamTableDescriptor
      .connect(new FileSystem()
        .path("E:\\WORKS\\Mine\\flinkBaseProject\\flink_batch2Stream\\src\\main\\resources\\sensor.csv"))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("timestamp",DataTypes.BIGINT())
        .field("temperature",DataTypes.DOUBLE())
      )
      //创建临时表
      .createTemporaryTable("inputTable")


//    //连接到kafka
//    TableEnv.connect(
//      new Kafka().version("0.11").topic("test")
//        .property("zookeeper.connect", "localhost:2181")
//        .property("bootstrap.servers", "localhost:9092")
//    )
//      //fixme: 作用？
//      .withFormat(new Csv())
//      .withSchema(new Schema()
//        .field("id",DataTypes.STRING())
//        .field("timestamp",DataTypes.BIGINT())
//        .field("temperature",DataTypes.DOUBLE())
//      )
//      //创建临时表
//      .createTemporaryTable("inputTable")
//
//
//
//
////    //fixme:定义输出结果表
//    TableEnv.connect(
//      new Kafka()
//        .version("0.11")
//        .topic("sinkTest")
//        .property("zookeeper.connect", "localhost:2181")
//        .property("bootstrap.servers", "localhost:9092")
//    )
//      .withFormat( new Csv() )
//      .withSchema( new Schema()
//        .field("id", DataTypes.STRING())
//        .field("temp", DataTypes.DOUBLE())
//      )
//      .createTemporaryTable("kafkaOutputTable")
//
//    //输出到 es : 添加 flink - json 的格式
//    TableEnv.connect(
//      new Elasticsearch()
//        .version("6") //es版本
//        .host("localhost", 9200, "http")
//        .index("sensor")
//        .documentType("temp")
//    )
//      // fixme: 指定是 Upsert 模式 : updateMode = UPDATE_MODE_VALUE_UPSERT
//      .inUpsertMode()
//      .withFormat(new Json())
//      .withSchema( new Schema()
//        .field("id", DataTypes.STRING())
//        .field("count", DataTypes.BIGINT())
//      )
//      .createTemporaryTable("esOutputTable")
//
//
//    //输出到mysql
//    val mysqlDDL =
//      """
//        |create table jdbcOutputTable (
//        |  id varchar(20) not null,
//        |  cnt bigint not null
//        | ) with (
//        |  'connector.type' = 'jdbc',
//        |  'connector.url' = 'jdbc:mysql://localhost:3306/wang',
//        |  'connector.table' = 'sensor_count',
//        |  'connector.driver' = 'com.mysql.jdbc.Driver',
//        |  'connector.username' = 'root',
//        |  'connector.password' = '123456'
//      """.stripMargin
//
//    //定义更新模式
//    TableEnv.sqlUpdate(mysqlDDL)
//    //输出表名称： jdbcOutputTable



    val result = TableEnv.fromDataStream(dataStream)

    //表查询 TableApi
   val ret: Table =  result.select("id,temperature").filter("id='sensor_1'")

   //聚合
    val aggTable = result
      .groupBy("id")
//      .groupBy("key").select("key, value.avg")
      .select("id, id.count as count")




    //从临时表读取数据： 表查询 Sql
    val sqlTable = TableEnv.sqlQuery("select id, temperature from inputTable where id ='sensor_1'")


    //sqlc查询2
    val sqlTable2: Table =  TableEnv.sqlQuery(
      """
        |select id, temperature
        |from inputTable
        |where id ='sensor_1'
      """.stripMargin)

    val resultStream =
//      result.toAppendStream[(String,Long,Double)]

//      sqlTable2.toAppendStream[(String, Double)]

//      aggTable.toRetractStream[(String,Long)]

//      ret.toRetractStream[(String,Double)]

    /**
     * toAppendStream 报错：
     * 相关问题： Table is not an append-only table. Use the toRetractStream() in order to handle add and retract messages.
     *
     * toRetratStream():
     * rest:4> (true,(sensor_1,1))
     * rest:4> (false,(sensor_1,1))
     * rest:4> (true,(sensor_1,2))
     * rest:4> (false,(sensor_1,2))
     * rest:4> (true,(sensor_1,3))
     *
     */


//    resultStream.print("rest")

    //结果写入输出表
//    result.insertInto("outputTable")

    env.execute("get test")

  }
}
