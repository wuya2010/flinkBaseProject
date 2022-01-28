package flink_cdc

import FlinkSQL.UserBehavior
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
  * @author kylinWang
  * @data 2022-01-27 14:12
  *
  */
object test_mysqlCdcStreamJoinStream3 {

  def main(args: Array[String]): Unit = {

    /**
      * 1. 2条流的关联
      * 2. 在给点时间范围或者窗口内对数据进行更新
      */

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    // 不设置 checkpoint

    //---------------------------------------获取直接的流，非binlog 的流 -------------------------------------------------------------

    // 传入数据格式： 1, 1547718205, 15 , 2022-01-27 17:03:07

    //配置两个时间区间
    val inputStream1 = env.socketTextStream("hadoop105", 6666)
    val inputStream2 = env.socketTextStream("localhost", 5555)
    val dataStream1 = inputStream1
      .map(data => {
        val dataArray = data.split(",")
        //id, long, time
        UserBehavior(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }).assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(1)) {
        override def extractTimestamp(t: UserBehavior): Long = t.timeOrder * 1000L
      }
    )


    // 定义watermarker
    val dataStream2 = inputStream2
      .map(data => {
        val dataArray = data.split(",")
        //id, long, time
        UserBehavior(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      }).assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(1)) {
        override def extractTimestamp(t: UserBehavior): Long = t.timeOrder * 1000L
      }
    )

    // 创建流的watermark

    tableEnv.createTemporaryView("stream1", dataStream1)
    tableEnv.createTemporaryView("stream2", dataStream2)


    /**
      * 1. 在没有设置 join 时间区间范围时, 这个与是否设置 watermark 无关
      * 初始关联，只会回去关联主键下 ，最新状态的数据
      * 不会在输出端，产生2条结果，所以是有必要设置的watermark 的 ，无论那一侧有关联到结果，都会输出一条信息
      * join 的方式，只会在结果进行追加
      *
      *
      * 2. 假定只有在时间范围的数据得到关联
      * AND t1.create_time BETWEEN t2.create_time - INTERVAL '5' SECOND AND t2.create_time + INTERVAL '5' SECOND
      *
      * 只会获取能关联到的结果
      *
      * fixme：
      * IntervalJoin doesn't support consuming update and delete changes which is produced by node TableSourceScan(table=[[default_catalog, default_database, stream1]], fields=[id, activity_id, sku_id, timestamp, create_time])
      */
    val joinTable = tableEnv.sqlQuery(
      """
          SELECT
          t1.id,
          t1.activity_id,
          t1.sku_id,
          t2.activity_id_2,
          t2.sku_id_2
          FROM stream1 t1 , stream2 t2
          WHERE t1.id = t2.id AND
          t1.create_time BETWEEN t2.create_time - INTERVAL '4' HOUR AND t2.create_time
          """.stripMargin) // 只对 t1.sku_id =1 的数据进行关联

    //结果写入目标表中  fixme: 各个表中的主键是自更新的
    tableEnv.executeSql(
      """
        |CREATE TABLE test_streamjoinstream(
        |id INT NOT NULL,
        |activity_id INT,
        |sku_id INT,
        |activity_id_2 INT,
        |sku_id_2 INT,
        |PRIMARY KEY (id) NOT ENFORCED
        |)WITH(
        |'connector' = 'jdbc',
        |'driver'='com.mysql.jdbc.Driver',
        |'username'='root',
        |'password'='123456',
        |'url' = 'jdbc:mysql://hadoop105:3306/test',
        |'table-name' = 'test_streamjoinstream'
        |)
      """.stripMargin)

    // 打印输出结果， 最好能设置主键
    val table = tableEnv.sqlQuery("select * from test_streamjoinstream")
    val testStream = tableEnv.toRetractStream[Row](table)
    testStream.print("test")


    joinTable.executeInsert("test_streamjoinstream")


    env.execute("get join")
  }
}
