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
object test_mysqlCdcStreamJoinStream2 {

  def main(args: Array[String]): Unit = {

    /**
      * 1. 2条流的关联
      * 2. 在给点时间范围或者窗口内对数据进行更新
      */

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    // 不设置 checkpoint

    //需要需要连接的 stream 1 , 定义一个eventtime , 基于evevmttime 进行关联
    // flink-sql 中，区分表名的大小写
    // flink-sql 中 ， 不天然支持 long 类型的数据直接设置为 timestamp（3） ； 时间格式 ddMMss 可以得到timestamp(3)
    tableEnv.executeSql(
      """
        |CREATE TABLE stream1(
        |id INT NOT NULL,
        |activity_id INT,
        |sku_id INT,
        |`timestamp` BIGINT,
        |create_time TIMESTAMP(3),
        |WATERMARK FOR create_time AS create_time - INTERVAL '1' SECOND,
        |PRIMARY KEY (id) NOT ENFORCED
        |)WITH(
        |'connector'='mysql-cdc',
        |'hostname'='hadoop105',
        |'port'='3306',
        |'username'='root',
        |'password'='123456',
        |'database-name'='test',
        |'table-name'='activity_sku'
        |)
      """.stripMargin)


    /**
      * 测试打印：如果不加水印，所有数据都可以打印输出
      *
        test:5> (true,2,199,1,1643266300,2022-01-27T14:53:06)
        test:4> (true,1,110,2,null,null)
        test:3> (true,1,99,1,1643266200,2022-01-27T14:53)
        test:6> (true,19,299,1,1643266400,2022-01-27T14:53:50)

      */

//    val table = tableEnv.sqlQuery("select * from stream1")
//    val testStream = tableEnv.toRetractStream[Row](table)
//    testStream.print("test")


    //需要需要连接的 stream 2
    tableEnv.executeSql(
      """
        |CREATE TABLE stream2(
        |id INT NOT NULL,
        |activity_id_2 INT,
        |sku_id_2 INT,
        |`timestamp` BIGINT,
        |create_time TIMESTAMP(3),
        |WATERMARK FOR create_time AS create_time - INTERVAL '1' SECOND,
        |PRIMARY KEY (id) NOT ENFORCED
        |)WITH(
        |'connector'='mysql-cdc',
        |'hostname'='hadoop105',
        |'port'='3306',
        |'username'='root',
        |'password'='123456',
        |'database-name'='test',
        |'table-name'='activity_sku2'
        |)
      """.stripMargin)

    /**
      *
      * 水印字段不能为空 ：
      * fixme:  RowTime field should not be null, please convert it to a non-null long value
      *
      */
//    val table = tableEnv.sqlQuery("select * from stream2")
//    val testStream = tableEnv.toRetractStream[Row](table)
//    testStream.print("test")


    /**
      * 1. 在没有设置 join 时间区间范围时, 这个与是否设置 watermark 无关
      *     初始关联，只会回去关联主键下 ，最新状态的数据
      *     不会在输出端，产生2条结果，所以是有必要设置的watermark 的 ，无论那一侧有关联到结果，都会输出一条信息
      *     join 的方式，只会在结果进行追加
      *
      *
      * 2. 假定只有在时间范围的数据得到关联
      *    AND t1.create_time BETWEEN t2.create_time - INTERVAL '5' SECOND AND t2.create_time + INTERVAL '5' SECOND
      *
      *    只会获取能关联到的结果
      *
      *    fixme：
      *    IntervalJoin doesn't support consuming update and delete changes which is produced by node TableSourceScan(table=[[default_catalog, default_database, stream1]], fields=[id, activity_id, sku_id, timestamp, create_time])
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
          """.stripMargin)  // 只对 t1.sku_id =1 的数据进行关联

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
