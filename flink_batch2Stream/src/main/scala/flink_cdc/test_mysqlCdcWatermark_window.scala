package flink_cdc

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
  * @author kylinWang
  * @data 2022-01-27 14:12
  *
  */
object test_mysqlCdcWatermark_window {

  def main(args: Array[String]): Unit = {

    /**
      * 1. flink-sql 设置watermark后
      * 2. 窗口内获取特征
      */

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

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



        // 数据都不能从CDC 这里获取？
        val windowTable = tableEnv.sqlQuery(
          """
            |select count(*) num,
            |TUMBLE_START(create_time,INTERVAL '10' SECOND) AS wStart
            |from stream1
            |group by TUMBLE(create_time,INTERVAL '10' SECOND)
          """.stripMargin)  // 只对 t1.sku_id =1 的数据进行关联

        //结果测试， 窗口聚合结果
        val testStream = tableEnv.toRetractStream[Row](windowTable)
        testStream.print("test")



//        //结果写入目标表中  fixme: 各个表中的主键是自更新的
//        tableEnv.executeSql(
//          """
//            |CREATE TABLE test_streamjoinstream(
//            |id INT NOT NULL,
//            |activity_id INT,
//            |sku_id INT,
//            |activity_id_2 INT,
//            |sku_id_2 INT,
//            |PRIMARY KEY (id) NOT ENFORCED
//            |)WITH(
//            |'connector' = 'jdbc',
//            |'driver'='com.mysql.jdbc.Driver',
//            |'username'='root',
//            |'password'='123456',
//            |'url' = 'jdbc:mysql://hadoop105:3306/test',
//            |'table-name' = 'test_streamjoinstream'
//            |)
//          """.stripMargin)
//
//        // 打印输出结果， 最好能设置主键
//        val table = tableEnv.sqlQuery("select * from test_streamjoinstream")
//        val testStream = tableEnv.toRetractStream[Row](table)
//        testStream.print("test")
//
//        joinTable.executeInsert("test_streamjoinstream")


    env.execute("get join")
  }
}
