package flink_cdc

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
  * @author kylinWang
  * @data 2022-01-28 9:48
  *
  */
object test_tempTable {

  def main(args: Array[String]): Unit = {

    /**
      * 1. 从mysql-cdc 读取表数据
      * 2. 设置成时态表
      * 3. 进行关联
      *
      * 结论： cdc 支持时态表，这对于维度表的关联具有重要意义
      */
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  val tableEnv = StreamTableEnvironment.create(env)

  //根据格式连接cdc的表 : 可以根据字段读取到表的内容
   tableEnv.executeSql(
     """
       |CREATE TABLE temp_order (
       | `product_id` STRING,
       | `order_id` STRING NOT NULL,
       | `order_time` TIMESTAMP(3),
       | WATERMARK FOR order_time AS order_time
       |)WITH(
       |'connector'='mysql-cdc',
       |'hostname'='hadoop105',
       |'port'='3306',
       |'username'='root',
       |'password'='123456',
       |'database-name'='test',
       |'table-name'='temp_orders'
       |)
     """.stripMargin)


    //读取一个表的内容
    tableEnv.executeSql(
      """
        |CREATE TABLE temp_product (
        |product_id STRING,
        |product_name STRING,
        |price DECIMAL(10,2),
        |update_time TIMESTAMP(3),
        |PRIMARY KEY (product_id) NOT ENFORCED,
        |WATERMARK FOR update_time AS update_time
        |)WITH(
        |'connector'='mysql-cdc',
        |'hostname'='hadoop105',
        |'port'='3306',
        |'username'='root',
        |'password'='123456',
        |'database-name'='test',
        |'table-name'='temp_product_changelog'
        |)
      """.stripMargin)

    //读取表数据
//    val table = tableEnv.sqlQuery("select * from temp_product")
//    val testStream = tableEnv.toRetractStream[Row](table)


    /**
      * test:6> (true,1001,2022-01-27T10:03:23,kk,9.00,2022-01-27T09:27:32)  --小于前面的时间的最新状态
      * test:6> (true,1001,2022-01-28T10:31:22,mm,8.00,2022-01-28T10:28:50)
      * test:6> (true,1001,2022-01-28T10:32:39,mm,8.00,2022-01-28T10:28:50)
      * test:6> (true,1001,2022-01-28T10:35:13,oo,118.00,2022-01-28T10:32:57)
      *
      * fixme: 版本表的关联：
      * 1. 定义主表中的 watermark ， 2个表根据 watermak 进行关联
      * 2. 根据主表中的 事件时间，在从表中，需要小于主表时间戳的最新状态的数据
      * 3. 从何达到获取最新状态数据的效果，随着水印的推进，输出结果， 达到一侧水位时，窗口关闭输出
      *
      */
    val table =  tableEnv.sqlQuery(
      """
        |select
        |t1.product_id,
        |t1.order_time,
        |t2.product_name,
        |t2.price,
        |t2.update_time
        |FROM temp_order t1
        |LEFT JOIN temp_product FOR SYSTEM_TIME AS OF t1.order_time AS t2
        |ON t1.product_id = t2.product_id
      """.stripMargin)

    //水印的作用是什么？版本表， 基于watermakr 进行关联， 什么范围的数据进行关联 ？
    val testStream = tableEnv.toRetractStream[Row](table)
    testStream.print("test")

    env.execute("temp")
  }
}
