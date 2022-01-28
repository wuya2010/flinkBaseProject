package flink_cdc

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
  * @author kylinWang
  * @data 2022-01-27 21:03
  *
  */
object test_kafka_window {

  def main(args: Array[String]): Unit = {

    //构建kafka 的连机器
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    // 获取各种不同的数据连接  --元数据、
    // 输入 -->1,1,he  打印输出： 1,1,he,2022-01-27T21:55:07.346
    tableEnv.executeSql(
      """
        |CREATE TABLE kafkaTable(
        |`user_id` BIGINT,
        |`item_id` BIGINT,
        |`behavior` STRING,
        |`ts` TIMESTAMP(3) METADATA FROM 'timestamp',
        |WATERMARK FOR ts AS ts
        |)WITH(
        |'connector'='kafka',
        |'topic'='test',
        |'properties.bootstrap.servers'='hadoop105:9092',
        |'properties.group.id'='testGroup',
        |'scan.startup.mode'='latest-offset',
        |'format'='csv'
        |)
      """.stripMargin)


    //fixme: 基本数据输出：  kafka:11> 1,2022-01-27T21:58:39.129
//    val table = tableEnv.sqlQuery(
//      """
//        |select user_id , ts
//        |from kafkaTable
//      """.stripMargin)


    //fixme: 2没有窗口的情况下，来一条更新一条
//    val table = tableEnv.sqlQuery(
//      """
//        |select user_id , count(*) num
//        |from kafkaTable
//        |group by user_id
//      """.stripMargin)


    //fixme: 3 获取窗口内的聚合
    /**
      * 根据 kafka 输入生成对应窗口的聚合函数
      * kafka:9> (true,1,2022-01-27 10:01:40,1)
      * kafka:9> (true,2,2022-01-27 10:01:40,1)
      * kafka:9> (true,1,2022-01-27 10:01:50,1)
      * kafka:9> (true,2,2022-01-27 10:01:00,1)
      * kafka:3> (true,4,2022-01-27 10:01:10,1)
      *
      */
    val table = tableEnv.sqlQuery(
          """
            |select user_id , date_format(TUMBLE_START(ts,INTERVAL '10' SECOND),'yyyy-MM-dd hh:MM:ss') AS wStart
            |, count(*) num
            |from kafkaTable
            |GROUP BY TUMBLE(ts,INTERVAL '10' SECOND),user_id
          """.stripMargin)

    // fixme2: 聚合不再支持 append 的方式，toAppendStream doesn't support consuming update changes
//    val getStream = tableEnv.toAppendStream[Row](table)
    val getStream = tableEnv.toRetractStream[Row](table)
    getStream.print("kafka")

    env.execute("get kafka")
  }

}
