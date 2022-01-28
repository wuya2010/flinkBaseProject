package flink_cdc

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
  * @author kylinWang
  * @data 2022-01-27 21:03
  *
  */
object test_kafka_window2 {

  def main(args: Array[String]): Unit = {

    /**
      * 1. 根据2个 kafka 流, 进行 inner-join
      * 2. 最终结果写入 mysql 中
      */

    //构建kafka 的连机器
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    // 输入流1
    tableEnv.executeSql(
      """
        |CREATE TABLE kafkaTable1(
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

    tableEnv.executeSql(
      """
        |CREATE TABLE kafkaTable2(
        |`user_id` BIGINT,
        |`item_id_2` BIGINT,
        |`behavior_2` STRING,
        |`ts` TIMESTAMP(3) METADATA FROM 'timestamp',
        |WATERMARK FOR ts AS ts
        |)WITH(
        |'connector'='kafka',
        |'topic'='start',
        |'properties.bootstrap.servers'='hadoop105:9092',
        |'properties.group.id'='testGroup',
        |'scan.startup.mode'='latest-offset',
        |'format'='csv'
        |)
      """.stripMargin)


    // 输出流2
    // 输出： 只能在结果范围输出  kafka:9> 1,2,tom,3,jim
    /**
      * kafka:9> 1,2,tom,3,jim
      * kafka:9> 1,3,tom,4,jim
      * kafka:9> 1,5,tom,6,jim
      */
    val retTable = tableEnv.sqlQuery(
      """
        | select t1.user_id , item_id ,behavior , item_id_2, behavior_2
        | from kafkaTable1 t1, kafkaTable2 t2
        | WHERE t1.user_id = t2.user_id AND
        | t1.ts BETWEEN t2.ts - INTERVAL '10' SECOND AND  t2.ts + INTERVAL '5' SECOND
      """.stripMargin)

//    val table = tableEnv.sqlQuery("select * from kafkaTable2");

    val getStream = tableEnv.toAppendStream[Row](retTable)
    getStream.print("kafka")

    env.execute("get kafka")
  }

}
