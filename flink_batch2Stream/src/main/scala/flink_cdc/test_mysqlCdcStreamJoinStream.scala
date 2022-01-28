package flink_cdc

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
  * @author kylinWang
  * @data 2022-01-27 14:12
  *
  */
object test_mysqlCdcStreamJoinStream {

  def main(args: Array[String]): Unit = {

    /**
      * 1. 2条流的关联
      * 2. 对于有时间区间的流和没有时间区间的流，怎么去规范定义
      * 3. flink-sql 界面中，中查询数据只能获取到静态的数据，不能实时更新到新的结果
      */

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    // 不设置 checkpoint

    //需要需要连接的 stream 1 , 定义一个eventtime , 基于evevmttime 进行关联
    tableEnv.executeSql(
      """
        |CREATE TABLE stream1(
        |id INT NOT NULL,
        |activity_id INT,
        |sku_id INT,
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

    // 测试打印
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
      * 初始关联时， 会根据时间状态获取最新状态的数据进行关联，类似分组最最新的操作
      *
      *
      * 动态流写入
      * 对于 inner join: join与关联是不一致的
      *  1. 对于有唯一主键的表，流的数据更新会对主键关联数据进行同步更新；
      *  2. 假如不设置主键，用 id 进行关联， 无论左侧流或者右侧流有数据更新，都会在结果表输出新的 id 关联结果，更新表
      *     为新的数据，非更新表为关联到的 id 下最新状态的数据
      *  3. 假如加入限制条件, 用where作为限制条件，也可以达到想要的效果，只是窗口太大，对于状态的保存会一直得不到关闭
      *
      *
      *  对于 left join:  fixme: 不管流的输入先后， 流都会去关联的表找结果
      *  1. 假如删掉某一个 id 的数据， 结果表中的对应的 id 的数据都会被删除
      *  2. 左表主表有新的输入，结果会同步输出1条结果到目标表， 右表如果能关联，则效果和前面一样，
      *     如果是无关联的输入，则不会有输出
      */
    val joinTable = tableEnv.sqlQuery(
      """
      SELECT
      t1.id,
      t1.activity_id,
      t1.sku_id,
      t2.activity_id_2,
      t2.sku_id_2
      FROM stream1 t1
      LEFT JOIN stream2 t2
      ON t1.id = t2.id
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

    // 没有执行
    val table = tableEnv.sqlQuery("select * from test_streamjoinstream")
    val testStream = tableEnv.toRetractStream[Row](table)
    testStream.print("test")


    joinTable.executeInsert("test_streamjoinstream")


    env.execute("get join")
  }
}
