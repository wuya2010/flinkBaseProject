package flink_cdc

import java.sql.{Connection, DriverManager, PreparedStatement}

import flink_source.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

/**
  * @author kylinWang
  * @data 2022-01-27 10:46
  *
  */
object test_mysqlCdc_field {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.enableCheckpointing(3000l)
    val config = env.getCheckpointConfig
    config.setMinPauseBetweenCheckpoints(3000l)
    // chenckpoint 不重复读取数据
    config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //The scheme (hdfs://, file://, etc)  mycluster/flink/checkpoint
    // file:///data/flink/checkpoints  --linux 的路径
    env.setStateBackend(new FsStateBackend("file:///flink/checkpoints"))

    val tableEnv = StreamTableEnvironment.create(env)

    /**
      * 1. 可以在创建表时，只选择需要的字段, 可以正常读取
      */
    tableEnv.executeSql(
      """
        |CREATE TABLE test2(
        |id INT NOT NULL
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


    val table = tableEnv.sqlQuery("select * from test2")


    val retStream = tableEnv.toRetractStream[Row](table)

    // 是否每次重启，都会重新开始读数据？
    retStream.print("mysql")


    //将获取的结果写入表中, 表数据不能通过cdc写入目标表, 遇到问题考虑配置对不对
    tableEnv.executeSql(
      """
        |CREATE TABLE rettable(
        |id INT NOT NULL,
        |PRIMARY KEY (id) NOT ENFORCED
        |)WITH(
        |'connector' = 'jdbc',
        |'driver'='com.mysql.jdbc.Driver',
        |'username'='root',
        |'password'='123456',
        |'url' = 'jdbc:mysql://hadoop105:3306/test',
        |'table-name' = 'mysql_cdc_ret'
        |)
      """.stripMargin)


    /**
      * executeInsert:
      *  SQL 查询中的 INSERT INTO 子句类似，该方法执行对已注册的输出表的插入操作。executeInsert() 方法将立即提交执行插入
      *
      *  fixme: please declare primary key for sink table when query contains update/delete record.
      */

    val stream = env.fromCollection(Seq(1,2,3))
    stream.print("test")
    val sourceTable = tableEnv.fromDataStream(stream)

    //方法1：mysql-cdc 的表数据写入目标表  -- fixme: (1) 如果原表的数据删除，则目标表数据也会清除， （2）如果原表数据更改，目标表也会更改
//    table.executeInsert("rettable")

    // 方法2：数据怎么写入表中,
    tableEnv.executeSql("""insert into rettable select * from test2""")


    env.execute("test")
  }

}

/*
    eg：

            EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        String schema = "id BIGINT ,name STRING, password STRING,age INT";
        String source_table = "student";
        String sink_table = "studentcp";
        String flink_source_table = "mysource";
        String flink_sink_table = "mysink";

        String base_sql = "CREATE TABLE %s (%s) " +
                "WITH (" +
                "'connector.type' = 'jdbc'," +
                "'connector.url' = 'jdbc:mysql://127.0.0.1:3306/feature'," +
                "'connector.driver' = 'com.mysql.cj.jdbc.Driver'," +
                "'connector.table' = '%s'," +
                " 'connector.username' = 'root'," +
                " 'connector.password' = '123456'" +
                " )";
        String source_ddl = String.format(base_sql, flink_source_table, schema, source_table);
        String sink_ddl = String.format(base_sql, flink_sink_table, schema, sink_table);

        tableEnvironment.executeSql(source_ddl);
        tableEnvironment.executeSql(sink_ddl);

        String insertsql = String.format("insert into %s select * from %s", flink_sink_table, flink_source_table);
        tableEnvironment.executeSql(insertsql).print();



 */