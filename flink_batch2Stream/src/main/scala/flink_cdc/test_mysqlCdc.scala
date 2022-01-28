package flink_cdc


// 类的导入要注意
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row




/**
  * @author kylinWang
  * @data 2022-01-26 23:34
  *
  */
object test_mysqlCdc {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 创建表环境
    val tableEnv = StreamTableEnvironment.create(env)

    val tableSql =
      """
        |CREATE TABLE TEST(
        |id INT NOT NULL,
        |activity_id INT,
        |sku_id INT
        |)WITH(
        |'connector'='mysql-cdc',
        |'hostname'='hadoop105',
        |'port'='3306',
        |'username'='root',
        |'password'='123456',
        |'database-name'='test',
        |'table-name'='activity_sku'
        |)
      """.stripMargin

    tableEnv.executeSql(tableSql)
    val result = tableEnv.sqlQuery("select * from TEST")
    /**
      * 了解cdc的特性
      *
      * Exception in thread "main" org.apache.flink.table.api.TableException:
      * toAppendStream doesn't support consuming update and delete changes which is produced by node TableSourceScan(table=[[default_catalog, default_database, TEST]], fields=[id, activity_id, sku_id])
      */
//    val retStream = tableEnv.toAppendStream[Row](result)

    val retStream = tableEnv.toRetractStream[Row](result)

    retStream.print("mysql")

    env.execute("get tables")

  }
}
