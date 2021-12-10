package FlinkSQL

import org.apache.flink.configuration.ConfigOption
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog


/**
  * @author kylinWang
  * @data 2021/3/15 22:08
  *
  * source:
  *2021-02-02 : Flink实战-Flink新特性之SQL Hive Streaming简单示例
  *2021-01-31 ：Flink集成Hive之Hive Catalog与Hive Dialect
  */
object FlinkHive {

  def main(args: Array[String]): Unit = {

    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    streamEnv.setParallelism(3)

    //创建表环境
    val tableEnvSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

   val tableEnv: StreamTableEnvironment =  StreamTableEnvironment.create(streamEnv,tableEnvSettings)

    //设置checkPoint
    tableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE,CheckpointingMode.EXACTLY_ONCE)
//    tableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL,Duration.ofSeconds(20))


    //fixme: 注册hive Catalog

    /**
      * 1. 什么是 hive_catalog
      * Hive使用Hive Metastore(HMS)存储元数据信息，使用关系型数据库来持久化存储这些信息。
      * 所以，Flink集成Hive需要打通Hive的metastore，去管理Flink的元数据，这就是Hive Catalog的功能。
      * Hive Catalog的主要作用是使用Hive MetaStore去管理Flink的元数据。
      * Hive Catalog可以将元数据进行持久化，这样后续的操作就可以反复使用这些表的元数据，
      * 而不用每次使用时都要重新注册。如果不去持久化catalog，那么在每个session中取处理数据，
      * 都要去重复地创建元数据对象，这样是非常耗时的
      *
      * 2.如何使用 hive_catalog
      * HiveCatalog是开箱即用的，所以，一旦配置好Flink与Hive集成，就可以使用HiveCatalog
      * 比如，我们通过FlinkSQL 的DDL语句创建一张kafka的数据源表，立刻就能查看该表的元数据信息
      *
      * HiveCatalog可以处理两种类型的表：一种是Hive兼容的表，(对于Hive兼容表而言，我们既可以使用Flink去操作该表，又可以使用Hive去操作该表)
      * 另一种是普通表(generic table) : 不能通过Hive去处理这些表，因为语法不兼容
      *
      * 对于是否是普通表，Flink使用is_generic属性进行标识。
      * fixme: 默认情况下，创建的表是普通表，即is_generic=true，说明该表是一张普通表，如果在Hive中去查看该表，则会报错
      * 如果要创建Hive兼容表，需要在建表属性中指定 is_generic=false
      *
      *
      */

    val catalogName = "my_catalog"
    val catalog = new HiveCatalog(
      catalogName,
      "default", //database
      "/Users/lmagic/develop", //path
      "1.1.0" //version, 此项可以没有
    )

    //注册表
    tableEnv.registerCatalog(catalogName,catalog) //注册函数
    tableEnv.useCatalog(catalogName)

println("------------------------------------------------------------------")

    //fixme: 1. 创建Kafka流表
    tableEnv.execute("CREATE DATABASE IF NOT EXISTS stream_tmp")
    tableEnv.execute("DROP TABLE IF EXISTS stream_tmp.analytics_access_log_kafka")

    tableEnv.execute( """
      |CREATE TABLE stream_tmp.analytics_access_log_kafka (
      |  ts BIGINT,
      |  userId BIGINT,
      |  eventType STRING,
      |  fromType STRING,
      |  columnType STRING,
      |  siteId BIGINT,
      |  grouponId BIGINT,
      |  partnerId BIGINT,
      |  merchandiseId BIGINT,
      |  procTime AS PROCTIME(),
      |  eventTime AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000,'yyyy-MM-dd HH:mm:ss')),
      |  WATERMARK FOR eventTime AS eventTime - INTERVAL '15' SECOND
      |) WITH (
      |  'connector' = 'kafka',
      |  'topic' = 'ods_analytics_access_log',
      |  'properties.bootstrap.servers' = 'kafka110:9092,kafka111:9092,kafka112:9092'
      |  'properties.group.id' = 'flink_hive_integration_exp_1', //'group1'
      |  'scan.startup.mode' = 'latest-offset',  //'earliest-offset'
      |  'format' = 'json',
      |  'json.fail-on-missing-field' = 'false',
      |  'json.ignore-parse-errors' = 'true',
      |  'is_generic' = 'false'  //显示的声明为兼容表，则可以在hive查询
   """.stripMargin)

    //由于已经注册了HiveCatalog,  在Hive中可以观察到创建的Kafka流表的元数据
    //hive> describe formatted stream_tmp.analytices_access_log_kafka

    //fixme: 兼容表： 我们可以使用FlinkSQL Cli或者HiveCli向该表中写入数据，
    // 然后分别通过FlinkSQL Cli和Hive Cli去查看该表数据的变化

    //重点：
//    Hive CHAR(p) 类型的最大长度为255
//      Hive VARCHAR(p)类型的最大长度为65535
//      Hive MAP类型的key仅支持基本类型，而Flink’s MAP 类型的key执行任意类型
//    Hive不支持联合数据类型，比如STRUCT
//    Hive’s TIMESTAMP 的精度是 9 ， Hive UDFs函数只能处理 precision <= 9的 TIMESTAMP 值
//      Hive 不支持 Flink提供的 TIMESTAMP_WITH_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE, 及MULTISET类型
//    FlinkINTERVAL 类型与 Hive INTERVAL 类型不一样


    println("------------------------------------------------------------------")


    //2. 创建 Hive 表
    /**
      * 1. 什么
      * 开启了Hive dialect配置，用户就可以使用HiveQL语法，这样我们就可以在Flink中使用Hive的语法使用一些DDL和DML操作
      *
      * Flink目前支持两种SQL方言(SQL dialects)：default 和 hive
      * 默认的SQL方言是default，如果要使用Hive的语法，需要将SQL方言切换到hive
      *
      * Flink SQL> set table.sql-dialect=hive; -- 使用hive dialect
      * Flink SQL> set table.sql-dialect=default; -- 使用default dialect
      *
      */
    tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)

    //flink 1.11 特性： executeSql
    tableEnv.execute("CREATE DATABASE IF NOT EXISTS hive_tmp")
    tableEnv.execute("DROP TABLE IF EXISTS hive_tmp.analytics_access_log_hive")

    //执行
    tableEnv.execute(
      """
        |CREATE TABLE hive_tmp.analytics_access_log_hive (
        |  ts BIGINT,
        |  user_id BIGINT,
        |  event_type STRING,
        |  from_type STRING,
        |  column_type STRING,
        |  site_id BIGINT,
        |  groupon_id BIGINT,
        |  partner_id BIGINT,
        |  merchandise_id BIGINT
        | ) PARTITIONED BY (
        |  ts_date STRING,
        |  ts_hour STRING,
        |  ts_minute STRING
        |) STORED AS PARQUET
        | TBLPROPERTIES (
        |  'sink.partition-commit.trigger' = 'partition-time',
        |  'sink.partition-commit.delay' = '1 min',
        |  'sink.partition-commit.policy.kind' = 'metastore,success-file',
        |  'partition.time-extractor.timestamp-pattern' = '$ts_date $ts_hour:$ts_minute:00'
        |)
      """.stripMargin
    )

    // sink.partition-commit.trigger ： 触发分区提交的时间特征
    // partition.time-extractor.timestamp-pattern：分区时间戳的抽取格式
    // sink.partition-commit.delay：触发分区提交的延迟
    // sink.partition-commit.policy.kind：分区提交策略


    //知识点
//    Hive dialect只能用于操作Hive表，不能用于普通表。Hive方言应与HiveCatalog一起使用。
//    虽然所有Hive版本都支持相同的语法，但是是否有特定功能仍然取决于使用的Hive版本。例如，仅在Hive-2.4.0或更高版本中支持更新数据库位置。
//    Hive和Calcite具有不同的保留关键字。例如，default在Calcite中是保留关键字，在Hive中是非保留关键字。所以，在使用Hive dialect时，必须使用反引号（`）引用此类关键字，才能将其用作标识符。
//    在Hive中不能查询在Flink中创建的视图。


    //fixme: 3. 流数据写入 Hive， kafka 数据写入hive
    tableEnv.getConfig.setSqlDialect(SqlDialect.DEFAULT)
    //插入数据
    tableEnv.execute(
      """
        |INSERT INTO hive_tmp.analytics_access_log_hive
        |SELECT
        |  ts,userId,eventType,fromType,columnType,siteId,grouponId,partnerId,merchandiseId,
        |  DATE_FORMAT(eventTime,'yyyy-MM-dd'),
        |  DATE_FORMAT(eventTime,'HH'),
        |  DATE_FORMAT(eventTime,'mm')
        |FROM stream_tmp.analytics_access_log_kafka
        |WHERE merchandiseId > 0
      """.stripMargin
    )

//备注：说明
    //checkpoint interval是20秒，可以看到，上图中的数据文件恰好是以20秒的间隔写入的。由于并行度为3，所以每次写入会生成3个文件
//分区内所有数据写入完毕后，会同时生成_SUCCESS文件。如果是正在写入的分区，则会看到.inprogress文件

    //fixme: 4. 流式读取 Hive; Hive表作为流式Source，需要启用dynamic table options
//  tableEnv.getConfig.getConfiguration.setBoolean(
//    config.TABLE_DYNAMIC_TABLE_OPTIONS_ENABLED,true
//  )

val result = tableEnv.sqlQuery(
  """
    |SELECT merchandise_id,count(1) AS pv
    |FROM hive_tmp.analytics_access_log_hive
    |/*+ OPTIONS(
    |  'streaming-source.enable' = 'true',
    |  'streaming-source.monitor-interval' = '1 min',
    |  'streaming-source.consume-start-offset' = '2020-07-15 23:30:00'
    |)  --注解 */
    |WHERE event_type = 'shtOpenGoodsDetail'
    |AND ts_date >= '2020-07-15'
    |GROUP BY merchandise_id
    |ORDER BY pv DESC LIMIT 10
  """.stripMargin
)

    //streaming-source.enable：设为true，表示该Hive表可以作为Source。
// streaming-source.monitor-interval：感知Hive表新增数据的周期
   //streaming-source.consume-start-offset：开始消费的时间戳

    //表数据输出
//    result[Row].print().setParallelism(1)


    streamEnv.execute()

    //result: Flink 1.11的Hive Streaming功能大大提高了Hive数仓的实时性，对ETL作业非常有利，同时还能够满足流式持续查询的需求，具有一定的灵活性

  }
}
