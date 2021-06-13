import java.math.BigInteger
import java.security.MessageDigest
import java.util.Collections
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import com.alibaba.fastjson.JSONObject
import com.atguigu.education.model.GlobalConfig
import com.atguigu.education.util.ParseJsonData
import com.google.common.cache.{Cache, CacheBuilder}
import com.yld.fwarehourse.util.ParseJsonData
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.util.ExecutorUtils
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Put}
import org.apache.hadoop.hbase.util.Bytes

/**
  * todo： flink 异步io join hbase
  * 根据官方推荐 异步io写法 使用异步客户端 如果没有异步客户自己可以写多线程实现
  */
class DimHbaseAsyncFunction extends RichAsyncFunction[String, String] {

  var executorService: ExecutorService = _  //创建连接池
  var cache: Cache[String, String] = _

  //使用opentsdb 的 异步客户端  http://opentsdb.github.io/asynchbase/javadoc/index.html

  override def open(parameters: Configuration): Unit = {
    //    val hbase_config = new Config()
    //    hbase_config.overrideConfig("hbase.zookeeper.quorum", GlobalConfig.HBASE_ZOOKEEPER_QUORUM)
    //    hbase_config.overrideConfig("hbase.zookeeper.property.clientPort", GlobalConfig.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)


    //    //创建连接
    //    hbaseClient = new HBaseClient(hbase_config)
    //初始化线程池 12个线程
    executorService = Executors.newFixedThreadPool(12)
    //初始化缓存： 目的 减小对hbase 的查询
    cache = CacheBuilder.newBuilder()
      .concurrencyLevel(12) //设置并发级别 允许12个线程同时访问
      .expireAfterAccess(2, TimeUnit.HOURS) //设置缓存 2小时 过期
      .maximumSize(10000) //设置缓存大小
      .build()
  }

  //优雅关闭线程池
  override def close(): Unit = {
    ExecutorUtils.gracefulShutdown(100, TimeUnit.MILLISECONDS, executorService);
  }



  override def timeout(input: String, resultFuture: ResultFuture[String]): Unit = {
    resultFuture.complete(Array("timeout:" + input))
  }

  //异步invoke: 每条数据的处理
  override def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {

    executorService.submit(new Runnable {
      override def run(): Unit = {
        try {
          val conf = HBaseConfiguration.create()
          conf.set("hbase.zookeeper.quorum", GlobalConfig.HBASE_ZOOKEEPER_QUORUM)
          conf.set("hbase.zookeeper.property.clientPort", GlobalConfig.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT)
          val connection = ConnectionFactory.createConnection(conf)
          //封装方法： 查询hbase维度表数据，得到关联好的数据
          val resultJsonObject = getHbaseJoinData(input, connection, cache)

          resultFuture.complete(Array(resultJsonObject.toJSONString))
          // 写入到宽表， 考虑rowkey 的设计
          writeDataToHbase(connection, resultJsonObject)
        } catch {
          case e: Exception => resultFuture.complete(Array("error:" + e.printStackTrace()))
        }
      }
    })
  }






  /**
    * 查询hbase维度表数据
    *
    * @param input
    * @param connection
    * @param cache
    * @return
    */
  def getHbaseJoinData(input: String, connection: Connection, cache: Cache[String, String]): JSONObject = {
    val jsonObject = ParseJsonData.getJsonData(input)
    //通过adid  siteid vipid 去hbase维度表查询三个表的数据
    val ad_id = jsonObject.getString("ad_id")
    val siteid = jsonObject.getString("siteid")
    val vip_id = jsonObject.getString("vip_id")
    val dn = jsonObject.getString("dn")

    //todo: 初始化名称，给空值
    var adname: String = ""
    var sitename: String = ""
    var siteurl: String = ""
    var delete: String = ""
    var site_createtime: String = ""
    var site_creator: String = ""
    var vip_level: String = ""
    var vip_start_time: String = ""
    var vip_end_tiem: String = ""
    var last_modify_time = ""
    var max_free = ""
    var min_free = ""
    var next_level = ""
    var operator = ""

    //todo: 1---查询广告关联表 adname 先去缓存中查询 如果缓存中没有再去hbase中查询
    adname = cache.getIfPresent("adname:" + ad_id + "_" + dn)
    //如果缓存里面没有，则从hbase里面取出来
    if (adname == null || "".equals(adname)) {
      val table = connection.getTable(TableName.valueOf("education:dwd_basead"))
      //根据rowkey 查询数据
      val get = new Get(Bytes.toBytes(ad_id + "_" + dn)).addColumn(Bytes.toBytes("info"), Bytes.toBytes("adname"))
     //查询，获得结果集
      val result = table.get(get)
      // 遍历，获取admna
      for (cell <- result.rawCells()) {
        adname = Bytes.toString(CellUtil.cloneValue(cell))
      }
      //广告 name 放入缓存中： 以减小hbase的查询，可以复用数据
      cache.put("adname:" + ad_id + "_" + dn, adname) //放入缓存
      table.close()
    }


    //todo: 2---查询网站关联表 sitename stiteurl等信息
    if (!"".equals(siteid)) {
      val siteDetail = cache.getIfPresent("siteDetail:" + siteid + "_" + dn) //先从缓存中取
      if (siteDetail == null || "".equals(siteDetail)) {
        val table = connection.getTable(TableName.valueOf("education:dwd_basewebsite"))
        val get = new Get(Bytes.toBytes(siteid + "_" + dn)).addFamily(Bytes.toBytes("info"))
        val result = table.get(get)
        for (cell <- result.rawCells()) {
          val colName = Bytes.toString(CellUtil.cloneQualifier(cell))
          val value = Bytes.toString(CellUtil.cloneValue(cell))
          colName match {
            case "sitename" => sitename = value
            case "siteurl" => siteurl = value
            case "delete" => delete = value
            case "createtime" => site_createtime = value
            case "creator" => site_creator = value
            case _ => null
          }
        }
        //将查询到的数据拼装成json格式 存入缓存
        val jsonObject = new JSONObject()
        jsonObject.put("sitename", sitename)
        jsonObject.put("siteurl", siteurl)
        jsonObject.put("delete", delete)
        jsonObject.put("site_createtime", site_createtime)
        jsonObject.put("site_creator", site_creator)
        cache.put("siteDetail:" + siteid + "_" + dn, jsonObject.toJSONString)
        table.close()
      } else {
        //如果缓存中有数据 则解析缓存中的json数据
        val jsonObject = ParseJsonData.getJsonData(siteDetail)
        sitename = jsonObject.getString("sitename")
        siteurl = jsonObject.getString("siteurl")
        delete = jsonObject.getString("delete")
        site_createtime = jsonObject.getString("site_createtime")
        site_creator = jsonObject.getString("site_creator")
      }
    }


    //todo: 3----vip表关联的数据
    if (!"".equals(vip_id)) {
      val vipDetail = cache.getIfPresent("vipDetail:" + vip_id + "_" + dn) //先查询缓存
      if (vipDetail == null || "".equals(vipDetail)) {
        val table = connection.getTable(TableName.valueOf("education:dwd_membervip"))
        val get = new Get(Bytes.toBytes(vip_id + "_" + dn)).addFamily(Bytes.toBytes("info"))
        val result = table.get(get)
        for (cell <- result.rawCells()) {
          val colName = Bytes.toString(CellUtil.cloneQualifier(cell))
          val value = Bytes.toString(CellUtil.cloneValue(cell))
          colName match {
            case "vip_level" => vip_level = value
            case "vip_start_time" => vip_start_time = value
            case "vip_end_time" => vip_end_tiem = value
            case "last_modify_time" => last_modify_time = value
            case "max_free" => max_free = value
            case "min_free" => min_free = value
            case "next_level" => next_level = value
            case "operator" => operator = value
            case _ => null
          }
        }
        //将查询到的数据拼装成json 存入缓存
        val jsonObject = new JSONObject()
        jsonObject.put("vip_level", vip_level)
        jsonObject.put("vip_start_time", vip_start_time)
        jsonObject.put("vip_end_tiem", vip_end_tiem)
        jsonObject.put("last_modify_time", last_modify_time)
        jsonObject.put("max_free", max_free)
        jsonObject.put("min_free", min_free)
        jsonObject.put("next_level", next_level)
        jsonObject.put("operator", operator)
        println(jsonObject.toJSONString)
        cache.put("vipDetail:" + vip_id + "_" + dn, jsonObject.toJSONString)
        table.close()
      } else {
        //如果缓存中有值 就解析缓存中的数据
        val jsonObject = ParseJsonData.getJsonData(vipDetail)
        vip_level = jsonObject.getString("vip_level")
        vip_start_time = jsonObject.getString("vip_start_time")
        vip_end_tiem = jsonObject.getString("vip_end_tiem")
        last_modify_time = jsonObject.getString("last_modify_time")
        max_free = jsonObject.getString("max_free")
        min_free = jsonObject.getString("min_free")
        next_level = jsonObject.getString("next_level")
        operator = jsonObject.getString("operator")
      }
    }

    //总得 json, 达到 hbase 维度表关联的效果
    jsonObject.put("adname", adname)
    jsonObject.put("sitename", sitename)
    jsonObject.put("siteurl", siteurl)
    jsonObject.put("delete", delete)
    jsonObject.put("site_createtime", site_createtime)
    jsonObject.put("site_creator", site_creator)
    jsonObject.put("vip_level", vip_level)
    jsonObject.put("vip_start_time", vip_start_time)
    jsonObject.put("vip_end_tiem", vip_end_tiem)
    jsonObject.put("last_modify_time", last_modify_time)
    jsonObject.put("max_free", max_free)
    jsonObject.put("min_free", min_free)
    jsonObject.put("next_level", next_level)
    jsonObject.put("operator", operator)
    //返回最终结果
    jsonObject
  }






  /**
    * 将数据写入hbase
    *
    * @param connection
    * @param resultJsonObject
    */
  def writeDataToHbase(connection: Connection, resultJsonObject: JSONObject) = {
    val table = connection.getTable(TableName.valueOf("education:dim_member"))
    //rowkey  substring(md5(uid),0,5)+uid+dn  //md5: 加密
    val uid = resultJsonObject.getString("uid")
    val dn = resultJsonObject.getString("dn")
    // 自定义加密方法： generateHash 散列
    val rowkey = generateHash(uid).substring(0, 5) + uid + dn  //rowkey设计： 散列，唯一， 不能长
    val put = new Put(Bytes.toBytes(rowkey))
    // 转数组
    val keySet = resultJsonObject.keySet().toArray
    for (key <- keySet) {
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(key.toString),
        Bytes.toBytes(resultJsonObject.getString(key.toString)))
    }
    //数据插入
    table.put(put)
    table.close()
  }





  /**
    * 对字符串进行MD5加密
    *  hash : 16 进制
    * @param input
    * @return
    */
  def generateHash(input: String): String = {
    try {
      if (input == null) {
        null
      }
      val md = MessageDigest.getInstance("MD5")
      md.update(input.getBytes());
      val digest = md.digest();
      val bi = new BigInteger(1, digest);
      var hashText = bi.toString(16);
      while (hashText.length() < 32) {
        hashText = "0" + hashText;
      }
      hashText
    } catch {
      case e: Exception => e.printStackTrace(); null
    }
  }
}
