package main.scala.com.yld.fwarehourse.bean

/**
  * @author kylinWang
  * @data 2020/7/27 10:44
  *
  */
case class BaseViplevel(vip_id: Int, vip_level: String, start_time: String,
                        end_time: String, last_modify_time: String, max_free: String,
                        min_free: String, next_level: String, operator: String, dn: String)
