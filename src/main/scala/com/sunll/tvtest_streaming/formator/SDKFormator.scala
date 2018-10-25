package com.sunll.tvtest_streaming.formator
import java.net.URLDecoder

import com.alibaba.fastjson.{JSON, JSONObject}
import com.sunll.tvtest_streaming.utils.{IPParser, SDKDecoder}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, Map}

/**
  * format the log string on all fields, then match we need
  * @author sunliangliang 2018-09-16
  * @version 1.0
  *
  */
class SDKFormator extends LogFormator {

  val logger = LoggerFactory.getLogger(this.getClass)

  val logRegex = """(\d+\.\d+\.\d+\.\d+).*?logger.php\?(.*?) HTTP.*""".r

  /**
    * 格式化、清洗日志
    * @param logStr 原始log
    * @return 返回包含结果的list
    */
  override def format(logStr: String, ipAreaIspCache: Array[String], fieldsMap: Map[String, ListBuffer[(String, Int)]]): (String, ListBuffer[String]) = {
    //不采用传入的引用，而是从主存中拿
    var paramMap: mutable.Map[String, String] = mutable.Map[String, String]()
    var appkey = "-"
    try{
      val logRegex(ip, query) = logStr
      val fieldsLogList = query.split("&").toList
      fieldsLogList.map(x => paramMap += x.split("=")(0) -> x.split("=")(1))
      val tup = IPParser.parse(ip, ipAreaIspCache)
      paramMap += "country" -> tup._1
      paramMap += "province" -> tup._2
      paramMap += "city" -> tup._3
      paramMap += "isp" -> tup._4
      if(!paramMap.getOrElse("enc", "0").equals("0")){
        paramMap("log") = SDKDecoder.decode(paramMap("log"))
        paramMap("ltype") = SDKDecoder.decode(paramMap("ltype"))
      }else{
        paramMap("log") = URLDecoder.decode(paramMap("log"), "utf-8")
      }
      val allJson = JSON.parseObject(paramMap("log"))
      val time = allJson.get("itime").toString
      appkey = paramMap.getOrElse("appkey", "-")
      if(appkey != "-"){
        val appkeyList = fieldsMap(appkey)
        val fieldValues = ListBuffer.fill(appkeyList.length)("-")
        var error: String = null
        for(i <- 0 until appkeyList.length){
          try{
            val field = appkeyList(i)
            error = field._1
            field._1 match {
              case "country" => fieldValues(field._2) = paramMap.getOrElse("country", "-")
              case "province" => fieldValues(field._2) = paramMap.getOrElse("province", "-")
              case "city" => fieldValues(field._2) = paramMap.getOrElse("city", "-")
              case "isp" => fieldValues(field._2) = paramMap.getOrElse("isp", "-")
              case "appkey" => fieldValues(field._2) = paramMap.getOrElse("appkey", "-")
              case "ltype" => fieldValues(field._2) = paramMap.getOrElse("ltype", "-")
              case "value" => fieldValues(field._2) = allJson.get("value").toString
              case "dt" => fieldValues(field._2) = time.split(" ")(0)
              case "hour" => fieldValues(field._2) = time.split(" ")(1).split(":")(0)
              case "mins" => fieldValues(field._2) = time.split(" ")(1).split(":")(1)
              case _ => fieldValues(field._2) = get2Json(allJson, field._1)
            }
          }catch {
            case e: Exception => logger.error("fail to find the field:" + error + "origin log is:" + logStr, e)
          }
        }
        println((appkey, fieldValues))
        return (appkey, fieldValues)
      }
    }catch {
      case e: Exception => logger.error("fail to format log" + logStr, e)
    }
    (appkey, new ListBuffer[String])
  }

  /**
    * 存在二级json的问题，一般value中不取出来，如果取出来就会出现空指针，此时去除二级json即可
    * @param allJson log的Json
    * @param field 字段key
    * @return 返回一级或者二级的value
    */
  def get2Json(allJson: JSONObject, field: String): String ={
    var res = "-"
    try{
      res = allJson.get(field).toString
    }catch {
      case e: NullPointerException => res = JSON.parseObject(allJson.get("value").toString).get(field).toString
    }
    return res
  }
}

/**
  * 测试main
  */
object SDKFormator{
  def main(args: Array[String]): Unit = {
    val testLogStr = """223.72.95.187 - - [16/Sep/2018:22:38:17 +0800] "GET /logger.php?appkey=tvtest&ltype=bwgl&enc=0&log=%7B%22uuid%22%3A%2200001123a3-ffff-ffffe7763000%22%2C%22imei%22%3A%22androidId01000000be03ddeb%22%2C%22uid%22%3A%22androidId00000000be03ddeb%22%2C%22userid%22%3A%22-%22%2C%22ctp%22%3A%22android%22%2C%22androidid%22%3A%2200000000be03ddeb%22%2C%22mac%22%3A%2200%3A00%3A00%3A00%3A00%3A00%22%2C%22mtype%22%3A%22BAOFENG_TV+MST_6A358%22%2C%22mos%22%3A%224.4.4%22%2C%22ver%22%3A%223.1.1.778%22%2C%22gid%22%3A%22dev%22%2C%22unet%22%3A%220%22%2C%22itime%22%3A%222018-09-16+13%3A34%3A36%22%2C%22value%22%3A%7B%22sn%22%3A%2260000AM3M00G18561699_95D9%22%2C%22t%22%3A%22fui_usercenter%22%2C%22patch_id%22%3A%22-%22%2C%22r%22%3A%22r153416368761800000208%22%2C%22itime%22%3A%222018-09-16+13%3A34%3A36%22%2C%22userid%22%3A%22%22%2C%22app_id%22%3A%22fui_usercenter%22%2C%22usertype%22%3A%22GUEST%22%2C%22llal%22%3A%220%22%2C%22ip%22%3A%220.0.0.0%22%7D%7D HTTP/1.1" 200 43 "-" "python-requests/2.19.1" "-" "-" "-""""
    val fieldsList: ListBuffer[(String, Int)] = ListBuffer(("country",0), ("value",17), ("mac",9), ("mtype",10), ("gid",11), ("mos",12), ("ver",13), ("unet",14), ("itime",15), ("userid",16), ("mins",18), ("province",1), ("city",2), ("isp",3), ("ltype",4), ("uuid",5), ("uid",6), ("imei",7), ("androidid",8),("dt",20),("hour",19),("usertype",21))
  }
}