package com.sunll.tvtest_streaming.formator
import java.net.URLDecoder

import com.sunll.tvtest_streaming.utils.{IPParser, SDKDecoder}
import org.json4s.DefaultFormats
import org.json4s.jackson.Json
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON


/**
  * format the log string on all fields, then match we need
  * @author sunliangliang 2018-09-16
  * @version 1.0
  *
  */
class SDKFormator extends LogFormator {
  val logger = LoggerFactory.getLogger(this.getClass)
  val logRegex = """(\d+\.\d+\.\d+\.\d+).*?logger.php\?(.*?) HTTP.*""".r
  override def format(logStr: String, fields: ListBuffer[(String, Int)]): ListBuffer[String] = {
    val fieldValues = ListBuffer.fill(fields.length)("-")
    var paramMap: mutable.Map[String, String] = mutable.Map[String, String]()
    try{
      val logRegex(ip, query) = logStr
      val fieldsLogList = query.split("&").toList
      fieldsLogList.map(x => paramMap += x.split("=")(0) -> x.split("=")(1))
      val tup = IPParser.parse(ip)
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
      val allJson = JSON.parseFull(paramMap("log"))
      for(i <- 0 until fields.length){
        val field = fields(i)
        field._1 match {
          case "country" => fieldValues(field._2) = paramMap.getOrElse("country", "-")
          case "province" => fieldValues(field._2) = paramMap.getOrElse("province", "-")
          case "city" => fieldValues(field._2) = paramMap.getOrElse("city", "-")
          case "appkey" => fieldValues(field._2) = paramMap.getOrElse("appkey", "-")
          case "ltype" => fieldValues(field._2) = paramMap.getOrElse("ltype", "-")
          case "value" => fieldValues(field._2) = getJSONValue(allJson, "value")
          case "dt" => fieldValues(field._2) = getValue(allJson, "itime").split(" ")(0)
          case "hour" => fieldValues(field._2) = getValue(allJson, "itime").split(" ")(1).split(":")(0)
          case "mins" => fieldValues(field._2) = getValue(allJson, "itime").split(" ")(1).split(":")(1)
          case _ => fieldValues(field._2) = getValue(allJson, field._1)
        }
      }
    }catch {
      case e: Exception => logger.error("fail to format log" + logStr, e)
    }
    val realLog = URLDecoder.decode(logStr, "utf-8")
    fieldValues
  }

  def getValue(m: Option[Any], key:String)= {
    try {
      m match {
        case Some(m: scala.collection.immutable.Map[String, Any]) => m(key).toString;
        case _ => "-"
      }
    } catch {
      case e: Exception => "-"
    }
  }

  def getJSONValue(m: Option[Any], key:String)= {
    try {
      m match {
        case Some(m: scala.collection.immutable.Map[String, Any]) => val a= m(key).asInstanceOf[scala.collection.immutable.Map[String, Any]];Json(DefaultFormats).write(a);
        case _ => "-"
      }

    } catch {
      case e: Exception => "-"
    }
  }
}

object SDKFormator{
  def main(args: Array[String]): Unit = {
    val testLogStr = """223.72.95.187 - - [16/Sep/2018:22:38:17 +0800] "GET /logger.php?appkey=tvtest&ltype=bwgl&enc=0&log=%7B%22uuid%22%3A%2200001123a3-ffff-ffffe7763000%22%2C%22imei%22%3A%22androidId01000000be03ddeb%22%2C%22uid%22%3A%22androidId00000000be03ddeb%22%2C%22userid%22%3A%22-%22%2C%22ctp%22%3A%22android%22%2C%22androidid%22%3A%2200000000be03ddeb%22%2C%22mac%22%3A%2200%3A00%3A00%3A00%3A00%3A00%22%2C%22mtype%22%3A%22BAOFENG_TV+MST_6A358%22%2C%22mos%22%3A%224.4.4%22%2C%22ver%22%3A%223.1.1.778%22%2C%22gid%22%3A%22dev%22%2C%22unet%22%3A%220%22%2C%22itime%22%3A%222018-09-16+13%3A34%3A36%22%2C%22value%22%3A%7B%22sn%22%3A%2260000AM3M00G18561699_95D9%22%2C%22t%22%3A%22fui_usercenter%22%2C%22patch_id%22%3A%22-%22%2C%22r%22%3A%22r153416368761800000208%22%2C%22itime%22%3A%222018-09-16+13%3A34%3A36%22%2C%22userid%22%3A%22%22%2C%22app_id%22%3A%22fui_usercenter%22%2C%22usertype%22%3A%22GUEST%22%2C%22llal%22%3A%220%22%2C%22ip%22%3A%220.0.0.0%22%7D%7D HTTP/1.1" 200 43 "-" "python-requests/2.19.1" "-" "-" "-""""
    val fieldsList: ListBuffer[(String, Int)] = ListBuffer(("country",0), ("value",17), ("mac",9), ("mtype",10), ("gid",11), ("mos",12), ("ver",13), ("unet",14), ("itime",15), ("userid",16), ("mins",18), ("province",1), ("city",2), ("isp",3), ("ltype",4), ("uuid",5), ("uid",6), ("imei",7), ("androidid",8),("dt",20),("hour",19))
    val res = new SDKFormator().format(testLogStr, fieldsList)
    println(res)
  }
}