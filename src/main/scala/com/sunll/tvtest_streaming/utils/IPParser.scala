package com.sunll.tvtest_streaming.utils

import java.io.{BufferedReader, FileInputStream, InputStreamReader}

import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.slf4j.LoggerFactory
import scala.collection.mutable.ArrayBuffer

/**
  * parse ip to country, province, city, isp
  *
  * @author sunliangliang 2018-09-17
  * @version 1.0
  *
  */

object IPParser {
  val logger =LoggerFactory.getLogger(this.getClass)
  var client: HttpClient = new HttpClient
  var method: GetMethod = null
  def parse2(ip: String): (String, String, String, String) = {
    val taobaoURL = "http://ip.taobao.com/service/getIpInfo.php?ip=" + ip
    try{
      method = new GetMethod(taobaoURL)
      client.executeMethod(method)
      println(method.getResponseBody)
    }catch{
      case e: Exception => logger.error("fail to parse ip" + ip)
    }finally {
      if(null != method){
        method.releaseConnection()
      }
    }
    ("a", "b", "c", "d")
  }

  def parse(ip: String, ipAreaIspCache: ArrayBuffer[String]): (String, String, String, String) = {
    if(binarySearch(ipAreaIspCache, ip2Long(ip)) != -1){
      val res = ipAreaIspCache(binarySearch(ipAreaIspCache, ip2Long(ip)))
      val res2 = res.split("\t").take(4)
      return (res2(0), res2(1), res2(2), res2(3))
    }else{
      return ("-", "-", "-", "-")
    }
  }

  def ip2Long(ip: String): Long = {
    val fragments = ip.split("\\.")
    var ipNum = 0L
    for(i <- 0 until fragments.length){
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def binarySearch(lines: ArrayBuffer[String], ip: Long): Int ={
    // 中国	福建省	福州市	铁通	3546428672	3546428679
    var low = 0
    var high = lines.length - 1
    try{
      while (low <= high) {
        val middle = (low + high) / 2
        if ((ip >= lines(middle).split("\t")(4).toLong) && (ip <= lines(middle).split("\t")(5).toLong))
          return middle
        if (ip < lines(middle).split("\t")(4).toLong)
          high = middle - 1
        else {
          low = middle + 1
        }
      }
    }catch {
      case e: Exception => logger.error("file ip_area_isp.txt include error format data")
    }
    return -1
  }

  def readIPAreaIsp(path: String): ArrayBuffer[String] = {
    var br: BufferedReader = null
    var s: String = null
    var flag = true
    var lines = ArrayBuffer[String]()
    try{
      br = new BufferedReader(new InputStreamReader(new FileInputStream(path)))
      while(flag){
        s = br.readLine()
        if(s != null){
          lines.append(s)
        }else{
          flag = false
        }
      }
    }catch{
      case e: Exception => logger.error("fail to load file " + path)
    }finally {
      if(br != null){
        br.close()
      }
    }
    lines
  }

  def main(args: Array[String]): Unit = {
//    readIPAreaIsp("src/main/resources/ip_area_isp.txt")
   // val res = binarySearch(readIPAreaIsp("src/main/resources/ip_area_isp.txt"), 3546428673L)
    val res = parse("103.26.158.33",readIPAreaIsp("src/main/resources/ip_area_isp.txt"))
    println(res)
  }
}
