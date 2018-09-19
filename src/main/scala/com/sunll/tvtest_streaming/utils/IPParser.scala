package com.sunll.tvtest_streaming.utils

import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.GetMethod
import org.slf4j.LoggerFactory


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

//  val conf: Configuration = new Configuration
//
//  var fs: FileSystem = null
//
//  var hdfsInStream: FSDataInputStream = null
//
//  val path: String = "hdfs://103.26.158.33:9000/test/sunliangliang/ip_area_isp.txt"
  /**
    * 根据已有的ip_area_isp.txt来匹配ip
    * @param ip ip
    * @param ipAreaIspCache 缓存的地理位置信息
    * @return 返回元祖结果
    */
  def parse(ip: String, ipAreaIspCache: Array[String]): (String, String, String, String) = {
    if(binarySearch(ipAreaIspCache, ip2Long(ip)) != -1){
      val res = ipAreaIspCache(binarySearch(ipAreaIspCache, ip2Long(ip)))
      val res2 = res.split("\t").take(4)
      return (res2(0), res2(1), res2(2), res2(3))
    }else{
      return ("-", "-", "-", "-")
    }
  }

  /**
    * ip to long
    * @param ip ip
    * @return long值
    */
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("\\.")
    var ipNum = 0L
    for(i <- 0 until fragments.length){
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  /**
    * 二分法查找ip对应的long值在缓存文件中（ArratBuffer）中的索引
    * @param lines 缓存的array
    * @param ip ip
    * @return 索引值
    */
  def binarySearch(lines: Array[String], ip: Long): Int ={
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
      case e: Exception => logger.error("file ip_area_isp.txt include error format data" + e)
    }
    return -1
  }

  /**
    * 把ip_area_isp.txt缓存到ArrayBuffer中
    * @return array
    */
//  def readIPAreaIsp(): ArrayBuffer[String] = {
//    fs = FileSystem.get(URI.create(path), conf)
//    hdfsInStream = fs.open(new Path(path))
//    hdfsInStream.read
//    var br: BufferedReader = null
//    var s: String = null
//    var flag = true
//    var lines = ArrayBuffer[String]()
//    try{
//      br = new BufferedReader(new InputStreamReader(new FileInputStream(path)))
//      while(flag){
//        s = br.readLine()
//        if(s != null){
//          lines.append(s)
//        }else{
//          flag = false
//        }
//      }
//    }catch{
//      case e: Exception => logger.error("fail to load file " + path + e)
//    }finally {
//      if(br != null){
//        br.close()
//      }
//    }
//    lines
//  }

  /**
    * 测试main
    * @param args
    */
  def main(args: Array[String]): Unit = {
//    readIPAreaIsp("src/main/resources/ip_area_isp.txt")
   // val res = binarySearch(readIPAreaIsp("src/main/resources/ip_area_isp.txt"), 3546428673L)
  }
}
