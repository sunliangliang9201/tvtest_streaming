import java.io.{BufferedReader, FileInputStream, InputStreamReader}

import org.apache.avro.SchemaBuilder.ArrayBuilder
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object testsc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("e:/ip_area_isp.txt")
    val arr = rdd.filter(x => {
      x.stripMargin != null && x.stripMargin != ""
    }).collect()

    println(arr.toList)
    println(arr.length)
    sc.stop()

//    var br: BufferedReader = null
//    var s: String = null
//    var flag = true
//    var lines = ArrayBuffer[String]()
//      br = new BufferedReader(new InputStreamReader(new FileInputStream("e:/ip_area_isp.txt")))
//      while(flag){
//        s = br.readLine()
//        if(s != null){
//          lines.append(s)
//        }else{
//          flag = false
//        }
//      }
//    println(lines)
  }
}
