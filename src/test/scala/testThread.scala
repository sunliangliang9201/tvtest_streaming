
import com.alibaba.fastjson.JSON

import scala.collection.mutable.ListBuffer
import util.control.Breaks._
object testThread {
  def main(args: Array[String]): Unit = {
    //    val allJson = JSON.parseFull("""{"uuid":"00001123a3-ffff-ffffe7763000","imei":"androidId01000000be03ddeb","uid":"androidId00000000be03ddeb","userid":"-","ctp":"android","androidid":"00000000be03ddeb","mac":"00:00:00:00:00:00","mtype":"BAOFENG_TV MST_6A358","mos":"4.4.4","ver":"3.1.1.778","gid":"dev","unet":"0","itime":2018-09-16 13:34:36,"value":"a"}""")
    //    println(allJson)

    val a = Array("a", "b", "c", "d", "e", "f")
    val b = Array("e", "d", "c", "b", "a")
    for (i <- 0 until a.length) {
      breakable{
        for (j <- 0 until b.length) {
          if(a(i).equals(b(j))){
            break()
          }
        }
        println(a(i))
      }
    }
//    val fields = ListBuffer[String]("a", "b", "cb")
//    val json = """{"a":"1","b":"2","c":{"ca":"31","cb":"32"}}"""
//    val fulljson = JSON.parseObject(json)
//    var res = ListBuffer.fill(fields.length)("-")
//    for(i <- 0 until fields.length) {
//      fields(i) match {
//        case "a" => res(i) = fulljson.get("a").toString
//        case "b" => res(i) = fulljson.get("b").toString
//        case "c" => res(i) = fulljson.get("c").toString
//        case _ => res(i) = JSON.parseObject(fulljson.get("c").toString).get(fields(i)).toString
//      }
//    }
//    println(res)
  }

}