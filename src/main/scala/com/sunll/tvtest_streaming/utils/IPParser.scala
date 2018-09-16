package com.sunll.tvtest_streaming.utils

import scala.io.Source

object IPParser {
  val ipMap: Map[]
  def parse(ip: String): (String, String, String, String) = {
    ("a", "b", "c", "d")
  }

  def main(args: Array[String]): Unit = {
    val ip_area_isp = Source.fromFile("ip_area_isp.txt")
    for(line <- ip_area_isp.getLines()){

    }
  }
}
