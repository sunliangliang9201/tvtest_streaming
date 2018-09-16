import scala.util.parsing.json.JSON

object testThread {
  def main(args: Array[String]): Unit = {
    val allJson = JSON.parseFull("""{"uuid":"00001123a3-ffff-ffffe7763000","imei":"androidId01000000be03ddeb","uid":"androidId00000000be03ddeb","userid":"-","ctp":"android","androidid":"00000000be03ddeb","mac":"00:00:00:00:00:00","mtype":"BAOFENG_TV MST_6A358","mos":"4.4.4","ver":"3.1.1.778","gid":"dev","unet":"0","itime":2018-09-16 13:34:36,"value":"a"}""")
    println(allJson)
  }
}
