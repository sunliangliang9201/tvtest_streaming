package com.sunll.tvtest_streaming.utils

import java.net.URLDecoder

/**
  * if env in the log is not 0, then we need to decode.
  * @author sunliangliang 2018-09-16
  * @version 1.0
  *
  */
object SDKDecoder {
  /**
    * 如果㤇解析的话，解密log或者ltype
    * @param logStr 原始日志
    * @return 解密后日志
    */
  def decode(logStr: String): String = {
    val decryptStr = " !_#$%&'()*+,-.ABCDEFGHIJKLMNOP?@/0123456789:;<=>QRSTUVWXYZ[\\]^\"`nopqrstuvwxyzabcdefghijklm{|}~"
    var resLogStr = ""
    val realLog = URLDecoder.decode(logStr, "utf-8")
    for(i <- 0 to realLog.length - 1){
      var ch = realLog.charAt(i)
      if(ch.toInt >= 32 && ch.toInt <= 126){
        resLogStr +=  decryptStr.charAt(ch.toInt - 32)
      }else{
        resLogStr += ch
      }
    }
    resLogStr
  }

  /**
    * 测试mian
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val test =
      """%7B_hhvq_L_ssssssss-oppo-FHos-ssss-ssssrIIHEBBB_%2C_vzrv_L_JHIJDEBDGCBBGBC_%2C_hvq_L_JHIJDEBDGCBBGBC_%2C_hfrevq_L_-_%2C_naqebvqvq_L_pHnFJpnCnJEsrK_%2C_znp_L_FBLFGLqnLsBLFFLHJ_%2C_zglcr_L_V%3DT%3D%205TI_%2C_zbf_L_G.C_%2C_ire_L_D.B.D_%2C_tvq_L_qDB_%2C_harg_L_C_%2C_vgvzr_L_DBCH-CB-EC%20CILBELBH_%2C_inyhr_L%7B_tebhcvq_L__%2C_pbagragvq_L_KCJK_%2C_fgnghf_L_D_%2C_zbqr_L_C_%2C_fvgr_L_osbayvar_%2C_erfglcr_L_beqvanelivqrb_%2C_sebz_L_C_%2C_cynlgvzr_L_CK_%2C_cntr_L_CH_%2C_punaary_L_ubzrcntr_%2C_glcr_L_ivqrb_%2C_hfre%22vq_L_-_%7D%7D"""

    val res = decode(test)
    println(res)
  }
}
