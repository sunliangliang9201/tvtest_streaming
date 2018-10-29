import org.apache.spark.{SparkConf, SparkContext}

/**
  * desc
  *
  * @author sunliangliang 2018-09-28 https://github.com/sunliangliang9201/tvtest_streaming
  * @version 1.0
  */
object RushPart {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]").setAppName("rushpart")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("e:/")

    sc.stop()
  }
}
