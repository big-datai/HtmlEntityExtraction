import org.apache.spark.{SparkConf, SparkContext}
import um.re.utils.{EsUtils, Utils}

object FullR2S3 extends App {
  val conf_s = new SparkConf()
  val sc = new SparkContext(conf_s)
  //Change in sparkmain in EsUtils the val ESIP => to the right ip adress of server  machine
  //Read demo Seeds From S3  

  val path = Utils.S3STORAGE + "/dpavlov/ESlight20150516"
  val seeds = sc.objectFile[(String)](path, 200)
  //manipulation in order to get desired format to write to ES ie RDD[Map[String,String]]
  val seedsjson = seeds.map { line =>
    Utils.string2Json(line)
  }
  val seedsmap = seedsjson.map { line => Utils.json2Map(line) }
  EsUtils.write2ES(seedsmap, "demo")
}