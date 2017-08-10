package um.re.streaming

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import um.re.utils.{UConf, Utils}

object SeedsFromS3Htmls2S3 extends App {

  // val conf = new SparkConf().setMaster("local[*]").setAppName("Test")
  // val sc = new SparkContext(conf)
  val conf_s = new SparkConf()
  val sc = new SparkContext(conf_s)
  val dataHtmls = new UConf(sc, 1)

  //val data= sc.objectFile[(String, Map[String, String])]("/home/eran/sampleHtmls", 1)
  val data = dataHtmls.getDataFromS3()
  val source = data.map { l =>
    val m = l._2
    val p = "0.0" //if (m.apply("price_updated") != null || !m.apply("price_updated").equals("(null)")) m.apply("price_updated") else "0.0"
    (m.apply("url"), Map("url" -> m.apply("url"), "title" -> m.apply("title"), "patternsHtml" -> m.apply("price_patterns"),
      "patternsText" -> m.apply("price_prop_anal"), "price" -> m.apply("price"), "updatedPrice" -> p,
      "html" -> m.apply("price_prop1"), "shipping" -> m.apply("shipping"), "prodId" -> m.apply("prod_id"), "domain" -> Utils.getDomain(m.apply("url"))))
  }
  val tuplelDataDom = source.map(l => ((l._2.apply("domain"), Utils.map2JsonString(l._2))))
  val dataOutput = relevantDomains(tuplelDataDom, sc)

  //Join on domains that are relevant (minCandNum==80) and choosing Kth percentile of domains => according to # of urls

  def relevantDomains(tuplelDataDom: RDD[(String, String)], sc: SparkContext): RDD[(String)] = {
    val Domlist = sc.textFile("/home/eran/domains.list").flatMap { l => l.split(",").filter(s => !s.equals("")) }.map(l => (l, "domain"))
    Domlist.join(tuplelDataDom).map { l => l._2._2 }
  }
  dataOutput.repartition(20).saveAsObjectFile(Utils.S3STORAGE + "/dpavlov/seeds20150603")
}
