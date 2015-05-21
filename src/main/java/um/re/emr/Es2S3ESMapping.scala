package um.re.emr

import java.io.File
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapred.JobConf
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.serializer.KryoRegistrator
import scala.math
import scala.collection.JavaConversions._
import play.api.libs.json._
import play.api.libs.json.{ Json, JsValue, JsObject, JsArray }
import org.elasticsearch.hadoop.mr.EsInputFormat
import org.elasticsearch.spark
import org.elasticsearch.spark.rdd.EsSpark
import org.apache.spark.SparkContext
import org.apache.hadoop.io.NullWritable
import org.elasticsearch.hadoop.mr.EsOutputFormat
import um.re.utils.EsUtils
import um.re.utils.UConf
import um.re.utils._
/**
 * This class is to map old schema to the new schema and at the same time save the data to ES or S3
 */
object Es2EsWithMapping extends App {

  val conf_s = new SparkConf()
  val sc = new SparkContext(conf_s)
  EsUtils.ESINDEX = "sourcehtmls20150517/data"
  val data = new UConf(sc, 10)
  val all = data.getData()

  //TODO MAP ALL FIELDS, AND RETURN MAP BACK
  val source = all.map { l =>
    /*
    val json = Utils.string2Json(Utils.map2JsonString(l._2).replaceAll("price_patterns", "patternsHtml").replaceAll("price_prop_anal", "patternsText").
      replaceAll("raw_text", "domain").replaceAll("last_scraped_time", "lastScrapedTime").replaceAll("last_updated_time", "lastUpdatedTime")
      .replaceAll("price_updated", "updatedPrice").replaceAll("price_prop1", "htmls").replaceAll("prod_id", "prodId"))
    (l._1, Utils.json2Map(json))
  */
    val m = l._2
    val p = "0.0" //if (m.apply("price_updated") != null || !m.apply("price_updated").equals("(null)")) m.apply("price_updated") else "0.0"
    (m.apply("url"), Map("url" -> m.apply("url"), "title" -> m.apply("title"), "patternsHtml" -> m.apply("price_patterns"),
      "patternsText" -> m.apply("price_prop_anal"), "price" -> m.apply("price"), "updatedPrice" -> p,
      "html" -> m.apply("price_prop1"), "shipping" -> m.apply("shipping"), "prodId" -> m.apply("prod_id"), "domain" -> Utils.getDomain(m.apply("url"))))
  }

  val conf1 = new JobConf()
  conf1.set("es.resource", "" + "/" + "data")
  conf1.set("es.nodes", EsUtils.ESIP)

  //SAVE DATA TO ES
  val conf2 = new JobConf()
  conf2.set("es.resource", "sourcehtmls20150517" + "/" + "data")
  conf2.set("es.nodes", EsUtils.ESIP)
  EsUtils.write2ESHadoop(source, conf2)

  //SAVE DATA TO S3
  source.repartition(100).saveAsObjectFile(Utils.S3STORAGE + "/dpavlov/es/sourcehtmls20150517")

  //all.repartition(100).saveAsObjectFile(Utils.S3STORAGE+"/dpavlov/es/source20150513")
}

