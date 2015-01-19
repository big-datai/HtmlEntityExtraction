package um.re.es.emr

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.serializer.KryoSerializer

object FilterProper {
  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  import org.apache.hadoop.conf.Configuration
  import org.apache.hadoop.io.MapWritable
  import org.elasticsearch.hadoop.mr.EsInputFormat
  import org.apache.hadoop.io.Text
  import org.apache.spark.serializer.KryoSerializer
  import java.io.File
  import java.io.FileWriter
  import java.io.BufferedWriter
  import collection.JavaConversions._
  import play.api.libs.json.Json
  import play.api.libs.json.Writes
  import play.api.libs.json._
  import org.elasticsearch.spark.rdd.EsSpark
  import um.re.es.emr.NumberFinder2
  import org.elasticsearch.spark
  val conf_s = new SparkConf().setAppName("es").setMaster("local[8]").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)

  var docs3 = sc.textFile("s3://pavlovout/3/part-00000")

  //transform comma new line etc...
  val reOrdered = docs3.map { l => l.drop(1).dropRight(1).toString + "," + System.lineSeparator() }.map { l => l.take(l.length() - 5) + l.takeRight(5).trim().replaceAll(",+?", ",") }

  val filtered = reOrdered.filter { l =>
    try {
      if (l == null)
        false
      val line = l
      val ind = line.indexOf("\"text\":\"")
      val ind_end = line.indexOf("\",\"location\"")
      if (ind == -1 || ind_end == -1)
        false
      val words = line.substring(ind, ind_end).split(" +").length
      if (words < 2)
        false
      else
        true
    } catch {
      case _: Throwable => false
    }
  }
  filtered.coalesce(1, true).saveAsTextFile("s3://pavlovout/3")
}