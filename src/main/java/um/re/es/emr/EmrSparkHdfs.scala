package um.re.es.emr

object EmrSparkHdfs {
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
  import um.re.es.emr.NumberFinder
  import org.elasticsearch.spark

  val conf_s = new SparkConf().setAppName("es").set("master", "yarn-cluster").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf_s)

  val source = sc.textFile("hdfs:///spark-logs/raw1/part-00000")

  val docs = sc.makeRDD(source.take(10)).map { hit =>
    {
      val nf = NumberFinder
      val dc = hit
      var html_start: Int = dc.indexOf("<html>")
      var html_end: Int = dc.indexOf("</html>")
      if (html_start == -1 || html_end == -1) {
        html_start = dc.indexOf("price_prop1=")
        html_end = dc.indexOf("price_prop_anal=")
      }
      val html = dc.substring(html_start, html_end)
      //val price_id = dc.indexOf("price_updated=")
      //val price_id_end = dc.length() - 2
      //val price = dc.substring(price_id, price_id_end)
      // val res = nf.find("", html)
      // res.toString
      html
    }
  }

}