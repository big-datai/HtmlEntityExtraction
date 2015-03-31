package um.re.models
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.serializer.KryoSerializer
import um.re.utils.EsUtils
import um.re.transform.Transformer
import um.re.utils.UConf
import org.elasticsearch.spark.rdd.EsSpark
import org.apache.spark.serializer.KryoSerializer

object TestSubmit extends App {

  println("Hello from submit")
  val conf = new SparkConf().setAppName("es").set("master", "yarn-cluster").set("spark.serializer", classOf[KryoSerializer].getName)
  val sc = new SparkContext(conf)
  val data = new UConf(sc, 1000)
  val all = data.getData
  val parsedData = Transformer.parseData(all)

  println("+++++++++++++++++++++++++++                    ++++++++++++++++++++++++++++++++++   " + parsedData.count)

  val cfg = Map("es.nodes" -> EsUtils.ESIP, "es.resource" -> "aaa/data", "es.index.auto.create" -> "true", "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer")
  val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
  val airports = Map("arrival" -> "Otopeni", "SFO" -> "san fransco")
  EsSpark.saveToEs(sc.makeRDD(Seq(numbers, airports)), cfg)
}