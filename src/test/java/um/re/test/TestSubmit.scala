package um.re.test
import org.apache.hadoop.mapred.JobConf
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.serializer.KryoSerializer
import um.re.utils.URegistrator
import scala.collection.JavaConversions._
import play.api.libs.json._
import org.elasticsearch.spark.rdd.EsSpark
import org.apache.spark.SparkContext
import org.apache.spark.serializer.KryoSerializer
import um.re.utils.EsUtils
object TestSubmit extends App {

  println("Hello from submit")

  val reg = new URegistrator

  //val conf = new Configuration()
  val conf = new JobConf()
  conf.set("es.resource", "htmls/data")
  conf.set("es.query", "?q=prod_id:23799864")
  conf.set("es.nodes", EsUtils.ESIP)

  val conf_s = new SparkConf().setAppName("es").set("master", "yarn-client").set("spark.serializer", classOf[KryoSerializer].getName)
  //val sc = new SparkContext(conf_s)

  //Run a query
  //val conf_s = new SparkConf().setAppName("es").setMaster("local[8]").set("spark.serializer", classOf[KryoSerializer].getName)
  conf_s.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  val sc = new SparkContext(conf_s)

  //val source = sc.newAPIHadoopRDD(conf, classOf[EsInputFormat[Text, MapWritable]], classOf[Text], classOf[MapWritable])

  //  val source2 = source.map { l => (l._1.toString(), l._2.map { case (k, v) => (k.toString, v.toString) }.toMap) }.repartition(100)
  //println(source2.partitions.size)

  //val source3 = source.map { l => (l._1.toString(), l._2.toString) }.repartition(100)

  val cfg = Map("es.nodes" -> EsUtils.ESIP, "es.resource" -> "aaa/data", "es.index.auto.create" -> "true", "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer")
  val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
  val airports = Map("arrival" -> "Otopeni", "SFO" -> "san fransco")
  EsSpark.saveToEs(sc.makeRDD(Seq(numbers, airports)), cfg)
}