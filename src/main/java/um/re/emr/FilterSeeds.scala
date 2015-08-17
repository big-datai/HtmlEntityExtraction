package um.re.emr
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import um.re.utils.Utils
import um.re.utils.{ UConf }
import um.re.utils.Utils
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.hadoop.io.compress.GzipCodec
import um.re.emr
import java.util.Calendar
object FilterSeeds {
  def relevantDomains(tuplelDataDom: RDD[(String, String)], sc: SparkContext): RDD[(String)] = {
    val Domlist = sc.textFile("s3n://AKIAJQUAOI7EBC6Y7ESQ:JhremVoqNuEYG8YS9J+duW0hFRtX+sWjuZ0vdQlE@dpavlov/domains.list").flatMap { l => l.split(",").filter(s => !s.equals("")) }.map(l => (l, "domain"))
    Domlist.take(1)
    //val domainsB = sc.broadcast(Domlist)
    Domlist.join(tuplelDataDom).map { l => l._2._2 }
    // tuplelDataDom.join(Domlist).map(l => (l._1, l._2._1))
  }
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val d=Calendar.getInstance().getTime()
    d.toString.replace(" ","").replace(":","")
    // filter data
    val dataAll = sc.textFile("s3n://AKIAJQUAOI7EBC6Y7ESQ:JhremVoqNuEYG8YS9J+duW0hFRtX+sWjuZ0vdQlE@dpavlov/seeds170820151439825456871").map{l=>(l,Utils.json2Map(Utils.string2Json(l)))} //.cache
    val tuplelDataDom = dataAll.map(l => ((l._2.apply("domain"), Utils.map2JsonString(l._2))))

    val dataOutput = relevantDomains(tuplelDataDom, sc)

    dataOutput.coalesce(20, false).saveAsTextFile("s3n://AKIAJQUAOI7EBC6Y7ESQ:JhremVoqNuEYG8YS9J+duW0hFRtX+sWjuZ0vdQlE@dpavlov/seedsFiltered" + d.toString.replace(" ","").replace(":",""), classOf[GzipCodec])

  }
}