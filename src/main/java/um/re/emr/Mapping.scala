package um.re.emr

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import um.re.utils.{UConf, Utils}

/**
  * @author dmitry
  *         this object is to re-map ids to a new standart DPID
  */

object Mapping {

  def relevantDomains(tuplelDataDom: RDD[(String, String)], sc: SparkContext): RDD[(String)] = {
    val Domlist = sc.textFile("file:///root/domains.list").flatMap { l => l.split(",").filter(s => !s.equals("")) }.map(l => (l, "domain"))
    Domlist.take(1)
    //val domainsB = sc.broadcast(Domlist)
    Domlist.join(tuplelDataDom).map { l => l._2._2 }
    // tuplelDataDom.join(Domlist).map(l => (l._1, l._2._1))
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName(getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val data = new UConf(sc, 1).getDataFromS3("s3n://AKIAJQUAOI7EBC6Y7ESQ:JhremVoqNuEYG8YS9J+duW0hFRtX+sWjuZ0vdQlE@dpavlov/es/source20150516") //.cache

    val byProdId = data.map(l => ((l._2.apply("prodId"), Utils.map2JsonString(l._2))))
    val byTitle = data.map(l => ((l._2.apply("title"), Utils.map2JsonString(l._2))))

    val uniqProdIds = byProdId.groupByKey() //.cache
    //uniqProdIds.count
    // byTitle.groupByKey().count

    var counter = 1000000000
    val map = uniqProdIds.map(l => l._1).collect().sorted.map { k =>
      counter = counter + 1
      (k, counter)
    }.toMap

    //sc.parallelize(map.toSeq, 1).saveAsTextFile("s3n://AKIAJQUAOI7EBC6Y7ESQ:JhremVoqNuEYG8YS9J+duW0hFRtX+sWjuZ0vdQlE@dpavlov/mapping.txt")
    val mapB = sc.broadcast(map)

    import java.io._
    printToFile(new File("mapping.txt")) { p =>
      map.foreach(p.println)
    }

    data.partitions.size
    /*
    val data2 = data.map { l =>

      val m = collection.mutable.Map() ++ l._2
      val id = m.apply("prodId")
      l._2-"prodId"
      val old = m.put("prodId", mapB.value.apply(id).toString())
      (Utils.map2JsonString(m.toMap))
    }.saveAsTextFile("s3n://AKIAJQUAOI7EBC6Y7ESQ:JhremVoqNuEYG8YS9J+duW0hFRtX+sWjuZ0vdQlE@dpavlov/seeds10082015",classOf[GzipCodec])
  */
    val data2 = data.map { l =>
      val id = l._2.apply("prodId")
      val m = l._2 - "prodId"
      val mm = m + ("prodId" -> mapB.value.apply(id).toString())
      (Utils.map2JsonString(mm.toMap))
    }.coalesce(200, false)

    data2.saveAsTextFile("s3n://AKIAJQUAOI7EBC6Y7ESQ:JhremVoqNuEYG8YS9J+duW0hFRtX+sWjuZ0vdQlE@dpavlov/seeds17082015" + System.currentTimeMillis(), classOf[GzipCodec])
  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }
}





