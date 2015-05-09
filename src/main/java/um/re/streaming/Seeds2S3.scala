package um.re.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.mllib.stat.Statistics
import um.re.transform.Transformer
import um.re.utils.Utils
import um.re.transform.Transformer
import um.re.utils.{ UConf }
import um.re.utils.Utils
import org.apache.hadoop.io.compress.GzipCodec
object Seeds2S3 extends App {

  val conf_s = new SparkConf()
  val sc = new SparkContext(conf_s)

 
 def relevantDomains(tuplelDataDom:RDD[(String, (String, String, String, String, String, String, String, 
 String, String, String, String, String))],sc:SparkContext):RDD[(String, (String, String, String, String, String, String, String, 
 String, String, String, String, String))] = {  
    val Domlist = sc.textFile("/domains.list").flatMap { l => l.split(",").filter(s => !s.equals(""))}.map(l=>(l,"domain"))
    tuplelDataDom.join(Domlist).map(l=>(l._1,l._2._1))
   }

 
//Load html data with  Title
    val dataHtmls = new UConf(sc, 1)
    //Read from ES
      //val allHtmls = dataHtmls.getData
      //val tuplelDataDom = allHtmls.map (l=>((Utils.getDomain(l._1),(l._2.apply("url"),(l._2.apply("prod_id")),(l._2.apply("title"))))))
      
    //Read from S3 (full_river data from ES that i've saved into S3)
     val tuplelDataDom=dataHtmls.getDataFromS3().map(l=>(Utils.getDomain(l._1),((l._2.apply("url")),(l._2.apply("title")),(l._2.apply("prod_id")),(l._2.apply("price_patterns")),(l._2.apply("price")),
        (l._2.apply("price_prop1")),(l._2.apply("price_prop_anal")),(l._2.apply("shipping")),
            (l._2.apply("raw_text")),(l._2.apply("last_scraped_time")),
                 (l._2.apply("last_updated_time")),(l._2.apply("price_updated"))
  )))

        
    //Join on domains that are relevant (minCandNum==80) and choosing Kth percentile of domains => according to # of urls 
    val FinalChosenDom=relevantDomains(tuplelDataDom,sc)

    //Saving to seeds S3
    //  val dataOutput =FinalChosenDom.map(l=>(l._1,l._2._1,l._2._2))
    val dataOutput=FinalChosenDom.map(l=>(l._1,l._2._1,l._2._2,l._2._3,l._2._4,l._2._5,l._2._6,l._2._7,l._2._8,
        l._2._9,l._2._10,l._2._11,l._2._12))
    
    //Cant save file larger than 150Mg so One way is to zip files like that:
    dataOutput.repartition(20).saveAsTextFile(Utils.S3STORAGE+Utils.SEEDS2S3, classOf[GzipCodec])
    //can also add some more partitions and save files
    // dataOutput.repartition(200).saveAsObjectFile(Utils.S3STORAGE+"/dpavlov/seeds")
}