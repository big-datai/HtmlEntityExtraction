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
import um.re.analysis.UConfAnal
import um.re.analysis.UConfAnal2
import um.re.transform.Transformer
import um.re.utils.{ UConf }
import um.re.utils.Utils
import um.re.analysis.EsExporter2

object Seeds2S3 extends App {

   val conf_s = new SparkConf()
  val sc = new SparkContext(conf_s)

  
  
  def domainList(allData:RDD[(String, Map[String, String])],minCandNum:Int): RDD[(String)]={
    val domain = allData.map {l => Utils.getDomain(l._2.apply("url"))}
    val words = domain.flatMap(x => x.split(","))
    val countDomain = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)
    countDomain.filter(d => d._2>=minCandNum).map(l=>l._1)
    }
  
 
 def relevantDomains(tuplelDataDom:RDD[(String, (String, String,String))],minCandNum:Int,sc:SparkContext):RDD[(String, (String, String,String))] = {  
    val dataCandid = new UConf(sc, 200)
    //Retrieve data from ES
      //val allCandid = dataCandid.getData
    //Retrieve data from S3
      val allCandid = dataCandid.getDataFS()
    val domainRelevantList= domainList(allCandid,minCandNum)
    val reldom =domainRelevantList.map(l=>(l,"domain"))
    tuplelDataDom.join(reldom).map(l=>(l._1,l._2._1))
   }

 def printDom2File(output:RDD[(String, String, String)],sc: SparkContext)={  
    //val output2Print=output.map{t=> 
     // (t._1+"\u0001"+t._2+"\u0001"+t._3).mkString("")}//.collect().mkString("\n")
    //sc.parallelize(List(output2Print), 1).saveAsTextFile("hdfs:///analysis/resrevised/")
    
    output.saveAsObjectFile(Utils.S3STORAGE+"/rawd/objects/full") 
    //output2Print.coalesce(1, false).saveAsTextFile("hdfs:///analysis/resultss/")
}     
 
 
//Load html data with  Title
    val dataHtmls = new UConfAnal(sc, 200)
    //Read from ES
      //val allHtmls = dataHtmls.getData
      //val tuplelDataDom = allHtmls.map (l=>((Utils.getDomain(l._1),(l._2.apply("url"),(l._2.apply("prod_id")),(l._2.apply("title"))))))
      
    //Read from S3 (full_river data from ES that i've saved into S3)
    val tuplelDataDom = dataHtmls.getDataFS()
    
//Join on domains that are relevant (minCandNum==80) and choosing Kth percentile of domains => according to # of urls 
    val FinalChosenDom=relevantDomains(tuplelDataDom,80,sc)

    //Saving to seeds S3
      val dataOutput =FinalChosenDom.map(l=>(l._1,l._2._1,l._2._2))
   // val output=beforeOutput.join(prods)
    printDom2File(dataOutput,sc)
   
   // val numProd=output.map(l=>(l._2)).distinct().count
//Save2s3      
   //tuplelDataDom.saveAsObjectFile(Utils.S3STORAGE+"/rawd/objects/full") 
//ReadFromS3  
  //val tuplelDataDomS3 = sc.objectFile[(String, (String, String, String))](Utils.S3STORAGE+"/rawd/objects/full", 200)

//ReadFromHDFS
  //val tuplelDataDom = sc.objectFile[(String, (String, String, String))]("hdfs:///analysis/data/", 200)
}