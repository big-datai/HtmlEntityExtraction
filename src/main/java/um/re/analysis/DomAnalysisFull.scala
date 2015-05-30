package um.re.analysis

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


object DomAnalysisFull extends App {
  val conf_s = new SparkConf()
  val sc = new SparkContext(conf_s)

  
  
  def domainList(allData:RDD[(String, Map[String, String])],minCandNum:Int): RDD[(String)]={
    val domain = allData.map {l => Utils.getDomain(l._2.apply("url"))}
    val words = domain.flatMap(x => x.split(","))
    val countDomain = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)
    countDomain.filter(d => d._2>=minCandNum).map(l=>l._1)
    }
  
 def topKPercentDom(tuplelDataDom:RDD[(String, (String, String,String))],minCandNum:Int,Precentage:Long,sc:SparkContext):RDD[(String, (String, String,String))] = {  
    val dataCandid = new UConf(sc, 200)
    //Retrieve data from ES
      //val allCandid = dataCandid.getData
    //Retrieve data from S3
    val allCandid = dataCandid.getDataFS()
    val domainRelevantList= domainList(allCandid,minCandNum)
    val reldom =domainRelevantList.map(l=>(l,"domain2"))
    val partList1 = tuplelDataDom.join(reldom).map(l=>(l._1,l._2._1))
    val cntByK=partList1.countByKey().toList.map(l=>(l._2,l._1))
    val newCntByK=sc.parallelize(cntByK, 1).sortByKey(false)
    val numOfChosenDom=(newCntByK.count * Precentage/100).toInt
    val chosenDom=newCntByK.take(numOfChosenDom)
    val chosenDomRDD=sc.parallelize(chosenDom, 1).map(l=>(l._2,l._1))
    chosenDomRDD.join(partList1).map(l=>(l._1,l._2._2))
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
   // val cntByK=partList1.countByKey().toList.map(l=>(l._2,l._1))
  //  val newCntByK=sc.parallelize(cntByK, 1).sortByKey(false)   
  //  val chosenDom=newCntByK.take(numOfChosenDom)
  //  val chosenDomRDD=sc.parallelize(chosenDom, 1).map(l=>(l._2,l._1))
  //  chosenDomRDD.join(partList1).map(l=>(l._1,l._2._2))
  }

 def printDom2File(output:RDD[(String, String, String,Long)],sc: SparkContext)={  
    val output2Print=output.map{t=> 
      (t._1+"\u0001"+t._2+"\u0001"+t._3+"\u0001"+t._4).mkString("")}
   output2Print.coalesce(1, false).saveAsTextFile("hdfs:///analysis/resfull/")
}     
 
 
//Load html data with  Title
    val dataHtmls = new UConf(sc, 200)
    //Read from ES
      //val allHtmls = dataHtmls.getData
      //val tuplelDataDom = allHtmls.map (l=>((Utils.getDomain(l._1),(l._2.apply("url"),(l._2.apply("prodId")),(l._2.apply("title"))))))
      
    //Read from S3
    val tuplelDataDom = dataHtmls.getAnalDataFS()
    
//Join on domains that are relevant (minCandNum==80) and choosing Kth percentile of domains => according to # of urls 
    val FinalChosenDom=tuplelDataDom
//Choose distinct (domain, prodId)
    val DistinctDomProd= FinalChosenDom.map(l=>(l._1,l._2._2)).distinct()
//Choose prodId which occur in more then 25 distinct domains => (prodId, # of occurrences in distinct domains )
    val CntDom= DistinctDomProd.map(l=>(l._2,1)).reduceByKey((x, y) => x + y).filter(f=>(f._2>30)).map(l=>(l._2,l._1)).sortByKey(false).map(l=>(l._2,l._1))
//Joining  # of occurrences of prodId in distinct domains 
    val FinalChosenProds=FinalChosenDom.map(l=>(l._2._2,(l._2._1,l._1,l._2._3))).join(CntDom).map(l=>(l._2._1._2,(l._2._1._1,l._1,l._2._1._3),l._2._2))
//Choosing domains that has more then 10 products where each prodId has more then 25 distinct compatitors    
    val GrpByDom=FinalChosenProds.map(l=>(l._1,(l._2._2,l._3.toLong))).map(l=>(l._1,1)).reduceByKey((x, y) => x + y).filter(f=>(f._2>15)).join(FinalChosenProds.map(l=>(l._1,(l._2._2,l._2._3,l._3.toLong)))).map(l=>(l._1,(l._2._2._1,l._2._2._2,l._2._2._3)))
//Saving to HDFS
    val output =GrpByDom.map(l=>(l._1,l._2._1,l._2._2,l._2._3)).distinct()
    printDom2File(output,sc)
    
//Save2s3      
   //tuplelDataDom.saveAsObjectFile(Utils.S3STORAGE+"/rawd/objects/full") 
//ReadFromS3  
  //val tuplelDataDomS3 = sc.objectFile[(String, (String, String, String))](Utils.S3STORAGE+"/rawd/objects/full", 200)

//ReadFromHDFS
  //val tuplelDataDom = sc.objectFile[(String, (String, String, String))]("hdfs:///analysis/data/", 200)


}