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
import um.re.analysis.UConfAnal
import um.re.analysis.UConfAnal2
import um.re.transform.Transformer
import um.re.utils.{ UConf }
import um.re.utils.Utils
import um.re.analysis.EsExporter2

object DomAnalysis extends App {
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
    val allCandid = dataCandid.getData
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
    val allCandid = dataCandid.getData
    val domainRelevantList= domainList(allCandid,minCandNum)
    val reldom =domainRelevantList.map(l=>(l,"domain"))
    tuplelDataDom.join(reldom).map(l=>(l._1,l._2._1))
   // val cntByK=partList1.countByKey().toList.map(l=>(l._2,l._1))
  //  val newCntByK=sc.parallelize(cntByK, 1).sortByKey(false)   
  //  val chosenDom=newCntByK.take(numOfChosenDom)
  //  val chosenDomRDD=sc.parallelize(chosenDom, 1).map(l=>(l._2,l._1))
  //  chosenDomRDD.join(partList1).map(l=>(l._1,l._2._2))
  }

 def printDom2File(output:RDD[(String, String, Long)],sc: SparkContext)={  
    val output2Print=output.map{t=> 
      (t._1+","+t._2+","+t._3).mkString("")}.collect().mkString("\n")
    sc.parallelize(List(output2Print), 1).saveAsTextFile("hdfs:///analysis/res/")
}     
 
/*
 def prodIdFrequncy(FinalChosenDom:RDD[(String, (String, String,String))],sc:SparkContext):RDD[(String, (String, String,String))] = {  
    val prodId = FinalChosenDom.map (l => l._2._2).map(x => (x, 1)).reduceByKey((x, y) => x + y)
    val reldom =domainRelevantList.map(l=>(l,"domain2"))
    val partList1 = tuplelDataDom.join(reldom).map(l=>(l._1,l._2._1))
    val cntByK=partList1.countByKey().toList.map(l=>(l._2,l._1))
    val newCntByK=sc.parallelize(cntByK, 1).sortByKey(false)
    val numOfChosenDom=(newCntByK.count * Precentage/100).toInt
    val chosenDom=newCntByK.take(numOfChosenDom)
    val chosenDomRDD=sc.parallelize(chosenDom, 1).map(l=>(l._2,l._1))
    chosenDomRDD.join(partList1).map(l=>(l._1,l._2._2))
  }
 */  
//Load html data with  Title
    val dataHtmls = new UConfAnal(sc, 200)
    val allHtmls = dataHtmls.getData
    val tuplelDataDom = allHtmls.map (l=>((Utils.getDomain(l._1),(l._2.apply("url"),(l._2.apply("prod_id")),(l._2.apply("title"))))))

//Join on domains that are relevant (minCandNum==80) and choosing Kth percentile of domains => according to # of urls 
    val FinalChosenDom=relevantDomains(tuplelDataDom,80,sc)
//Choose distinct (domain, prod_id)
    val DistinctDomProd= FinalChosenDom.map(l=>(l._1,l._2._2)).distinct()
//Choose prod_id which occur in more then 25 distinct domains => (prod_id, # of occurrences in distinct domains )
    val CntDom= DistinctDomProd.map(l=>(l._2,1)).reduceByKey((x, y) => x + y).filter(f=>(f._2>25)).map(l=>(l._2,l._1)).sortByKey(false).map(l=>(l._2,l._1))
//Joining  # of occurrences of prod_id in distinct domains 
    val FinalChosenProds=FinalChosenDom.map(l=>(l._2._2,(l._2._1,l._1,l._2._3))).join(CntDom).map(l=>(l._2._1._2,(l._2._1._1,l._1,l._2._1._3),l._2._2))
//Choosing domains that has more then 10 products where each prod_id has more then 25 distinct compatitors    
    val GrpByDom=FinalChosenProds.map(l=>(l._1,(l._2._2,l._3.toLong))).map(l=>(l._1,1)).reduceByKey((x, y) => x + y).filter(f=>(f._2>10)).join(FinalChosenProds.map(l=>(l._1,(l._2._2,l._3.toLong)))).map(l=>(l._1,(l._2._2._1,l._2._2._2)))
//Saving to HDFS
    val output =GrpByDom.map(l=>(l._1,l._2._1,l._2._2)).distinct()
    printDom2File(output,sc)
    
//Save2s3      
   //tuplelDataDom.saveAsObjectFile(Utils.S3STORAGE+"/rawd/objects/full") 
//ReadFromS3  
  //val tuplelDataDomS3 = sc.objectFile[(String, (String, String, String))](Utils.S3STORAGE+"/rawd/objects/full", 200)

//ReadFromHDFS
  //val tuplelDataDom = sc.objectFile[(String, (String, String, String))]("hdfs:///analysis/data/", 200)

    
    
    
    
 //TODO count per each product (from the top K) in how many competitors it is contained (out of all domains) 
   
 //   for (d <- list) {

        // filter domain group by url (url => Iterator.cadidates)
 //       val parsedDataPerURL = parsed.filter(l => l._2._4.equals(d)).groupBy(_._1)

       
        //     val scoreString = selectedScore.map { l =>
        //  d + " : " + l.toString
       // }
        
          //sc.parallelize(scoreString, 1).saveAsTextFile(Utils.HDFSSTORAGE + Utils.DSCORES + dMap.apply(d)+System.currentTimeMillis().toString().replace(" ", "_")) // list on place i
         // selectedModel.save(sc, Utils.HDFSSTORAGE + Utils.DMODELS + dMap.apply(d)+System.currentTimeMillis().toString().replace(" ", "_"))
          // sc.parallelize(scoreString, 1).saveAsTextFile(Utils.S3STORAGE + Utils.DSCORES + dMap.apply(d)) // list on place i
          // selectedModel.save(sc, Utils.S3STORAGE + Utils.DMODELS + dMap.apply(d))
       
        //TODO add function to choose candidates and evaluate on url level

        //TODO CHOOSE MODEL BY F
   
  //  }
  
}