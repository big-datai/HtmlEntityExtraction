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
import um.re.transform.Transformer
import um.re.utils.{ UConf }
import um.re.utils.Utils

object DomAnalysis extends App {
  val conf_s = new SparkConf()
  val sc = new SparkContext(conf_s)

  
  
  def domainList(allData:RDD[(String, Map[String, String])],minCandNum:Int): RDD[(String)]={
    val domain = allData.map {l => Utils.getDomain(l._2.apply("url"))}
    val words = domain.flatMap(x => x.split(","))
    val countDomain = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)
    countDomain.filter(d => d._2>=minCandNum).map(l=>l._1)
    }
  
// def topKdomains()
  
  /*
  def domainsList(allData:RDD[(String, Map[String, String])],minCandNum:Int,minGrpNum:Int): RDD[(String, Long)]={
    val domain = allData.map {l => Utils.getDomain(l._2.apply("url"))}
    val words = domain.flatMap(x => x.split(","))
    val countDomain = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)
    val domainList = countDomain.filter(d => d._2>=minCandNum)
    val indexedDomainList=domainList.map(n => n._1).zipWithIndex
    def domNameGrp(indexedDomainList: RDD[(String,Long)]): RDD[(String,Long)] = {
      val domainGrp= indexedDomainList.count/minGrpNum
      indexedDomainList.map(n => (n._1, n._2 % domainGrp))
      }
   domNameGrp(indexedDomainList)
  }
  
  */
  
  
//Load html data with  Title
    val dataHtmls = new UConfAnal(sc, 200)
    val allHtmls = dataHtmls.getData
    //val list = List("richtonemusic.co.uk","wholesalesupplements.shop.rakuten.com","shop.everythingbuttheweddingdress.com","DiscountCleaningProducts.com","yesss.co.uk","idsecurityonline.com","janitorialequipmentsupply.com","sanddollarlifestyles.com","protoolsdirect.co.uk","educationalinsights.com","faucet-warehouse.com","rexart.com","chronostore.com","racks-for-all.shop.rakuten.com","musicdirect.com","budgetpackaging.com","americanblinds.com","overthehill.com","thesupplementstore.co.uk","intheholegolf.com","alldesignerglasses.com","nitetimetoys.com","instrumentalley.com","ergonomic-chairs.officechairs.com","piratescave.co.uk")
    //val list = List("fawnandforest.com","parrotshopping.com")
  //  all.take(1).foreach(println)
   // val test= all.take(1).map(l=> l._1)
   // val test2= all.take(1).map(l=> l._2)//.filter(n=> n._1=="title")//.filter(l=>l._1="title")
  //   val partialDataDom = allHtmls.map (l=>((l._2.apply("raw_text"),"domain1")))
     //val partialDataUrl = allHtmls.map (l=>((l._2.apply("raw_text"),l._2.apply("url"))))
     //val partialDataTitles = allHtmls.map (l=>((l._2.apply("raw_text"),l._2.apply("title"))))
    // val partialDataTitle = allHtmls.map (l=>((l._2.apply("raw_text"),l._2.apply("title")))
    val partialDataDom = allHtmls.map (l=>((l._2.apply("raw_text"),(l._2.apply("url"),l._2.apply("title")))))
      
     //,("url",l._2.apply("url"))
 //   val pairs = title.map(x => (x.split("+")(0), x))
    //list of domains 
    //TODO load list of domains that are relevant minCandNum==80
    
    val dataCandid = new UConf(sc, 200)
    val allCandid = dataCandid.getData
    val domainRelevantList= domainList(allCandid,80)
    val reldom =domainRelevantList.map(l=>(l,"domain2"))
    val partList1 = partialDataDom.join(reldom).map(l=>(l._1,l._2._1))
    val cntByK=partList1.countByKey().toList.map(l=>(l._2,l._1))
    val newCntByK=sc.parallelize(cntByK, 1).sortByKey(false)
    val numOfChosenDom=(newCntByK.count * 20/100).toInt
    val chosenDom=newCntByK.take(numOfChosenDom)
    // val accum = sc.accumulator(0)
    //val chosenDom=newCntByK.foreach(x => accum += 1)
         
 //   val partList2=partialDataDom.join(partialDataUrl)
 //   val partList3=partialDataDom.join(partialDataTitles)
 //   val partList4=partList1.join(partList2)
 //   val partList5=partList4.join(partList3)
    
    
    val dMap = sc.textFile((Utils.S3STORAGE + Utils.DMODELS + "dlist"), 1).collect().mkString("\n").split("\n").map(l => (l.split("\t")(0), l.split("\t")(1))).toMap
   //val parsed = Transformer.parseDataPerURL(all).cache

    val list = args(0).split(",").filter(s => !s.equals("")).filter(dMap.keySet.contains(_))

    sc.parallelize(list.toSeq, 1).saveAsTextFile("/dima/list/" + list.apply(0)+System.currentTimeMillis().toString().replace(" ", "_"))
    //TODO join html domains on list of domains that are relevant
    
    //TODO add count of urls per each domain (using word count)
    //TODO choose Kth percentile of domains => according to # of urls
    
    //TODO choose top K products from the domains above
    
    //TODO count per each product (from the top K) in how many competitors it is contained (all domains) 
    for (d <- list) {

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
   
    }
  
}