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

object DomAnalysis extends App {
  val conf_s = new SparkConf()
  val sc = new SparkContext(conf_s)

//Load html data with  Title
    val data = new UConfAnal(sc, 200)
    val all = data.getData
    //val list = List("richtonemusic.co.uk","wholesalesupplements.shop.rakuten.com","shop.everythingbuttheweddingdress.com","DiscountCleaningProducts.com","yesss.co.uk","idsecurityonline.com","janitorialequipmentsupply.com","sanddollarlifestyles.com","protoolsdirect.co.uk","educationalinsights.com","faucet-warehouse.com","rexart.com","chronostore.com","racks-for-all.shop.rakuten.com","musicdirect.com","budgetpackaging.com","americanblinds.com","overthehill.com","thesupplementstore.co.uk","intheholegolf.com","alldesignerglasses.com","nitetimetoys.com","instrumentalley.com","ergonomic-chairs.officechairs.com","piratescave.co.uk")
    //val list = List("fawnandforest.com","parrotshopping.com")
    
    //list of domains 
    //TODO load list of domains that are relevant minCandNum==80
    def domainList(allData:RDD[(String, Map[String, String])],minCandNum:Int): RDD[(String)]={
    val domain = all.map {l => Utils.getDomain(l._2.apply("url"))}
    val words = domain.flatMap(x => x.split(","))
    val countDomain = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)
    countDomain.filter(d => d._2>=minCandNum).map(l=>l._1)
    }
  
    val dMap = sc.textFile((Utils.S3STORAGE + Utils.DMODELS + "dlist"), 1).collect().mkString("\n").split("\n").map(l => (l.split("\t")(0), l.split("\t")(1))).toMap
    val parsed = Transformer.parseDataPerURL(all).cache

    val list = args(0).split(",").filter(s => !s.equals("")).filter(dMap.keySet.contains(_))

    sc.parallelize(list.toSeq, 1).saveAsTextFile("/dima/list/" + list.apply(0)+System.currentTimeMillis().toString().replace(" ", "_"))
    //TODO join html domains on list of domains that are relevant
    
    //TODO add count of urls per each domain (using word count)
    //TODO choose Kth percentile of domains => according to # of urls
    
    //TODO choose top K products from the domains above
    
    //TODO count per each product (from the top K) in how many competitors it is contained (all domains) 
    for (d <- list) {

        // filter domain group by url (url => Iterator.cadidates)
        val parsedDataPerURL = parsed.filter(l => l._2._4.equals(d)).groupBy(_._1)

       
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