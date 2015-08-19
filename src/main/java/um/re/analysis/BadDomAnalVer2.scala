package um.re.analysis

import org.apache.spark.SparkConf
import com.utils.aws.AWSUtils
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import um.re.utils.Utils

object BadDomAnalVer2 {
  case class Temp(domain: String, numBadSeed: Long)
  case class Seed(domain: String, SeedPerDom: Long)

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
    var (cassandraHost, keySpace, tableCL, tableRT, threshold, path) = ("", "", "", "", "", "")
    if (args.size == 5) {
      cassandraHost = args(0)
      keySpace = args(1)
      tableCL = args(2)
      tableRT = args(3)
      threshold = args(4)
      path=args(5)
    } else {
      cassandraHost = "localhost"
      val keySpace = "demo"
      val tableCL = "core_logs"
      val tableRT="real_time_market_prices"
      val threshold = "0.3"
      val path="/home/ec2-user"
      conf.setMaster("local[*]")
    } 
   

    try {
      val innerCassandraHost = AWSUtils.getPrivateIp(cassandraHost)
      cassandraHost = innerCassandraHost
    } catch {
      case e: Exception => {
        println("#?#?#?#?#?#?#  Couldn't get inner Cassandra IP, using : " + cassandraHost +
          "\n#?#?#?#?#?#?#  ExceptionMessage : " + e.getMessage +
          "\n#?#?#?#?#?#?#  ExceptionStackTrace : " + e.getStackTraceString)
      }
    }
    
    conf.set("spark.cassandra.connection.host", cassandraHost)
    val sc = new SparkContext(conf)  
    val cc = new CassandraSQLContext(sc)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    
    def makeComparison(JoinedT:DataFrame,SeedDomain:DataFrame,ComparisonTypeOne:String,ComparisonTypeTwo:String,threshold:String,tempTable:String) : DataFrame
    ={
      val Comp=  JoinedT.filter(JoinedT(ComparisonTypeOne)<(JoinedT(ComparisonTypeTwo)*(1-threshold.toDouble)) ||
                   JoinedT(ComparisonTypeOne)>(JoinedT(ComparisonTypeTwo)*(1+threshold.toDouble)))
      val CompResults = Comp.groupBy("domain").agg(max("domain") as "domain", count("domain") as "count").cache    
      val rdd=CompResults.map{l=>Temp(l.getString(0), l.getLong(1))}
      val df=sqlContext.createDataFrame(rdd)
      df.registerTempTable(tempTable+"Results")
      val TempRes=sqlContext.sql("SELECT * from "+tempTable+"Results"+" ORDER BY numBadSeed DESC")
      TempRes.join(SeedDomain,TempRes("domain")===SeedDomain("domain")).select(TempRes("domain"),TempRes("numBadSeed"),SeedDomain("SeedPerDom"))  
      //FinalRes.rdd.coalesce(1, false).saveAsTextFile(path+"/"+tempTable)
    }
    
    //read core_logs data
    val coreLogsData = cc.sql("SELECT url,domain,price,updatedprice,modelprice,selectedprice,prodid,lastupdatedtime FROM " + keySpace + "." + tableCL ).cache
    
    //calculate number of seeds per domain
    val realTime =cc.sql("SELECT store_id,sys_prod_id,price FROM " + keySpace + "." + tableRT)
    val seedPerDomain=realTime.groupBy("store_id").agg(max(realTime("store_id")) as "domain", count(realTime("store_id")) as "SeedPerDom").cache
    //seedPerDomain.filter(seedPerDomain("domain") === "aceofficemachines.com").show()
   
    val rdd=seedPerDomain.map{l=>Seed(l.getString(0), l.getLong(1))}.cache
    val df=sqlContext.createDataFrame(rdd)
    df.registerTempTable("Seeds")
    val SeedsDomain=sqlContext.sql("Select * from Seeds")
    
    //extract last date of update
    //val maxLastUpdatedTime = coreLogsData.agg(max(coreLogsData("lastupdatedtime"))).rdd.take(1).mkString("").take(11).drop(1)
    //Avg modelPrice & updatedPrice grouped by domain and prodId
    
    val avgPricesSpecificDom = coreLogsData.filter(coreLogsData("modelprice")>0 && coreLogsData("updatedprice")>0 && coreLogsData("selectedprice")>0).groupBy("domain","prodid").agg(avg(coreLogsData("modelprice")) as "avgModelPrice" ,avg(coreLogsData("updatedprice")) as "avgUpdatedPrice",
         avg(coreLogsData("selectedprice")) as "avgSelectedPrice", max(coreLogsData("price")) as "Price"
        , max(coreLogsData("domain")) as "domain", max(coreLogsData("prodid")) as "prodid").cache 
       
    
    val PrimePricesSpecificDom = coreLogsData.filter(coreLogsData("price")>0).groupBy("domain","prodid").agg(max(coreLogsData("price")) as "PrimePrice"
        , max(coreLogsData("domain")) as "domain", max(coreLogsData("prodid")) as "prodid").cache 

    //Avg modelPrice & updatedPrice & selectedPrice & Price grouped by prodId
    /*val avgPricesAllDom = coreLogsData.filter(coreLogsData("modelprice")>0 && coreLogsData("updatedprice")>0 && coreLogsData("selectedprice")>0 && coreLogsData("price")>0).groupBy("prodid").agg(avg(coreLogsData("modelprice")) as "allAvgModelPrice" ,avg(coreLogsData("updatedprice")) as "allAvgUpdatedPrice" ,
        avg(coreLogsData("selectedprice")) as "allAvgSelectedPrice", avg(coreLogsData("price")) as "allAvgPrice"
        , max(coreLogsData("prodid")) as "prodid")*/
        
   val avgPricesAllDom = coreLogsData.filter(coreLogsData("modelprice")>0 && coreLogsData("updatedprice")>0 && coreLogsData("selectedprice")>0 && coreLogsData("price")>0).groupBy("prodid").agg(avg(coreLogsData("modelprice")) as "allAvgModelPrice" ,avg(coreLogsData("updatedprice")) as "allAvgUpdatedPrice" ,
        avg(coreLogsData("selectedprice")) as "allAvgSelectedPrice"
        , max(coreLogsData("prodid")) as "prodid")
    
    val allAvgIndices= avgPricesAllDom.select(
                  avgPricesAllDom("prodid"),
                  avgPricesAllDom("allAvgModelPrice"),
                  avgPricesAllDom("allAvgUpdatedPrice"),
                  avgPricesAllDom("allAvgSelectedPrice")
                 )
    
    //Real-time prices 
    val realTimePrices = realTime.select(realTime("price"),realTime("sys_prod_id") as "prodid",realTime("store_id") as "domain")
   // coreLogsData.unpersist  
    //Join between avgPrices and realTimePrices
    val JoinedTable = allAvgIndices.join(realTimePrices,allAvgIndices("prodid")===realTimePrices("prodid")) .select(
            allAvgIndices("prodid"),
            realTimePrices("domain"), 
            allAvgIndices("allAvgModelPrice"),
            allAvgIndices("allAvgUpdatedPrice"),
            allAvgIndices("allAvgSelectedPrice"),
            realTimePrices("price"))        
   JoinedTable.cache
     // avgPrices.unpersist
    //realTimePrices.unpersist  
   
   val specificDomJoinedTable=PrimePricesSpecificDom.join(realTimePrices,PrimePricesSpecificDom("prodid")===realTimePrices("prodid") && 
       PrimePricesSpecificDom("domain")===realTimePrices("domain")) .select(
          realTimePrices("price"),
          realTimePrices("domain"),
          PrimePricesSpecificDom("PrimePrice"),
          PrimePricesSpecificDom("prodid")
          )

    /*
    //Current selectedprice compared to average model price
    val avgModelPriceRes= makeComparison(JoinedTable,SeedsDomain,"price","avgModelPrice",threshold,"ModelComparison")
    //Current selectedprice compared to average pattern price
    val avgPatternComparisonRes = makeComparison(JoinedTable,SeedsDomain,"price","avgUpdatedPrice",threshold,"PatternComparison")
    //Current selectedprice compared to average selectedprice
    val avgSelectedPriceRes= makeComparison(JoinedTable,SeedsDomain,"price","avgSelectedPrice",threshold,"SelectedPriceComparison")
    //Current selectedprice compared to average selectedprice

    val PriceRes= makeComparison(JoinedTable,SeedsDomain,"price","Price",threshold,"PrimePriceComparison")
    allAvgModelPriceRes.rdd.coalesce(1, false).saveAsTextFile(path+"/"+"PrimePriceRes")
    
    */
    val PrimePriceRes= makeComparison(specificDomJoinedTable,SeedsDomain,"price","PrimePrice",threshold,"PrimePriceComparison")
    PrimePriceRes.rdd.coalesce(1, false).saveAsTextFile(path+"/"+"PrimePriceRes")
  
    //Current selectedprice compared to all average model price
    val allAvgModelPriceRes= makeComparison(JoinedTable,SeedsDomain,"price","allAvgModelPrice",threshold,"AllModelComparison")
    allAvgModelPriceRes.rdd.coalesce(1, false).saveAsTextFile(path+"/"+"allAvgModelPriceRes")
    //Current selectedprice compared to average pattern price
    val allAvgPatternComparisonRes = makeComparison(JoinedTable,SeedsDomain,"price","allAvgUpdatedPrice",threshold,"AllPatternComparison")
    allAvgPatternComparisonRes.rdd.coalesce(1, false).saveAsTextFile(path+"/"+"allAvgPatternComparisonRes")
    //Current selectedprice compared to average selectedprice
    val allAvgSelectedPriceRes= makeComparison(JoinedTable,SeedsDomain,"price","allAvgSelectedPrice",threshold,"AllSelectedPriceComparison")
    allAvgSelectedPriceRes.rdd.coalesce(1, false).saveAsTextFile(path+"/"+"allAvgSelectedPriceRes")
    //Current selectedprice compared to average selectedprice
  //  val allPriceRes= makeComparison(JoinedTable,SeedsDomain,"price","allAvgPrice",threshold,"AllPrimePriceComparison")
   // allPriceRes.rdd.coalesce(1, false).saveAsTextFile(path+"/"+"allPriceRes")
   // val FinalCompRes=avgModelPriceRes.join(avgPatternComparisonRes,avgModelPriceRes("domain")===avgPatternComparisonRes("domain"))
  }
}
