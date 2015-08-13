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

object BadDomAnal {
  def main(args:Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
     var (cassandraHost, keySpace, tableCL, threshold ,path) = ("", "", "","","")
    if (args.size == 4) {
      cassandraHost = args(0)
      keySpace = args(1)
      tableCL = args(2)
      threshold = args(3)
      path=args(4)
    } else {
      cassandraHost = "localhost"
      keySpace = "demo"
      tableCL = "core_logs"
      threshold = "0.3"
      path=Utils.S3STORAGE+"/BadDomAnal"
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
    
    def makeComparison(JoinedT:DataFrame,SeedDomain:DataFrame,ComparisonTypeOne:String,ComparisonTypeTwo:String,threshold:String,tempTable:String,path:String){
      val Comp = {if (ComparisonTypeOne=="allPriceType" || ComparisonTypeTwo=="allAvgType") 
                   JoinedT.filter((JoinedT("modelprice")<(JoinedT("avgModelPrice")*(1-threshold.toDouble))&&
                   JoinedT("updatedprice")<(JoinedT("avgUpdatedPrice")*(1-threshold.toDouble))) ||
                  (JoinedT("modelprice")>(JoinedT("avgModelPrice")*(1+threshold.toDouble))&&
                   JoinedT("updatedprice")>(JoinedT("avgUpdatedPrice")*(1+threshold.toDouble))))
                  else 
                   JoinedT.filter(JoinedT(ComparisonTypeOne)<(JoinedT(ComparisonTypeTwo)*(1-threshold.toDouble)) ||
                   JoinedT(ComparisonTypeOne)>(JoinedT(ComparisonTypeTwo)*(1+threshold.toDouble)))}
      Comp.registerTempTable(tempTable)
      val CompResults = Comp.groupBy("domain").agg(count("domain") as "count")    
      CompResults.registerTempTable(tempTable+"Results")
      val TempRes=sqlContext.sql("SELECT domain, count(1) as value from "+tempTable+"Results"+" GROUP BY domain ORDER BY value DESC")
      val FinalRes=TempRes.join(SeedDomain,TempRes("domain")===SeedDomain("domain")).select(TempRes("domain"),TempRes("count"),SeedDomain("SeedPerDom"))
      FinalRes.saveAsParquetFile(path+"/"+tempTable)
      }
    
    //read core_logs data
    val coreLogsData = cc.sql("SELECT url,domain,price,updatedprice,modelprice,prodid,lastupdatedtime FROM " + keySpace + "." + tableCL ).cache
    //calculate number of seeds per domain
    val seedPerDomain=coreLogsData.groupBy("domain").agg(max(coreLogsData("domain")) as "domain", count(coreLogsData("domain")) as "SeedPerDom").cache
    //extract last date of update
    val maxLastUpdatedTime = coreLogsData.agg(max(coreLogsData("lastupdatedtime"))).rdd.take(1).mkString("").take(11).drop(1)
    //Avg modelPrice & updatedPrice grouped by domain and prodId
    val avgPrices = coreLogsData.groupBy("domain","prodid").agg(avg(coreLogsData("modelprice")) as "avgModelPrice" ,avg(coreLogsData("updatedprice")) as "avgUpdatedPrice" 
        , max(coreLogsData("domain")) as "domain", max(coreLogsData("prodid")) as "prodid").cache   
    //Real-time prices 
    val realTimePrices = coreLogsData.filter(coreLogsData("lastupdatedtime") >= maxLastUpdatedTime).cache
    coreLogsData.unpersist  
    //Join between avgPrices and realTimePrices
    val JoinedTable = avgPrices.join(realTimePrices,avgPrices("prodid")===realTimePrices("prodid") && avgPrices("domain")===realTimePrices("domain") ).select(avgPrices("prodid"),avgPrices("domain"),avgPrices("avgModelPrice"),avgPrices("avgUpdatedPrice"),realTimePrices("price"),realTimePrices("updatedprice"),realTimePrices("modelprice"),realTimePrices("lastupdatedtime"),realTimePrices("url"))
    avgPrices.unpersist
    realTimePrices.unpersist  
    //Current model price compared to average model price
    makeComparison(JoinedTable,seedPerDomain,"modelprice","avgModelPrice",threshold,"ModelComparison",path)
    //Current pattern price compared to average pattern price
    makeComparison(JoinedTable,seedPerDomain,"updatedprice","avgUpdatedPrice",threshold,"PatternComparison",path)
    //Current pattern price compared to average pattern price and model price
    makeComparison(JoinedTable,seedPerDomain,"allPriceType","allAvgType",threshold,"PatternAndModelComparison",path)
  }
}