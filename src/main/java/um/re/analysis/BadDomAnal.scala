package um.re.analysis

import com.utils.aws.AWSUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object BadDomAnal {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
    var (cassandraHost, keySpace, tableCL, threshold, path) = ("", "", "", "", "")
    if (args.size == 4) {
      cassandraHost = args(0)
      keySpace = args(1)
      tableCL = args(2)
      threshold = args(3)
      path = args(4)
    } else {
      cassandraHost = "localhost"
      val keySpace = "demo"
      val tableCL = "core_logs"
      val threshold = "0.3"
      val path = "/home/ec2-user"
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


    def makeComparison(JoinedT: DataFrame, SeedDomain: DataFrame, ComparisonTypeOne: String, ComparisonTypeTwo: String, threshold: String, tempTable: String, path: String) {
      val Comp = {
        if (ComparisonTypeOne == "allPriceType" || ComparisonTypeTwo == "allAvgType")
          JoinedT.filter((JoinedT("modelprice") < (JoinedT("avgModelPrice") * (1 - threshold.toDouble)) &&
            JoinedT("updatedprice") < (JoinedT("avgUpdatedPrice") * (1 - threshold.toDouble))) ||
            (JoinedT("modelprice") > (JoinedT("avgModelPrice") * (1 + threshold.toDouble)) &&
              JoinedT("updatedprice") > (JoinedT("avgUpdatedPrice") * (1 + threshold.toDouble))))
        else
          JoinedT.filter(JoinedT(ComparisonTypeOne) < (JoinedT(ComparisonTypeTwo) * (1 - threshold.toDouble)) ||
            JoinedT(ComparisonTypeOne) > (JoinedT(ComparisonTypeTwo) * (1 + threshold.toDouble)))
      }
      val CompResults = Comp.groupBy("domain").agg(max("domain") as "domain", count("domain") as "count").cache
      val rdd = CompResults.map { l => Temp(l.getString(0), l.getLong(1)) }
      val df = sqlContext.createDataFrame(rdd)
      df.registerTempTable(tempTable + "Results")
      val TempRes = sqlContext.sql("SELECT * from " + tempTable + "Results" + " ORDER BY numBadSeed DESC")
      val FinalRes = TempRes.join(SeedDomain, TempRes("domain") === SeedDomain("domain")).select(TempRes("domain"), TempRes("numBadSeed"), SeedDomain("SeedPerDom"))
      FinalRes.rdd.coalesce(1, false).saveAsTextFile(path + "/" + tempTable)
    }

    //read core_logs data
    val coreLogsData = cc.sql("SELECT url,domain,price,updatedprice,modelprice,prodid,lastupdatedtime FROM " + keySpace + "." + tableCL).cache
    //calculate number of seeds per domain
    val seedPerDomain = coreLogsData.groupBy("domain").agg(max(coreLogsData("domain")) as "domain", count(coreLogsData("domain")) as "SeedPerDom").cache

    val rdd = seedPerDomain.map { l => Seed(l.getString(0), l.getLong(1)) }.cache
    val df = sqlContext.createDataFrame(rdd)
    df.registerTempTable("Seeds")
    val SeedsDomain = sqlContext.sql("Select * from Seeds")
    //extract last date of update
    val maxLastUpdatedTime = coreLogsData.agg(max(coreLogsData("lastupdatedtime"))).rdd.take(1).mkString("").take(11).drop(1)
    //Avg modelPrice & updatedPrice grouped by domain and prodId
    val avgPrices = coreLogsData.groupBy("domain", "prodid").agg(avg(coreLogsData("modelprice")) as "avgModelPrice", avg(coreLogsData("updatedprice")) as "avgUpdatedPrice"
      , max(coreLogsData("domain")) as "domain", max(coreLogsData("prodid")) as "prodid").cache
    //Real-time prices
    val realTimePrices = coreLogsData.filter(coreLogsData("lastupdatedtime") >= maxLastUpdatedTime).cache
    coreLogsData.unpersist
    //Join between avgPrices and realTimePrices
    val JoinedTable = avgPrices.join(realTimePrices, avgPrices("prodid") === realTimePrices("prodid") && avgPrices("domain") === realTimePrices("domain")).select(avgPrices("prodid"), avgPrices("domain"), avgPrices("avgModelPrice"), avgPrices("avgUpdatedPrice"), realTimePrices("price"), realTimePrices("updatedprice"), realTimePrices("modelprice"), realTimePrices("lastupdatedtime"), realTimePrices("url"))
    avgPrices.unpersist
    realTimePrices.unpersist
    //Current model price compared to average model price
    makeComparison(JoinedTable, SeedsDomain, "modelprice", "avgModelPrice", threshold, "ModelComparison", path)
    //Current pattern price compared to average pattern price
    makeComparison(JoinedTable, SeedsDomain, "updatedprice", "avgUpdatedPrice", threshold, "PatternComparison", path)
    //Current pattern price compared to average pattern price and model price
    makeComparison(JoinedTable, SeedsDomain, "allPriceType", "allAvgType", threshold, "PatternAndModelComparison", path)
  }

  case class Temp(domain: String, numBadSeed: Long)

  case class Seed(domain: String, SeedPerDom: Long)
}
