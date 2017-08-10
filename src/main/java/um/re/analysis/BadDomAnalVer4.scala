package um.re.analysis

import java.text.SimpleDateFormat
import java.util.Calendar

import com.utils.aws.AWSUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

object BadDomAnalVer4 {

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
      path = args(5)
    } else {
      cassandraHost = "localhost"
      val keySpace = "demo"
      val tableCL = "core_logs"
      val tableRT = "real_time_market_prices"
      val threshold = "0.3"
      val path = "/home/ec2-user/badDomainAnal"
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


    def makeComparison(JoinedT: DataFrame, SeedDomain: DataFrame, ComparisonTypeOne: String, ComparisonTypeTwo: String, threshold: String, tempTable: String): DataFrame
    = {
      val Comp = JoinedT.filter(JoinedT(ComparisonTypeOne) < (JoinedT(ComparisonTypeTwo) * (1 - threshold.toDouble)) ||
        JoinedT(ComparisonTypeOne) > (JoinedT(ComparisonTypeTwo) * (1 + threshold.toDouble)))
      val CompResults = Comp.groupBy("domain").agg(max("domain") as "domain", count("domain") as "count").cache
      val rdd = CompResults.map { l => Temp(l.getString(0), l.getLong(1)) }
      val df = sqlContext.createDataFrame(rdd)
      df.registerTempTable(tempTable + "Results")
      val TempRes = sqlContext.sql("SELECT * from " + tempTable + "Results" + " ORDER BY numBadSeed DESC")
      TempRes.join(SeedDomain, TempRes("domain") === SeedDomain("domain")).select(TempRes("domain"), TempRes("numBadSeed"), SeedDomain("SeedPerDom"))
    }

    //read core_logs data
    val coreLogsData = cc.sql("SELECT url,domain,price,updatedprice,modelprice,selectedprice,prodid,lastupdatedtime FROM " + keySpace + "." + tableCL).cache
    val UniqCoreLogsData = coreLogsData.groupBy("url", "prodid").agg(coreLogsData("url") as "url", coreLogsData("prodid") as "prodid", max(coreLogsData("lastupdatedtime")) as "lastupdatedtime")
    // UniqCoreLogsData.rdd.coalesce(1, false).saveAsTextFile(path+"/"+"test")


    //calculate number of seeds per domain UpdatedCoreLogsData.filter(UpdatedCoreLogsData("store_id") === "abglovesandabrasives.com").count
    val maxLastUpdatedTime = coreLogsData.agg(max(coreLogsData("lastupdatedtime"))).rdd.take(1).mkString("").take(11).drop(1)
    val realTime = coreLogsData.filter(coreLogsData("lastupdatedtime") >= maxLastUpdatedTime).cache
    val seedPerDomain = realTime.groupBy("domain").agg(max(realTime("domain")) as "domain", count(realTime("domain")) as "SeedPerDom").cache
    //seedPerDomain.filter(seedPerDomain("domain") === "aceofficemachines.com").show()  abglovesandabrasives.com


    val rdd = seedPerDomain.map { l => Seed(l.getString(0), l.getLong(1)) }.cache
    val df = sqlContext.createDataFrame(rdd)
    df.registerTempTable("Seeds")
    val SeedsDomain = sqlContext.sql("Select * from Seeds")

    val avgPricesAllDom = coreLogsData.filter(coreLogsData("modelprice") > 0 && coreLogsData("updatedprice") > 0 && coreLogsData("selectedprice") > 0 && coreLogsData("price") > 0).groupBy("prodid").agg(avg(coreLogsData("modelprice")) as "allAvgModelPrice", avg(coreLogsData("updatedprice")) as "allAvgUpdatedPrice",
      avg(coreLogsData("selectedprice")) as "allAvgSelectedPrice"
      , max(coreLogsData("prodid")) as "prodid")

    val allAvgIndices = avgPricesAllDom.select(
      avgPricesAllDom("prodid"),
      avgPricesAllDom("allAvgModelPrice"),
      avgPricesAllDom("allAvgUpdatedPrice"),
      avgPricesAllDom("allAvgSelectedPrice")
    )

    //Real-time prices
    val realTimePrices = realTime.select(realTime("price"), realTime("prodid"), realTime("domain"))
    // coreLogsData.unpersist
    //Join between avgPrices and realTimePrices
    val JoinedTable = allAvgIndices.join(realTimePrices, allAvgIndices("prodid") === realTimePrices("prodid")).select(
      allAvgIndices("prodid"),
      realTimePrices("domain"),
      allAvgIndices("allAvgModelPrice"),
      allAvgIndices("allAvgUpdatedPrice"),
      allAvgIndices("allAvgSelectedPrice"),
      realTimePrices("price"))
    JoinedTable.cache
    // avgPrices.unpersist
    //realTimePrices.unpersist


    val UpdatedCoreLogsData = coreLogsData.join(UniqCoreLogsData, coreLogsData("url") === UniqCoreLogsData("url") && coreLogsData("lastupdatedtime") === UniqCoreLogsData("lastupdatedtime") && coreLogsData("prodid") === UniqCoreLogsData("prodid")
    ).select(coreLogsData("url"),
      coreLogsData("domain"),
      coreLogsData("price"),
      coreLogsData("updatedprice"),
      coreLogsData("modelprice"),
      coreLogsData("selectedprice"),
      coreLogsData("prodid"),
      coreLogsData("lastupdatedtime")
    ).cache

    val format = new SimpleDateFormat("dd-MM-y")
    val dt = format.format(Calendar.getInstance().getTime())
    //primePrice compared to pattern price
    val PrimePatternRes = makeComparison(UpdatedCoreLogsData, SeedsDomain, "updatedprice", "price", threshold, "PrimePatternComparison")
    PrimePatternRes.rdd.coalesce(1, false).saveAsTextFile(path + "/" + dt + "/PrimePatternComparison")
    //primePrice compared to model price
    val PrimeModelRes = makeComparison(UpdatedCoreLogsData, SeedsDomain, "modelprice", "price", threshold, "PrimeModelComparison")
    PrimeModelRes.rdd.coalesce(1, false).saveAsTextFile(path + "/" + dt + "/PrimeModelComparison")
    //primePrice compared to selected price
    val PrimeSelectedRes = makeComparison(UpdatedCoreLogsData, SeedsDomain, "selectedprice", "price", threshold, "PrimeSelectedComparison")
    PrimeSelectedRes.rdd.coalesce(1, false).saveAsTextFile(path + "/" + dt + "/PrimeSelectedComparison")
    //Pattern compared to model price price
    val PatternModelRes = makeComparison(UpdatedCoreLogsData, SeedsDomain, "updatedprice", "modelprice", threshold, "PatternModelComparison")
    PatternModelRes.rdd.coalesce(1, false).saveAsTextFile(path + "/" + dt + "/PatternModelComparison")
    //Current selectedprice compared to all average model price
    val allAvgModelPriceRes = makeComparison(JoinedTable, SeedsDomain, "price", "allAvgModelPrice", threshold, "AllModelComparison")
    allAvgModelPriceRes.rdd.coalesce(1, false).saveAsTextFile(path + "/" + dt + "/allAvgModelPriceRes")
    //Current selectedprice compared to average pattern price
    val allAvgPatternComparisonRes = makeComparison(JoinedTable, SeedsDomain, "price", "allAvgUpdatedPrice", threshold, "AllPatternComparison")
    allAvgPatternComparisonRes.rdd.coalesce(1, false).saveAsTextFile(path + "/" + dt + "/allAvgPatternComparisonRes")
    //Current selectedprice compared to average selectedprice
    val allAvgSelectedPriceRes = makeComparison(JoinedTable, SeedsDomain, "price", "allAvgSelectedPrice", threshold, "AllSelectedPriceComparison")
    allAvgSelectedPriceRes.rdd.coalesce(1, false).saveAsTextFile(path + "/" + dt + "/allAvgSelectedPriceRes")
  }

  case class Temp(domain: String, numBadSeed: Long)

  case class Seed(domain: String, SeedPerDom: Long)
}