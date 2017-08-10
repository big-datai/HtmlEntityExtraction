package um.re.emr

import com.datastax.spark.connector._
import com.utils.aws.AWSUtils
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * @author mike
  */
object AutoMatcher {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)

    var (cassandraHost, keySpace, tableRT, tableMP, tableMPT, tableCMS, storeID, numParts) = ("", "", "", "", "", "", "", "")
    if (args.size == 8) {
      cassandraHost = args(0)
      keySpace = args(1)
      tableRT = args(2)
      tableMP = args(3)
      tableMPT = args(4)
      tableCMS = args(5)
      storeID = args(6)
      numParts = args(7)
    } else {
      cassandraHost = "localhost"
      keySpace = "demo"
      tableRT = "real_time_market_prices"
      tableMP = "matching_prods"
      tableMPT = "matching_prods_by_tmsp"
      tableCMS = "cms_simulator"
      storeID = "*"
      numParts = "100"
      conf.setMaster("local[*]")
    }
    // try getting inner IPs
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

    //set accumulators
    val realTimeMarketPricesRowsCounter = sc.accumulator(0L)
    val CMSRowsCounter = sc.accumulator(0L)
    val MPRowsCounter = sc.accumulator(0L)
    val MPTRowsCounter = sc.accumulator(0L)
    try {
      val partitioner = new HashPartitioner(numParts.toInt)
      val cms = {
        if (storeID.equals("*"))
          sc.cassandraTable(keySpace, tableCMS)
        else
          sc.cassandraTable(keySpace, tableCMS).where("store_id = ?", storeID)
      }.map { row =>
        val store_id = row.get[String]("store_id")
        val store_prod_id = row.get[String]("store_prod_id")
        //val store_prod_price = row.get[String]("store_prod_price") 
        val store_prod_title = row.get[String]("store_prod_title")
        val store_prod_url = row.get[String]("store_prod_url")
        val store_domain = row.get[String]("store_domain")
        CMSRowsCounter += 1
        (store_id + "||" + store_prod_title, (store_id, store_prod_id, store_domain, store_prod_title, store_prod_url))
      }.partitionBy(partitioner)

      val realTimeMarketPrices = sc.cassandraTable(keySpace, tableRT).map { row =>
        val store_id = row.get[String]("store_id")
        val sys_prod_id = row.get[String]("sys_prod_id")
        val sys_prod_title = row.get[String]("sys_prod_title")
        val store_doamin = row.get[String]("store_domain")
        realTimeMarketPricesRowsCounter += 1
        (store_id + "||" + sys_prod_title, (store_id, sys_prod_title, store_doamin, sys_prod_id))
      }.partitionBy(partitioner)

      val matchingProds = cms.join(realTimeMarketPrices).map { case (key, ((store_id, store_prod_id, store_doamin, store_prod_title, store_prod_url), (s, sys_prod_title, store_domain, sys_prod_id))) =>
        MPRowsCounter += 1
        (store_id, store_prod_id, store_domain, 0, store_prod_title, sys_prod_id, sys_prod_title, store_prod_url)
      }.cache

      matchingProds.saveToCassandra(keySpace, tableMP, SomeColumns("store_id", "store_prod_id", "store_domain", "analyze_ind", "store_prod_title", "sys_prod_id", "sys_prod_title", "url"))

      val matchingProdsByTmsp = matchingProds.map { case (store_id, store_prod_id, store_domain, analye_ind, store_prod_title, sys_prod_id, sys_prod_title, url) =>
        val date = new java.util.Date
        MPTRowsCounter += 1
        (store_id, date, store_domain, store_prod_id, store_prod_title, sys_prod_id, sys_prod_title, url)
      }
      matchingProdsByTmsp.saveToCassandra(keySpace, tableMPT, SomeColumns("store_id", "tmsp", "store_domain", "store_prod_id", "store_prod_title", "sys_prod_id", "sys_prod_title", "url"))

      println("!@!@!@!@!   realTimeMarketPricesRowsCounter : " + realTimeMarketPricesRowsCounter.value +
        "\n!@!@!@!@!   CMSRowsCounter : " + CMSRowsCounter.value +
        "\n!@!@!@!@!   MPRowsCounter : " + MPRowsCounter.value +
        "\n!@!@!@!@!   MPTRowsCounter : " + MPTRowsCounter.value)
    } catch {
      case e: Exception => {
        println("########  Somthing went wrong :( ")
        println("#?#?#?#?#?#?#  ExceptionMessage : " + e.getMessage +
          "\n#?#?#?#?#?#?#  ExceptionStackTrace : " + e.getStackTraceString)
      }
    }

  }
}