package um.re.analytics

import org.apache.spark.SparkConf
import com.utils.aws.AWSUtils
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import um.re.utils.Utils
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.HashPartitioner
object Hotspots {
 def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
    var (cassandraHost, keySpace, tableHP, tableRT, tableCL, threshold, path) = ("", "", "", "", "", "", "")
    if (args.size == 4) {
      cassandraHost = args(0)
      keySpace = args(1)
      tableHP = args(2)
      tableRT = args(3)
      threshold = args(4)
      path = args(5)
    } else {
      cassandraHost = "localhost"
      keySpace = "demo"
      tableHP = "historical_prices"
      tableRT = "real_time_market_prices"
      path = "/home/ec2-user"
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
    //read core_logs data

    //val (keySpace, tableHP, tableCL) = ("demo", "historical_prices", "core_logs")

    val hp = cc.sql("SELECT * FROM " + keySpace + "." + tableHP).cache
    //url,domain, prodid, price, selectedprice,updatedprice,modelprice
    val cl = cc.sql("SELECT * FROM " + keySpace + "." + tableCL).cache
    val hot1_3 = cl.select(cl("url"), cl("prodid"), cl("domain")).where(cl("price") !== cl("selectedprice")).distinct
    val hot4 = cl.where(cl("price") === cl("modelprice") || cl("price") === cl("updatedprice")).distinct

    val h13 = hp.join(cl, hp("sys_prod_id") === cl("prodid") && hp("store_id") === cl("domain")).select("url", "sys_prod_id", "store_id", "tmsp", "selectedprice")
    h13.orderBy("store_id", "sys_prod_id", "tmsp").groupBy("sys_prod_id", "store_id")
    h13.registerTempTable("h13")
    h13.sqlContext.sql("select * from h13 where ")

    val keyed = h13.rdd.map { r => (r.getString(0), (r.getString(0), r.getString(1), r.getString(2), r.getString(3))) }

    val partitioner = new HashPartitioner(100)
    keyed.partitionBy(partitioner)
    val priceChangesPerUrl=keyed.groupByKey().mapValues { it =>
      val tmPrice = it.map(p => (p._3, p._4)).toSeq
      var tmps = tmPrice.map(l => (l._1, l._2.toDouble, 0)).sortBy(_._1).toArray
      var sum=0
      for (i <- 0 to tmps.size - 1) {
        if (tmps.apply(i + 1)._2 - tmps.apply(i)._2 == 0) {
          //tmps(i + 1) = (tmps.apply(i + 1)._1, tmps.apply(i + 1)._2, 1)
          sum=sum+1
        }
      }
    (it,sum)  
    }

    cl.select("url").distinct
    hot4.show(2)

  }
}