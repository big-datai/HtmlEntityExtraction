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

object ReduceLoad {

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
    val hot1_3 = cl.where(cl("price") !== cl("selectedprice"))
    val hot4 = cl.where(cl("price") === cl("modelprice") || cl("price") === cl("updatedprice")).distinct

    cl.select("url").distinct
    hot4.show(2)
    
    
  }
}