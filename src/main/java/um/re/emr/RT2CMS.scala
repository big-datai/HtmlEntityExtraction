package um.re.emr

import com.datastax.spark.connector._
import com.utils.aws.AWSUtils
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * @author mike
 */
object RT2CMS {
    def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)

    var (cassandraHost, keySpace, tableRT, tableCMS) = ("", "", "", "")
    if (args.size == 4) {
      cassandraHost = args(0)
      keySpace = args(1)
      tableRT = args(2)
      tableCMS = args(3)
    } else {
      cassandraHost = "localhost"
      keySpace = "demo"
      tableRT = "real_time_market_prices"
      tableCMS = "cms_simulator"
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
    val rowsCounter = sc.accumulator(0L)
    try {
      val transformedRT2CMS = sc.cassandraTable(keySpace, tableRT)
         .map{row => 
        val store_id = row.get[String]("store_id") 
        val sys_prod_id = row.get[String]("sys_prod_id")
        val price = row.get[String]("price") 
        val sys_prod_title = row.get[String]("sys_prod_title")
        rowsCounter += 1
        (store_id,sys_prod_id,price,sys_prod_title,"")
        }
      
       transformedRT2CMS.saveToCassandra(keySpace, tableCMS, SomeColumns("store_id","store_prod_id" ,"store_prod_price" ,"store_prod_title","store_prod_url"))
     
     println("!@!@!@!@!   rowsCounter : " + rowsCounter.value)
    } catch {
      case e: Exception => {
        println("########  Somthing went wrong :( ")
        println("#?#?#?#?#?#?#  ExceptionMessage : " + e.getMessage +
          "\n#?#?#?#?#?#?#  ExceptionStackTrace : " + e.getStackTraceString)
      }
    }

  }
}