package um.re.emr

import com.datastax.spark.connector._
import com.utils.aws.AWSUtils
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


/**
 * @author mike
 */
object AutoMatcher {
  def main(args:Array[String]){
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)

    var (cassandraHost, keySpace, tableRT, tableMP, tableMPID) = ( "", "", "", "","")
    if (args.size == 5) {
      cassandraHost = args(0)
      keySpace = args(1)
      tableRT = args(2)
      tableMP = args(3)
      tableMPID = args(4)
      
    } else {
      cassandraHost = "localhost"
      keySpace = "demo"
      tableRT = "real_time_market_prices"
      tableMP = "matching_prods"
      tableMPID = "matching_prods_by_sys_prod_id"
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

    //conf.set("spark.cassandra.connection.host", cassandraHost)
    val sc = new SparkContext(conf)
    sc.setLocalProperty("spark.cassandra.connection.host", cassandraHost)
    
    //set accumulators
    val inputRowsCounter = sc.accumulator(0L)
    
    try {
    val matches = sc.cassandraTable(keySpace, tableRT).map{row => 
      val store_id = row.get[String]("store_id")
      val sys_prod_id = row.get[String]("sys_prod_id")
      val sys_prod_title = row.get[String]("sys_prod_title")
      inputRowsCounter += 1
      (store_id,sys_prod_title,sys_prod_id,sys_prod_title,sys_prod_id,"",1)
      }.cache
    matches.saveToCassandra(keySpace, tableMP
        , SomeColumns("store_id","store_prod_title","store_prod_id","sys_prod_title","sys_prod_id","url","analyze_ind"))
    matches.saveToCassandra(keySpace, tableMPID
        , SomeColumns("store_id","store_prod_title","store_prod_id","sys_prod_title","sys_prod_id","url","analyze_ind"))
    println("inputRowsCounter : "+inputRowsCounter.value)
    } catch {
      case e: Exception => {
        println("########  Somthing went wrong :( ")
        println("#?#?#?#?#?#?#  ExceptionMessage : " + e.getMessage +
          "\n#?#?#?#?#?#?#  ExceptionStackTrace : " + e.getStackTraceString)
      }
    }
    
  }
}