package um.re.analytics

import org.apache.spark.SparkConf
import com.utils.aws.AWSUtils
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.HashPartitioner
import um.re.utils.Utils
import java.util.Calendar
import java.util.Date

/**
 * @author mike
 */
object DeltaCalc {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
    var (cassandraHost, keySpace, tableHP, tableRT, tableCL, threshold, numParts) = ("", "", "", "", "", "", "")
    if (args.size == 4) {
      cassandraHost = args(0)
      keySpace = args(1)
      tableHP = args(2)
      tableRT = args(3)
      threshold = args(4)
      numParts = args(5)
    } else {
      cassandraHost = "localhost"
      keySpace = "demo"
      tableHP = "historical_prices"
      tableRT = "real_time_market_prices"
      numParts = "128"
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
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -2)
    val yesterday = cal.getTime
    yesterday.setHours(0)
    yesterday.setMinutes(0)
    yesterday.setSeconds(0)

    val partitioner = new HashPartitioner(numParts.toInt)

    val hp = sc.cassandraTable(keySpace, tableHP).where("tmsp > ?",yesterday.getTime).map { row =>
      val store_id = row.get[String]("store_id")
      val sys_prod_id = row.get[String]("sys_prod_id")
      val tmsp = row.get[java.util.Date]("tmsp")
      val price = row.get[Double]("price")
      val sys_prod_title = row.get[String]("sys_prod_title")
      ((sys_prod_id, store_id),(store_id, sys_prod_id, tmsp, price, sys_prod_title))
    }
    val deltas = hp.groupByKey(partitioner).filter{_._2.count(_=> true)>1}.map{
      case((sys_prod_id, store_id),iter)=>
        val sortedList = iter.toList.map{case(store_id, sys_prod_id, tmsp, price, sys_prod_title) =>
          (tmsp,(store_id, sys_prod_id, tmsp, price, sys_prod_title))}.sorted.reverse
        val (store_id, sys_prod_id, tmsp, price, sys_prod_title) = sortedList.head._2
        val currentPrice = price
        val previousPrice = sortedList.tail.head._2._4
        val delta = currentPrice-previousPrice
        val relativeChange = delta/previousPrice
        (sys_prod_id,(store_id, sys_prod_id, tmsp, price, sys_prod_title,delta,relativeChange))
    }
    
    val data = deltas.groupByKey(partitioner).filter{_._2.count(_=> true)>1}.flatMap{
      case(sys_prod_id,iter)=>
        val sourceList = iter.toList
        val sortedByDelta = sourceList.map{case(store_id, sys_prod_id, tmsp, price, sys_prod_title,delta,relativeChange)=>
          (delta,(store_id, sys_prod_id, tmsp, price, sys_prod_title,delta,relativeChange))}.sorted
       val sortedByRelativeChange = sourceList.map{case(store_id, sys_prod_id, tmsp, price, sys_prod_title,delta,relativeChange)=>
          (relativeChange,(store_id, sys_prod_id, tmsp, price, sys_prod_title,delta,relativeChange))}.sorted
        val top2ByDelta = (sortedByDelta.reverse.head,sortedByDelta.reverse.tail.head)
        val bottom2ByDelta = (sortedByDelta.head,sortedByDelta.tail.head)
        val top2ByRelativeChange = (sortedByRelativeChange.reverse.head,sortedByRelativeChange.reverse.tail.head)
        val bottom2ByRelativeChange = (sortedByRelativeChange.head,sortedByRelativeChange.tail.head)
          val stores = sourceList.map{case((store_id, sys_prod_id, tmsp, price, sys_prod_title,delta,relativeChange)) =>
          store_id}.toList.sorted
       val stats = (top2ByDelta,bottom2ByDelta,top2ByRelativeChange,bottom2ByRelativeChange)
       val results =  stores.map { store_id => 
         (sys_prod_id,store_id,stats) }
       results
    }

  }
}