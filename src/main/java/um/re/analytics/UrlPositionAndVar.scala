
package um.re.analytics

import java.text.SimpleDateFormat
import java.util.Calendar

import com.datastax.spark.connector._
import com.utils.aws.AWSUtils
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.util.StatCounter

/**
  * @author eran
  */

object UrlPositionAndVar {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
    var (cassandraHost, keySpace, tableRT, tableVPT, threshold, numParts) = ("", "", "", "", "", "")
    if (args.size == 6) {
      cassandraHost = args(0)
      keySpace = args(1)
      tableRT = args(2)
      tableVPT = args(3)
      threshold = args(4)
      numParts = args(5)
    } else {
      cassandraHost = "localhost"
      val keySpace = "demo"
      val tableRT = "real_time_market_prices"
      val tableVPT = "real_time_price_pos_var"
      val threshold = "0.3"
      val numParts = "28"
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
    val partitioner = new HashPartitioner(numParts.toInt)
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ")
    val dt = format.format(Calendar.getInstance().getTime())

    val RtData = sc.cassandraTable(keySpace, tableRT).map { row =>
      val sys_prod_id = row.get[String]("sys_prod_id")
      val store_id = row.get[String]("store_id")
      val price = row.get[Double]("price")
      val url = row.get[String]("url")
      (sys_prod_id, (store_id, price, url))
    }.partitionBy(partitioner)

    RtData.groupByKey().flatMap {
      case (sys_prod_id, iter) =>
        var cnt = 0
        val NewTuple = iter.map {
          case (store_id, price, url) => (price, (store_id, url))
        }.toList.sorted.map {
          case (price, (store_id, url)) =>
            cnt += 1
            (sys_prod_id, price, store_id, url, cnt)
        }

        val sze = NewTuple.size
        val priceList = NewTuple.map { case (sys_prod_id, price, store_id, url, cnt) => price }
        val std = Math.sqrt(StatCounter(priceList).variance).toDouble
        val mean = StatCounter(priceList).mean
        val FinalTuples = NewTuple.map { case (sys_prod_id, price, store_id, url, cnt) =>
          val relPlace = (cnt.toDouble / sze)
          val cv = (std.toDouble / mean)
          val cvRank = {
            if (cv >= 0 && cv <= 0.2) 1
            else if (cv > 0.2 && cv <= 0.4) 2
            else if (cv > 0.4 && cv <= 0.6) 3
            else if (cv > 0.6 && cv <= 0.85) 4
            else 5
          }

          val t = (sys_prod_id, price, store_id, url, cnt, relPlace, cvRank)
          (sys_prod_id, price, store_id, url, cnt, relPlace, cvRank)

        }
        FinalTuples
    }.saveToCassandra(keySpace, tableVPT, SomeColumns("sys_prod_id", "price", "domain", "url", "place", "relplace", "cvrank"))
  }
}


/*

CREATE TABLE demo.real_time_price_pos_var (
    url text,
    sys_prod_id text,
    domain text,
    place text,
    price double,
    relplace double,
    cvrank int,
    PRIMARY KEY (url, sys_prod_id)
) WITH CLUSTERING ORDER BY (sys_prod_id ASC);
 
CREATE INDEX by_sys_prod_id
        ON real_time_price_pos_var (sys_prod_id);
        
SELECT * from real_time_price_pos_var WHERE sys_prod_id='1002257957' and relplace<0.2 ALLOW FILTERING;


######################################################################################################

CREATE TABLE demo.real_time_price_pos_var_new (
    url text,
    sys_prod_id text,
    domain text,
    place text,
    price double,
    relplace double,
    cvrank int,
    PRIMARY KEY ((domain),relplace,cvrank,sys_prod_id));
    
//) WITH CLUSTERING ORDER BY (domain ASC);
import java.text.SimpleDateFormat
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ")
      val dt = format.format(Calendar.getInstance().getTime())


 
*/
