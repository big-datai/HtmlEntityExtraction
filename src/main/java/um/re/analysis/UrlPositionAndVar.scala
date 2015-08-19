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

object UrlPositionAndVar {
 def main(args:Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
     var (cassandraHost, keySpace, tableRT,tablePV, threshold ,path) = ("", "", "","","","")
    if (args.size == 5) {
      cassandraHost = args(0)
      keySpace = args(1)
      tableRT = args(2)
      tablePV = args(3)
      threshold = args(4)
      path=args(5)
    } else {
      cassandraHost = "localhost"
      val keySpace = "demo"
      val tableRT="real_time_market_prices"
      val tablePT = "real_time_price_position"
      val threshold = "0.3"
      val path="/home/ec2-user"
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
       
    //need to add url field into realTime table and also in code
   // val realTime =cc.sql("SELECT store_id,sys_prod_id,price FROM " + keySpace + "." + tableRT)
      val Data =cc.sql("SELECT store_id,sys_prod_id,price FROM " + keySpace + "." + "historical_prices limit 2000")
     // val realTime =Data.distinct.cache
  
      val Rdata=Data.select(Data("store_id"),Data("sys_prod_id"),Data("price")).distinct.cache
      
     val TestData=Rdata.filter(Rdata("sys_prod_id")==="1002557569" || Rdata("sys_prod_id")==="1001800638" ||Rdata("sys_prod_id")=== "1001874870")
     // val TestData=Rdata.filter(Rdata("sys_prod_id")==="1002557569")
    //val OrderedByPrice=realTime.orderBy(realTime("price")).groupBy("sys_prod_id").agg(max(realTime("sys_prod_id")) as "prodid", realTime("price") as "price",realTime("url") as "url" ,realTime("domain") as "domain").cache
    //val OrderedByPrice=realTime.orderBy(realTime("price")).groupBy("sys_prod_id","price").agg(max(realTime("sys_prod_id")) as "prodid", realTime("price") as "price").cache

    val ditinctProdId=TestData.select(Rdata("sys_prod_id")).distinct.collect
    
    TestData.rdd.
    
    
    
    ditinctProdId.map{ l =>
    println(l.mkString(""))
    //val Uniqdata = Rdata.filter(Rdata("sys_prod_id")===l.mkString("")).orderBy("price")//.dropRight(1).drop(1))
    val Uniqdata = Rdata.filter(Rdata("sys_prod_id")==="1002557569")
   
    
    Uniqdata.show
    }
 
 /*
    val ditinctProdId=realTime.select(realTime("sys_prod_id")).distinct.collect
    ditinctProdId.map{ l => 
    val OrderedByPrice = realTime.filter(realTime("sys_prod_id")===l.toString()).count
  */    
      
    
    
    
    
  }
}