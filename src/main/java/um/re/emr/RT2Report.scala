package um.re.emr

import com.datastax.spark.connector._
import com.utils.aws.AWSUtils
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.CassandraSQLContext
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Session
import com.datastax.driver.core.Statement
import com.datastax.driver.core.querybuilder.QueryBuilder
import QueryBuilder.{eq => equ}
import com.datastax.driver.core.PoolingOptions
import com.datastax.driver.core.HostDistance

/**
 * @author eran
 */
object RT2Report {
    def main(args: Array[String]) {
   val conf = new SparkConf(true)
      .setAppName(getClass.getSimpleName)
  
    var (cassandraHost, keySpace, tableRT,storeId) = ("", "", "","")
    if (args.size == 4) {
      cassandraHost = args(0)
      keySpace = args(1)
      tableRT = args(2)
      storeId=args(3)
    } else {
      cassandraHost = "localhost"
      keySpace = "demo"
      tableRT = "real_time_market_prices"
      storeId="EliteFixtures.com"
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
    val cc = new CassandraSQLContext(sc)
     
try{
 val results = cc.sql("SELECT sys_prod_id, store_id, price, sys_prod_title, url from demo.real_time_market_prices where store_id='"+storeId+"'").toDF()
 // val poolingOptions = new PoolingOptions()
 // poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL,  4).setMaxConnectionsPerHost( HostDistance.LOCAL, 40)  
  lazy val cluster = Cluster.builder().addContactPoint(cassandraHost).build()//.withPoolingOptions(poolingOptions).build();
  lazy val session = cluster.connect("demo");
 
    
val z = results.repartition(5).mapPartitions { x => 
    val newPartition= 
       x.map{l=>
       val sPID=l.get(0)
       val statement = QueryBuilder.select()
        .all()
        .from("demo", "real_time_market_prices")
        .allowFiltering()
        .where(QueryBuilder.eq("sys_prod_id",sPID));
     val result = session.execute(statement)
     var resultList : List[(Double,(String,String,String,Double,String,String,String))] = List()
     var firstTuple=(0.0,("","","",0.0,"","","")) 
     while (!result.isExhausted()){
          val tempRes = result.one()
          val tempAns=(tempRes.getDouble(3),(tempRes.getString(0),tempRes.getString(1),tempRes.getString(2),tempRes.getDouble(3),tempRes.getString(4),tempRes.getString(5),tempRes.getString(6)))
          if (tempAns._2._2==storeId){
            firstTuple=tempAns
          }else resultList = resultList :+ tempAns  
          
      }     
           resultList=firstTuple::resultList.sorted   
           var isMin=1
           var isMax=1
           if (resultList.tail!=Nil){
             isMin= if(resultList.tail.head._1< resultList.head._1){0}else{1}
             isMax=if(resultList.tail.reverse.head._1> resultList.head._1){0}else{1}
           }
          val minPrice=resultList.sorted.head._1
          val maxPrice=resultList.sorted.reverse.head._1
          val numCompetitors=resultList.size
          val firstR=//resultList.head._2._1+','+
                     resultList.head._2._6+','+
                     numCompetitors+','+
                     isMin+','+
                     isMax+','+
                     resultList.head._2._4+','+
                     minPrice+','+
                     maxPrice+','+
                     resultList.head._2._7
                     
          
         var newList=List[String]()
         newList=firstR::newList
         var fullString=""
         resultList.tail.map{l=>
           fullString=fullString+','+l._2._2+','+l._2._4+','+l._2._7
          
            }
     val fin =newList :+ fullString 
     fin.mkString
      }
      newPartition
 }


val finalRDD=sc.parallelize(Array("MPN+Title,Num_Comp,Is_Min,Is_Max,My_Price,Min_Price,Max_Price,My_URL"))
val finalFile=finalRDD.union(z)
finalFile.coalesce(1, true).saveAsTextFile("/home/ec2-user/"+storeId+"_Report");
session.close()
cluster.close()    
 
  println("!@!@!@!@!   Written2CSV : " + z.count())

} catch {
      case e: Exception => {
        println("########  Somthing went wrong :( ")
        println("#?#?#?#?#?#?#  ExceptionMessage : " + e.getMessage +
          "\n#?#?#?#?#?#?#  ExceptionStackTrace : " + e.getStackTraceString)
      }
    }
 
    }
}
 
