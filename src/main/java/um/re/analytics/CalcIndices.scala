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
import org.apache.spark.util.StatCounter

object CalcIndices {
 
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
    var (cassandraHost, keySpace, tableHP, tableRT, tableCL ,tablePM, numParts) = ("", "", "", "", "", "", "")
    if (args.size == 7) {
      cassandraHost = args(0)
      keySpace = args(1)
      tableHP = args(2)
      tableRT = args(3)
      tableCL = args(4)
      tablePM = args(5)
      numParts = args(6)
    } else {
      cassandraHost = "localhost"
      keySpace = "demo"
      tableHP = "historical_prices"
      tableRT = "real_time_market_prices"
      tableCL = "core_logs"
      tablePM = "prod_metrics"
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
    
    def descretize(cont:Double):Int = {
      val abs = Math.abs(cont)
      if(abs <=0.2) 1 
      else if (abs >0.2 && abs <=0.4) 2
      else if (abs >0.4 && abs <=0.6) 3
      else if (abs >0.6 && abs <=0.8) 4
      else if (abs >0.8 && abs <=1) 5
      else 6}
    
    conf.set("spark.cassandra.connection.host", cassandraHost)
    val sc = new SparkContext(conf)
    val cal = Calendar.getInstance()
    val today=cal.getTime()
    cal.add(Calendar.DATE, -6)
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
    val deltas = hp.groupByKey(partitioner).map{
      case((sys_prod_id, store_id),iter)=>
        val sortedList = iter.toList.map{case(store_id, sys_prod_id, tmsp, price, sys_prod_title) =>
          (tmsp,(store_id, sys_prod_id, tmsp, price, sys_prod_title))}.sorted.reverse
        val (store_id, sys_prod_id, tmsp, price, sys_prod_title) = sortedList.head._2
        val currentPrice = price
        
        if (iter.count(_ => true)>1){
        val previousPrice = sortedList.tail.head._2._4
        val delta = currentPrice-previousPrice
        val relativeChange = delta/previousPrice
        (sys_prod_id,(store_id, sys_prod_id, tmsp, price, sys_prod_title,delta,relativeChange))}
        else {
          (sys_prod_id,(store_id, sys_prod_id, tmsp, price, sys_prod_title,0.0,0.0))
        }
        
    }
    
    val deltaData = deltas.groupByKey(partitioner).flatMap{
      case(sys_prod_id,iter)=>
        val sourceList = iter.toList
        val sortedByDelta = sourceList.map{case(store_id, sys_prod_id, tmsp, price, sys_prod_title,delta,relativeChange)=>
          (delta,(store_id, sys_prod_id, tmsp, price, sys_prod_title,delta,relativeChange))}.sorted
       val sortedByRelativeChange = sourceList.map{case(store_id, sys_prod_id, tmsp, price, sys_prod_title,delta,relativeChange)=>
          (relativeChange,(store_id, sys_prod_id, tmsp, price, sys_prod_title,delta,relativeChange))}.sorted
       // val top2ByDelta = (sortedByDelta.reverse.head,sortedByDelta.reverse.tail.head)
       // val bottom2ByDelta = (sortedByDelta.head,sortedByDelta.tail.head)
       // val top2ByRelativeChange = (sortedByRelativeChange.reverse.head,sortedByRelativeChange.reverse.tail.head)
       // val bottom2ByRelativeChange = (sortedByRelativeChange.head,sortedByRelativeChange.tail.head)
          val top2ByDelta = (sortedByDelta.reverse.head,"")//sortedByDelta.reverse.tail.head)
          val bottom2ByDelta = (sortedByDelta.head,"")//sortedByDelta.tail.head)
          val top2ByRelativeChange = (sortedByRelativeChange.reverse.head,"")//sortedByRelativeChange.reverse.tail.head)
          val bottom2ByRelativeChange = (sortedByRelativeChange.head,"")//sortedByRelativeChange.tail.head)
          val stores = sourceList.map{case((store_id, sys_prod_id, tmsp, price, sys_prod_title,delta,relativeChange)) =>
          (store_id,sys_prod_title)}.toList.sorted
       val stats = (top2ByDelta,bottom2ByDelta,top2ByRelativeChange,bottom2ByRelativeChange)
       val results =  stores.map { case (store_id,sys_prod_title) => 
        val all =  (sys_prod_id,sys_prod_id,store_id,stats) 
        val max_abs_delta_val = top2ByDelta._1._1
        val max_rel_delta_val_cont = top2ByRelativeChange._1._1
        val max_rel_delta_level = descretize(max_rel_delta_val_cont)
        val min_abs_delta_val = bottom2ByDelta._1._1
        val min_rel_delta_val_cont = bottom2ByRelativeChange._1._1
        val min_rel_delta_level = descretize(min_rel_delta_val_cont)
        ((store_id,sys_prod_id),(sys_prod_title,max_abs_delta_val,max_rel_delta_val_cont,max_rel_delta_level,min_rel_delta_val_cont,min_abs_delta_val,min_rel_delta_level))}
       results}
    
       
      
  
   val RtData = sc.cassandraTable(keySpace, tableRT).map{row => 
        val sys_prod_id = row.get[String]("sys_prod_id") 
        val store_id = row.get[String]("store_id")
        val price = row.get[Double]("price") 
        val url = row.get[String]("url")
        val hot = row.get[String]("hot_level")
        (sys_prod_id,(store_id,price,url,hot))
        }
        
   val varPosData = RtData.groupByKey(partitioner).flatMap{
                          case(sys_prod_id,iter) =>
                          var cnt=0            
                          val NewTuple = iter.map{
                                   case(store_id,price,url,hot) =>(price,(store_id,url,hot))}.toList.sorted.map{
                                     case(price,(store_id,url,hot)) => 
                                         cnt+=1
                                         (sys_prod_id,price,store_id,url,hot,cnt)
                                        }

                          val sze=NewTuple.size
                          val priceList = NewTuple.map{case(sys_prod_id,price,store_id,url,hot,cnt) => price}
                          val std = Math.sqrt(StatCounter(priceList).variance).toDouble
                          val mean=StatCounter(priceList).mean
                          val FinalTuples=NewTuple.map{case(sys_prod_id,price,store_id,url,hot,cnt) =>
                            val relPlace=(cnt.toDouble/sze)
                            val cv = (std.toDouble/mean)
                            val cvRank={if(cv >= 0 && cv <=0.2) 1 
                              else if (cv > 0.2 && cv <=0.4) 2
                              else if (cv > 0.4 && cv <=0.6) 3
                              else if (cv > 0.6 && cv <=0.85) 4
                              else 5}
                           
                            val relPlaceRank={if(relPlace >= 0 && relPlace <=0.05) 5 
                              else if (relPlace > 0 && relPlace <=0.05) 5
                              else if (relPlace > 0.05 && relPlace <=0.1) 10
                              else if (relPlace > 0.1 && relPlace <=0.2) 20
                              else if (relPlace > 0.2 && relPlace <=0.3) 30
                              else if (relPlace > 0.3 && relPlace <=0.4) 40
                              else if (relPlace > 0.4 && relPlace <=0.5) 50
                              else if (relPlace > 0.5 && relPlace <=0.6) 60
                              else if (relPlace > 0.6 && relPlace <=0.7) 70
                              else if (relPlace > 0.7 && relPlace <=0.8) 80
                              else if (relPlace > 0.8 && relPlace <=0.9) 90
                              else if (relPlace > 0.9 && relPlace <=0.95) 95                          
                              else 100}
                            
                          //  val t= (sys_prod_id,price,store_id,url,cnt,relPlace,cvRank)
                            ((store_id,sys_prod_id),(price,url,hot,cnt,relPlace,relPlaceRank,cv,cvRank))
                            
                          }
                         FinalTuples
                }//.saveToCassandra(keySpace, tableVPT, SomeColumns("sys_prod_id","price","domain","url","place","relplace","cvrank"))        

   
  val t= (deltaData.join(varPosData)).map(l=>(l._1._1,l._1._2,
            l._2._1._1,l._2._1._2,l._2._1._3,l._2._1._4,l._2._1._5,l._2._1._6,l._2._1._7,
            l._2._2._1,l._2._2._2,l._2._2._3,l._2._2._4,l._2._2._5,l._2._2._6,l._2._2._7,l._2._2._8,
            today))
            
            t.saveToCassandra(keySpace, tablePM, 
      SomeColumns("store_id","sys_prod_id","sys_prod_title","max_abs_delta_val","max_rel_delta_val",
          "max_rel_delta_level","min_rel_delta_val","min_abs_delta_val","min_rel_delta_level",
          "price","url","hot_level","abs_position","relative_position","position_level","var_val","var_level","tmsp"))  
  } 
}


/*
 

 
CREATE TABLE demo.prod_metrics (
    store_id text,
    hot_level int,
    var_level int,
    position_level int,
    max_rel_delta_level int,
    min_rel_delta_level int,
    tmsp timestamp,
    sys_prod_id text,
    abs_position int,
    max_abs_delta_val double,
    max_rel_delta_val double,
    min_abs_delta_val double,
    min_rel_delta_val double,
    price double,
    relative_position double,
    sys_prod_title text,
    url text,
    var_val double,
    PRIMARY KEY (store_id, hot_level, var_level, position_level, max_rel_delta_level, min_rel_delta_level, tmsp, sys_prod_id)
    ) WITH CLUSTERING ORDER BY (hot_level ASC, var_level ASC, position_level ASC, max_rel_delta_level ASC, min_rel_delta_level ASC, tmsp DESC, sys_prod_id ASC)
    AND default_time_to_live = 259200; 
 
 */
