package um.re.light


import org.apache.spark.SparkContext
import org.apache.spark._
import kafka.producer._
import um.re.utils.Utils
import org.apache.spark.sql.cassandra.CassandraSQLContext
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.hive.HiveContext

object ReduceLoadCassQueries extends App{

//import org.apache.spark.sql.functions._  
 //val conf=new SparkConf().setMaster("local")  
   //                         .setAppName("CountingSheep")

    //                        .set("spark.executor.memory", "1g")



 val conf = new SparkConf(true)
    .setAppName(getClass.getSimpleName)
/*
  var (cassandraHost, keySpace, messagesCT,inputFilePath,numPartitions ) = ("", "", "", "", "")
  if (args.size == 5) {
    cassandraHost = args(0)
    keySpace = args(1)
    messagesCT = args(2)
    inputFilePath = args(3)
    numPartitions = args(4)
  } else {
    cassandraHost = "127.0.0.1"
    keySpace = "dev"
    messagesCT = "messages"
    inputFilePath = "/Users/dmitry/umbrella/seeds_sample"
    numPartitions = "200"
    conf.setMaster("local[*]")
  }
  
  */
     conf.setMaster("local[*]")
   val cassandraHost = "127.0.0.1"
  conf.set("spark.cassandra.connection.host", cassandraHost)

 val sc = new SparkContext(conf)                            
 val cc = new CassandraSQLContext(sc)
 val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
 val cal = Calendar.getInstance()
 //Set how many days back
 cal.add(Calendar.DATE, -5);
 val dateBefore5Days = cal.getTime();
 //print(dateBefore5Days)
 val datetime = dateFormat.format(dateBefore5Days);
 //println(datetime)
 val df = cc.sql("SELECT  sys_prod_id, store_id, tmsp, price, 0 as delta from dev.historical_prices where tmsp  <='" + datetime +"'"
     +" ORDER BY store_id, tmsp ASC ")

        
  
  /*
     val bla=cc.sql("select t1.sys_prod_id,t1.store_id,cast(t1.tmsp as Date) as dt1,cast(t2.tmsp as Date) as dt2,t1.price,t2.price,"
                    + "(t1.price-t2.price) as delta from dev.historical_prices t1 "
                    +"left outer JOIN dev.historical_prices t2 "
                    +"on t1.sys_prod_id = t2.sys_prod_id "
                    +"and t1.store_id = t2.store_id "
                    +"and cast(t1.tmsp as Date) > cast(t2.tmsp as Date) "
                    +"where t1.tmsp <='" + datetime +"'"
                    +"and t1.tmsp <='" + datetime +"'")
   */                 
     // val bb=bla.select("dt1")
// - (cast(t2.tmsp as Date)).getTIme =1")
                   // +"and cast(t1.tmsp as Date)- cast(t2.tmsp as Date) =1")
               //     diffInMillies = date2.getTime() - date1.getTime()
 //
 // val dd=df.groupBy("sys_prod_id","store_id","tmsp","price").agg($"sys_prod_id")
// val mm = dd.select("sys_prod_id as spd")
//val tst=dd.groupBy("sys_prod_id","count").agg("sys_prod_id" as "col1", "count" as "col2_total").printSchema 
 // val ee= df.select("store_id","sys_prod_id","tmsp", "price").groupBy("store_id","sys_prod_id")
  
 //val ee= df.select("sys_prod_id","store_id").distinct.groupBy("sys_prod_id").count()
 //val ff= dd.select("sys_prodid", "count" )
 //val df1 = df.withColumn("newCol", df("col") + 1) 
//val ff=df1.join(df2, df1("sys_prod_id1") === df2("sys_prod_id2"))
 //df.map(l=>l.sys_pro)

     
      val sparkConf = new SparkConf().setAppName("HiveFromSpark")
   
    // A hive context adds support for finding tables in the MetaStore and writing queries
    // using HiveQL. Users who do not have an existing Hive deployment can still create a
    // HiveContext. When not configured by the hive-site.xml, the context automatically
    // creates metastore_db and warehouse in the current directory.
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    import hiveContext.sql
  //df.take(100).foreach(println)
  val b=df.select(rowNumber().over(Window.partitionBy("store_id").orderBy("tmsp")))
 // val c=df.select(rowNumber().over(Window))
   
      //over(partition By("k")).partitionBy("k")
        //       .orderBy("k")
              
        // .alias("rowNum"))
         
         
  /*
  df.select("k", "v",
        
      df.rowNumber()
         .over(Window
               .partitionBy("k")
               .orderBy("k")
              )
         .alias("rowNum")
        )
 .show()
)
*/
println(b)
}