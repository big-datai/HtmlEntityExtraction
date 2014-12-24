package com.sql
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.SparkContext._
import scala.util.Try
import scala.reflect.runtime.universe

case class ltv_kpi_final_new(provider: String, /*event_type: String,*/ entitykey: String, entityvalue1: String, entityvalue2: String, entityvalue3: String, age: String, installs: String,
  kpi_measure: String, kpi_estimated_measure: String, /* kpi_actual_measure: Double,*/
  estimated_revenue: String, install_date: String, estimated_revenue_next_year: String, /*entityvalue4: String, entityvalue5: String, entityvalue6: String, entityvalue7: String,*/ product: String)
//case class ltv_kpi_final_new(provider: Int, /*event_type: String,*/ entitykey: Int, entityvalue1: String, entityvalue2: String, entityvalue3: String, age: Int, installs: Int, kpi_measure: Double, kpi_estimated_measure: Double,/* kpi_actual_measure: Double,*/ estimated_revenue: Double, install_date: Int, estimated_revenue_next_year: String, /*entityvalue4: String, entityvalue5: String, entityvalue6: String, entityvalue7: String,*/ product: String)
//								1               search               3               AR               NULL              	 NULL               	4             	  0    1.6460273667188956          1.6460273667188956               1                     0.006158458312033644        20140608               \N                                   \N               \N                     \N                   \N                   1

//case class ltv_kpi_final_new_string(provider: String, event_type: String, entitykey: Int, entityvalue1: String, entityvalue2: String, entityvalue3: String, age: String, installs: String, kpi_measure: String, kpi_estimated_measure: String, kpi_actual_measure: String, estimated_revenue: String, install_date: String, estimated_revenue_next_year: String, entityvalue4: String, entityvalue5: String, entityvalue6: String, entityvalue7: String, product: String)

object touchSQL {
  def parseDouble(s: String): Option[Double] = Try { s.toDouble }.toOption
  def parseInt(s: String): Option[Int] = Try { s.toInt }.toOption
  def getInt(o: Option[Int]): Int = {
    if (o == None) {
      0
    } else {
      o.get
    }
  }

  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  def main(args: Array[String]) {

    val ll = parseDouble("2014020w1")

    val kkk = parseInt("20140201")

    if (kkk != None) {
      println(kkk.get)
    }

    val conf = new SparkConf().setAppName("touchstone").setMaster("local[20]").set("spark.executor.memory", "20g")
    conf.set("spark.storage.memoryFraction", "0.5")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext._

    // createSchemaRDD is used to implicitly convert an RDD to a SchemaRDD.
    import sqlContext.createSchemaRDD

    val kpi = sc.textFile("hdfs://mastervir:8020/user/hive/warehouse/ltv_db.db/ltv_sp_ru_kpi_final_new_20141102_202331/product=SP_RU").map(_.split("\\u0001"))
      .filter(p => p(2).equals("7") && p(0).equals("2") && p.length > 17 && (parseInt(p(12)) != None) && getInt(parseInt(p(12))) > 20140201)
      .map(p => ltv_kpi_final_new(p(0), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(11), p(12), p(13), p.length.toString))

      
    //val kpi = sc.textFile("hdfs://mastervir:8020/user/hive/warehouse/ltv_db.db/ltv_sp_ru_kpi_final_new_20141102_202331/product=SP_RU/000000_0").map(_.split("\\u0001")).filter(l=>if(l!=null&&l.length>17){true}else{false}).filter(p=>p(2).equals("7"))//.map(p => ltv_kpi_final_new(p(0), p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(11), p(12), p(13), p(18)))
    // val kpi = sc.textFile("hdfs://mastervir:8020/user/hive/warehouse/ltv_db.db/ltv_kpi_final_new/product=SP_RU/000000_0").map(_.split("\\u0001")).filter(l=>if(l.length>17){true}else{false}).filter(p=>p(2).equals("7")).map(p => ltv_kpi_final_new(p(0).trim.toInt, p(2).trim.toInt, p(3), p(4), p(5), p(6).trim.toInt, p(7).trim.toInt, p(8).trim.toDouble, p(9).trim.toDouble, p(11).trim.toDouble, p(12).trim.toInt, p(13), p(18)))
    val l = kpi.toArray.length

    println("======================== kpi ====================")
    kpi.take(3).foreach(println)

    println("======================== sp  ====================")
    //println(sp.take(3).toString.mkString("|||"))
    println("======================== pp  ====================")
    //println(pp.take(3).toString.mkString("|||"))

    kpi.registerTempTable("kpi")
    println("length of pp: " + kpi.toArray.length)

    val res = sqlContext.sql("SELECT * FROM kpi where entityvalue2='US' and install_date>20140201 and age in (4,6,9) LIMIT 10")
    println("length of sql: " + res.toArray.length)

    println("kpi length: " + l)
    res.toArray.foreach(println)

    /*
    val kpi_test = sc.textFile("hdfs://mastervir:8020/user/hive/warehouse/ltv_db.db/ltv_kpi_final_new/product=SP_RU/000000_0").flatMap(_.split("\\u0001"))

    kpi.registerTempTable("kpi")

    val res = sqlContext.sql("SELECT * FROM kpi LIMIT 10")
    
    println(res.toArray.mkString(" "))

     println(res.toArray.length)
     
    res.map(t => "Name: " + t(0)).collect().foreach(println)

    println(kpi_test.take(19).mkString("               "))    
    val raw = sc.textFile("hdfs://mastervir:8020/user/hive/warehouse/ltv_db.db/ltv_kpi_final_new/product=SP_RU/000000_0")
    val raw_test = sc.textFile("hdfs://mastervir:8020/user/hive/warehouse/ltv_db.db/locations/000000_0")
    val words = raw.flatMap(line => line.split("\\u0001")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    println(raw.toArray.length)
    println(words.take(2).mkString)

    System.setProperty("spark.cores.max", "5")
    System.getenv("JAVA_OPTS")
    System.setProperty("JAVA_OPTS", "-Xms30g -Xmx30g -XX:MaxPermSize=30g -XX:-UseGCOverheadLimit")
    System.setProperty("JAVA_OPTS", " -XX:-UseGCOverheadLimit")
    */
  }

}