package um.re.analytics

import java.sql.Timestamp
import java.util.Calendar

import scala.reflect.runtime.universe

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.HashPartitioner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.cassandra.CassandraSQLContext

import com.datastax.spark.connector.SomeColumns
import com.datastax.spark.connector.toNamedColumnRef
import com.datastax.spark.connector.toRDDFunctions

import um.re.utils.Utils

object Hotspots {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
    val sc = new SparkContext(conf)

    val cc = new CassandraSQLContext(sc)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val d = Calendar.getInstance().getTime()
    d.toString.replace(" ", "").replace(":", "")

    val (keySpace, tableHP, tableCL) = ("demo", "historical_prices", "core_logs")
    val hp = cc.sql("SELECT sys_prod_id, store_id,tmsp, price AS h_price, sys_prod_title FROM " + keySpace + "." + tableHP)

    val urlKeyData = hp.rdd.map { r =>
      val time = r.getAs[Timestamp](2)
      time.setHours(0)
      time.setMinutes(0)
      time.setSeconds(0)
      time.setNanos(0)
      (r.getString(0) + r.getString(1), (r.getString(0), r.getString(1), time, r.getDouble(3), r.getString(4)))
    }.distinct()

    val partitioner = new HashPartitioner(100)
    //count number of historical price changes 
    val priceChangesPerUrl = urlKeyData.groupByKey(partitioner).mapValues { it =>
      val tmps = it.map(p => (p._3, p._4)).toSeq.map { l => (l._1, l._2) }.toMap.toArray.sortWith(_._1.getTime > _._1.getTime)
      var sum = 0
      for (i <- 0 to tmps.size - 2) { if (math.abs(tmps.apply(i + 1)._2 - tmps.apply(i)._2) > 0.0) { sum = sum + 1 } }
      (sum, it.map(v => (sum, v)))
    }.cache

    val reduce = priceChangesPerUrl.map { l => (l._1, l._2._1) }
    val r1 = reduce.filter { l => (l._2 >= 6) }
    val r2 = reduce.filter { l => (l._2 < 6 && l._2 >= 3) }
    val r3 = reduce.filter { l => (l._2 < 3 && l._2 >= 1) }
    val r4 = reduce.filter { l => (l._2 == 0) }

    //uncomment for statistics
    //val distribution = reduce.map(l => (l._1, 1)).reduceByKey(_ + _) //.map { l => (l._1, l._2.count(p => true)) }
    //distribution.collect.foreach(println)

    //import seeds
    val rawSeeds = sc.textFile("s3n://AKIAJQUAOI7EBC6Y7ESQ:JhremVoqNuEYG8YS9J+duW0hFRtX+sWjuZ0vdQlE@dpavlov/seedsFilteredMonAug24142239UTC2015")
    val parsedSeeds = rawSeeds.map { line =>
      try {
        val data = Utils.json2Map(Utils.string2Json(line))
        (data.apply("prodId") + data.apply("domain"), data)
      } catch {
        case e: Exception => null
      }
    }.filter { _ != null }.cache

    //save reduce seeds
    val s1 = r1.join(parsedSeeds).map { l =>
      val rank = l._2._1
      val m = l._2._2 - "errorLocation" + ("errorLocation" -> "1")
      Utils.map2JsonString(m)
    }.saveAsTextFile("s3n://AKIAJQUAOI7EBC6Y7ESQ:JhremVoqNuEYG8YS9J+duW0hFRtX+sWjuZ0vdQlE@dpavlov/seedsReduce1_" + d.toString.replace(" ", "").replace(":", ""), classOf[GzipCodec])
    val s2 = r2.join(parsedSeeds).map { l =>
      val rank = l._2._1
      val m = l._2._2 - "errorLocation" + ("errorLocation" -> "2")
      Utils.map2JsonString(m)
    }.saveAsTextFile("s3n://AKIAJQUAOI7EBC6Y7ESQ:JhremVoqNuEYG8YS9J+duW0hFRtX+sWjuZ0vdQlE@dpavlov/seedsReduce2_" + d.toString.replace(" ", "").replace(":", ""), classOf[GzipCodec])
    val s3 = r3.join(parsedSeeds).map { l =>
      val rank = l._2._1
      val m = l._2._2 - "errorLocation" + ("errorLocation" -> "3")
      Utils.map2JsonString(m)
    }.saveAsTextFile("s3n://AKIAJQUAOI7EBC6Y7ESQ:JhremVoqNuEYG8YS9J+duW0hFRtX+sWjuZ0vdQlE@dpavlov/seedsReduce3_" + d.toString.replace(" ", "").replace(":", ""), classOf[GzipCodec])
    val s4 = r4.join(parsedSeeds).map { l =>
      val rank = l._2._1
      val m = l._2._2 - "errorLocation" + ("errorLocation" -> "4")
      Utils.map2JsonString(m)
    }.saveAsTextFile("s3n://AKIAJQUAOI7EBC6Y7ESQ:JhremVoqNuEYG8YS9J+duW0hFRtX+sWjuZ0vdQlE@dpavlov/seedsReduce4_" + d.toString.replace(" ", "").replace(":", ""), classOf[GzipCodec])

    //hot product    
    val reduce2 = priceChangesPerUrl.map { l => (l._2._2.head)}
    //test
    val test = reduce2.filter(l => l._2._1 == "1002547791")
    //uncomment for statistics
    /*
    val byProdId = reduce2.map { l => (l._2._1, l) }.groupByKey(partitioner).map { l =>
      val it = l._2
      val size = it.count(p => true)
      val sum = it.map(l => l._1).sum
      val avg = (sum / size)
      (l._1, avg)
    }
    val numOfHotInEachCategory = byProdId.map(l => (l._2, 1)).reduceByKey(_ + _)
     */
    val byProdIdHot = reduce2.map { l => (l._2._1, l) }.groupByKey(partitioner).map { l =>
      val it = l._2
      val size = it.count(p => true)
      val sum = it.map(l => l._1).sum
      val avg = (sum / size)
      val r = avg match {
        case 4 | 5 | 6 => "2"
        case 2 | 3     => "3"
        case 1         => "4"
        case 0         => "5"
        case _         => "1"
      }
      (l._1, r)
    }
//test
    val t2=byProdIdHot.filter(l=>l._1=="1002547791").collect
    
    val rtp = cc.sql("SELECT * FROM " + keySpace + "." + "real_time_market_prices").cache
    val hotDf = byProdIdHot.toDF("sys_prod_id", "hot_level")
    val res = rtp.select("sys_prod_id", "store_id", "price", "sys_prod_title", "url").join(hotDf, hotDf("sys_prod_id") === rtp("sys_prod_id")).select(rtp("sys_prod_id"), rtp("store_id"), hotDf("hot_level"), rtp("price"), rtp("sys_prod_title"), rtp("url"))
    res.rdd.map(l => (l.getString(0), l.getString(1), l.getString(2), l.getDouble(3), l.getString(4), l.getString(5))).saveToCassandra("demo", "real_time_market_prices", SomeColumns("sys_prod_id", "store_id", "hot_level", "price", "sys_prod_title", "url"))

  }
}


    //val sumUrl = priceChangesPerUrl.map { l => (l._2._1, (1, l._1)) } //.reduceByKey((x, y) => (x._1 + y._1, x._2 + ", " + y._2))
    //val sumUrl= priceChangesPerUrl.map{l=>(l._2._1,(1))}

    //val s2=sumUrl.reduceByKey(_+_)
    //s2.count()
    //s2.collect.foreach(println)

    //val hot1_3 = cl.select(cl("url"), cl("prodid"), cl("domain")).where(cl("price") !== cl("selectedprice")).distinct
    //val hot4 = cl.where(cl("price") === cl("modelprice") || cl("price") === cl("updatedprice")).distinct

    //val joinHPCL = hp//.join(cl, hp("sys_prod_id") === cl("prodid") && hp("store_id") === cl("domain")) //.select("url", "sys_prod_id", "store_id", "tmsp", "selectedprice", "h_price", "domain", "modelprice", "price").cache
    //joinHPCL.save("s3n://AKIAJQUAOI7EBC6Y7ESQ:JhremVoqNuEYG8YS9J+duW0hFRtX+sWjuZ0vdQlE@dpavlov/historic"+d.toString.replace(" ","").replace(":",""))

    //val data = sqlContext.parquetFile("s3n://AKIAJQUAOI7EBC6Y7ESQ:JhremVoqNuEYG8YS9J+duW0hFRtX+sWjuZ0vdQlE@dpavlov/historicCore23082015").persist(StorageLevel.MEMORY_AND_DISK)
    //data.registerTempTable("data")
    //val temp = data.sqlContext.sql("select url, sys_prod_id, store_id, selectedprice, h_price, tmsp, domain, modelprice, price, lastupdatedtime, cast(lastupdatedtime as timestamp) as lastupdatedtime2 from data") // where price!=selectedprice")
    //val temp = data.sqlContext.sql("select sys_prod_id, store_id,tmsp, h_price, sys_prod_title from data") // where price!=selectedprice")

    //sys_prod_id: string, store_id: string, tmsp: timestamp, h_price: double, sys_prod_title: string
    //val h13 = hp //temp.distinct //hp //temp.where(temp("lastupdatedtime2") > "2015-08-22 06:46:17.57")

    //val h4 = data.where(data("price") === data("selectedprice") && data("lastupdatedtime") > "2015-08-21 18:17:25+0000").select("url", "sys_prod_id", "store_id", "tmsp", "selectedprice", "h_price", "domain", "modelprice", "price")
    //url            url              prodID          storeId        h_price            h_price
    //sys_prod_id: string, store_id: string, tmsp: timestamp, h_price: double, sys_prod_title: string
    //val urlKeyData = h13.rdd.map { r => (r.getString(0), (r.getString(0), r.getString(1), r.getString(2), r.getDouble(4), r.getAs[Timestamp](5))) }