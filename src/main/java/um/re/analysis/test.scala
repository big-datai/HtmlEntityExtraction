package um.re.analytics

import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}

object test {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)

    val sc = new SparkContext(conf)


    val Data = sc.cassandraTable("demo", "prod_metrics").map { row =>
      val store_id = row.get[String]("store_id")
      val sys_prod_id = row.get[String]("sys_prod_id")
      val sys_prod_title = row.get[String]("sys_prod_title")
      val max_abs_delta_val = row.get[Double]("max_abs_delta_val")
      val max_rel_delta_val = row.get[Double]("max_rel_delta_val")
      val max_rel_delta_level = row.get[Int]("max_rel_delta_level")
      val min_rel_delta_val = row.get[Double]("min_rel_delta_val")
      val min_abs_delta_val = row.get[Double]("min_abs_delta_val")
      val min_rel_delta_level = row.get[Int]("min_rel_delta_level")
      val price = row.get[Double]("price")
      val url = row.get[String]("url")
      val hot_level = row.get[Int]("hot_level")
      val abs_position = row.get[Int]("abs_position")
      val relative_position = row.get[Double]("relative_position")
      val position_level = row.get[Int]("position_level")
      val var_val = row.get[Double]("var_val")
      val var_level = row.get[Int]("var_level")
      val tmsp = row.get[String]("tmsp")
      (sys_prod_id, (store_id, sys_prod_title, max_abs_delta_val, max_rel_delta_val, max_rel_delta_level,
        min_rel_delta_val, min_abs_delta_val, min_rel_delta_level, price, url, hot_level, abs_position,
        relative_position, position_level, var_val, var_level, tmsp))
    }.cache


    val prodId1 = Data.filter { case (k, v) => k == "1002547791" }.cache
    prodId1.collect.foreach(println)

    val prodId2 = Data.filter { case (k, v) => k == "1002551799" }.cache
    prodId2.collect.foreach(println)

    val prodId3 = Data.filter { case (k, v) => k == "1002759208" }.cache
    prodId3.collect.foreach(println)

    val prodId4 = Data.filter { case (k, v) => k == "1002554574" }.cache
    prodId4.collect.foreach(println)

    val prodId5 = Data.filter { case (k, v) => k == "1002758807" }.cache
    prodId5.collect.foreach(println)


    val result = Data.map { case (sys_prod_id, (store_id, sys_prod_title, max_abs_delta_val, max_rel_delta_val, max_rel_delta_level,
    min_rel_delta_val, min_abs_delta_val, min_rel_delta_level, price, url, hot_level, abs_position,
    relative_position, position_level, var_val, var_level, tmsp)) => (hot_level.toString(), 1) //.reduceByKey(_ + _)}
    }.reduceByKey(_ + _).collect
  }

}