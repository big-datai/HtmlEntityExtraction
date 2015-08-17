package um.re.emr

import com.utils.aws.AWSUtils
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import scala.collection.immutable.HashMap
import scala.collection.immutable.HashSet
/**
 * @author mike
 */
object SwapIDs {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)

    var (cassandraHost, keySpace, tableRT, tableCMS, tableHP, path2Mapping) = ("", "", "", "", "", "")
    if (args.size == 6) {
      cassandraHost = args(0)
      keySpace = args(1)
      tableRT = args(2)
      tableCMS = args(3)
      tableHP = args(4)
      path2Mapping = args(5)
    } else {
      cassandraHost = "localhost"
      keySpace = "demo"
      tableRT = "real_time_market_prices"
      tableCMS = "cms_simulator"
      tableHP = "historical_prices"
      path2Mapping = "/Users/mike/umbrella/mapping.txt"
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

    val connector = CassandraConnector.apply(sc.getConf)

    //set accumulators
    // for RT table
    val inputRTCounter = sc.accumulator(0L)
    val validRTCounter = sc.accumulator(0L)
    val newRTCounter = sc.accumulator(0L)
    val deletedRTCounter = sc.accumulator(0L)
    val missingMappingRTCounter = sc.accumulator(0L)
    // for HP table
    val inputHPCounter = sc.accumulator(0L)
    val validHPCounter = sc.accumulator(0L)
    val newHPCounter = sc.accumulator(0L)
    val deletedHPCounter = sc.accumulator(0L)
    val missingMappingHPCounter = sc.accumulator(0L)
    // for CMS table
    val inputCMSCounter = sc.accumulator(0L)
    val validCMSCounter = sc.accumulator(0L)
    val newCMSCounter = sc.accumulator(0L)
    val deletedCMSCounter = sc.accumulator(0L)
    val missingMappingCMSCounter = sc.accumulator(0L)

    //broadcast
    val emptyMap = new HashMap
    val mapping = emptyMap ++ sc.textFile(path2Mapping, 1).map { line =>
      val Array(origID, newID) = line.substring(1, line.length - 1).split(",")
      (origID, newID)
    }.collect().toMap
    val mappingBC = sc.broadcast(mapping)
    val emptySet = new HashSet
    val validIDs = emptySet ++ mapping.values.toSet
    val validIDsBC = sc.broadcast(validIDs)
    try {
      // swap process for RT table 
      val RT = sc.cassandraTable(keySpace, tableRT)
        .map { row =>
          val store_id = row.get[String]("store_id")
          val sys_prod_id = row.get[String]("sys_prod_id")
          val price = row.get[String]("price")
          val sys_prod_title = row.get[String]("sys_prod_title")
          val newID = mappingBC.value.get(sys_prod_id).getOrElse("missingMapping")

          inputRTCounter += 1
          ((store_id, newID, price, sys_prod_title), (sys_prod_id, store_id))
        }.filter {case (newRow, oldKey) =>
            if (validIDsBC.value.contains(oldKey._1)) {
              validRTCounter += 1
              false
            } else true
        }.cache
      RT.filter {
        case (newRow, oldKey) =>
          if (newRow._2.equals("missingMapping")) {
            missingMappingRTCounter += 1
            true
          } else
            false
      }.saveAsTextFile(path2Mapping.substring(0, path2Mapping.lastIndexOf("/") + 1) + "RT_missingMapping.txt")
      RT.filter { case (newRow, oldKey) => !newRow._2.equals("missingMapping") }
        .map { t =>
          newRTCounter += 1
          t._1
        }.saveToCassandra(keySpace, tableRT, SomeColumns("store_id", "sys_prod_id", "price", "sys_prod_title"))
      RT.filter { case (newRow, oldKey) => !newRow._2.equals("missingMapping") }.foreachPartition { part =>
        val session = connector.openSession()
        part.foreach {
          case (newRow, oldKey) =>
            val delete = s"DELETE FROM " + keySpace + "." + tableRT + " where     sys_prod_id='" + oldKey._1 + "' and store_id='" + oldKey._2 + "';"
            session.execute(delete)
            deletedRTCounter += 1
        }
        session.close()
      }
      RT.unpersist(false)

      // swap process for HP table 
      val HP = sc.cassandraTable(keySpace, tableHP)
        .map { row =>
          val store_id = row.get[String]("store_id")
          val sys_prod_id = row.get[String]("sys_prod_id")
          val tmsp = row.get[java.util.Date]("tmsp")
          val price = row.get[String]("price")
          val sys_prod_title = row.get[String]("sys_prod_title")
          val newID = mappingBC.value.get(sys_prod_id).getOrElse("missingMapping")

          inputHPCounter += 1
          ((store_id, newID, tmsp, price, sys_prod_title), (sys_prod_id, store_id, tmsp))
        }.filter {case (newRow, oldKey) =>
            if (validIDsBC.value.contains(oldKey._1)) {
              validHPCounter += 1
              false
            } else true
        }.cache
      HP.filter {
        case (newRow, oldKey) =>
          if (newRow._2.equals("missingMapping")) {
            missingMappingHPCounter += 1
            true
          } else
            false
      }.saveAsTextFile(path2Mapping.substring(0, path2Mapping.lastIndexOf("/") + 1) + "HP_missingMapping.txt")
      HP.filter { case (newRow, oldKey) => !newRow._2.equals("missingMapping") }
        .map { t =>
          newHPCounter += 1
          t._1
        }.saveToCassandra(keySpace, tableHP, SomeColumns("store_id", "sys_prod_id", "tmsp", "price", "sys_prod_title"))
      HP.filter { case (newRow, oldKey) => !newRow._2.equals("missingMapping") }.foreachPartition { part =>
        val session = connector.openSession()
        part.foreach {
          case (newRow, oldKey) =>
            val delete = s"DELETE FROM " + keySpace + "." + tableHP + " where     sys_prod_id='" + oldKey._1 + "' and store_id='" + oldKey._2 + "';"
            session.execute(delete)
            deletedHPCounter += 1
        }
        session.close()
      }
      HP.unpersist(false)

      // swap process for CMS table 
      val CMS = sc.cassandraTable(keySpace, tableCMS)
        .map { row =>
          val store_id = row.get[String]("store_id")
          val store_prod_id = row.get[String]("store_prod_id")
          val store_prod_price = row.get[String]("store_prod_price")
          val store_prod_title = row.get[String]("store_prod_title")
          val store_prod_url = row.get[String]("store_prod_url")
          val newID = mappingBC.value.get(store_prod_id).getOrElse("missingMapping")

          inputCMSCounter += 1
          ((store_id, newID, store_prod_price, store_prod_title, store_prod_url), (store_id, store_prod_id))
        }.filter {case (newRow, oldKey) =>
            if (validIDsBC.value.contains(oldKey._2)) {
              validCMSCounter += 1
              false
            } else true
        }.cache
      CMS.filter {
        case (newRow, oldKey) =>
          if (newRow._2.equals("missingMapping")) {
            missingMappingCMSCounter += 1
            true
          } else
            false
      }.saveAsTextFile(path2Mapping.substring(0, path2Mapping.lastIndexOf("/") + 1) + "CMS_missingMapping.txt")
      CMS.filter { case (newRow, oldKey) => !newRow._2.equals("missingMapping") }
        .map { t =>
          newCMSCounter += 1
          t._1
        }.saveToCassandra(keySpace, tableCMS, SomeColumns("store_id", "store_prod_id", "store_prod_price", "store_prod_title", "store_prod_url"))
      CMS.filter { case (newRow, oldKey) => !newRow._2.equals("missingMapping") }.foreachPartition { part =>
        val session = connector.openSession()
        part.foreach {
          case (newRow, oldKey) =>
            val delete = s"DELETE FROM " + keySpace + "." + tableCMS + " where     store_prod_id='" + oldKey._2 + "' and store_id='" + oldKey._1 + "';"
            session.execute(delete)
            deletedCMSCounter += 1
        }
        session.close()
      }
      CMS.unpersist(false)

      println("!@!@!@!@!        RT table           !@!@!@!@!" +
        "\n!@!@!@!@!   inputRTCounter : " + inputRTCounter.value +
        "\n!@!@!@!@!   validRTCounter : " + validRTCounter.value +
        "\n!@!@!@!@!   newRTCounter : " + newRTCounter.value +
        "\n!@!@!@!@!   deletedRTCounter : " + deletedRTCounter.value +
        "\n!@!@!@!@!   missingMappingRTCounter : " + missingMappingRTCounter.value +
        "\n!@!@!@!@!!@!@!@!@!!@!@!@!@!!@!@!@!@!!@!@!@!@!!@!@!@!@!")
      println("!@!@!@!@!        HP table           !@!@!@!@!" +
        "\n!@!@!@!@!   inputHPCounter : " + inputHPCounter.value +
        "\n!@!@!@!@!   validHPCounter : " + validHPCounter.value +
        "\n!@!@!@!@!   newHPCounter : " + newHPCounter.value +
        "\n!@!@!@!@!   deletedHPCounter : " + deletedHPCounter.value +
        "\n!@!@!@!@!   missingMappingHPCounter : " + missingMappingHPCounter.value +
        "\n!@!@!@!@!!@!@!@!@!!@!@!@!@!!@!@!@!@!!@!@!@!@!!@!@!@!@!")
      println("!@!@!@!@!        CMS table           !@!@!@!@!" +
        "\n!@!@!@!@!   inputCMSCounter : " + inputCMSCounter.value +
        "\n!@!@!@!@!   validCMSCounter : " + validCMSCounter.value +
        "\n!@!@!@!@!   newCMSCounter : " + newCMSCounter.value +
        "\n!@!@!@!@!   deletedCMSCounter : " + deletedCMSCounter.value +
        "\n!@!@!@!@!   missingMappingCMSCounter : " + missingMappingCMSCounter.value +
        "\n!@!@!@!@!!@!@!@!@!!@!@!@!@!!@!@!@!@!!@!@!@!@!!@!@!@!@!")

    } catch {
      case e: Exception => {
        println("########  Somthing went wrong :( ")
        println("#?#?#?#?#?#?#  ExceptionMessage : " + e.getMessage +
          "\n#?#?#?#?#?#?#  ExceptionStackTrace : " + e.getStackTraceString)
      }
    }

  }
}