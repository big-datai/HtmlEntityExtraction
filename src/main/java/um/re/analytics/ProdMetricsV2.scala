package um.re.analytics

import java.sql.DriverManager
import java.util.Calendar

import com.datastax.spark.connector._
import com.utils.aws.AWSUtils
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.util.StatCounter
import um.re.utils.Utils

object ProdMetricsV2 {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
    var (cassandraHost, keySpace, tableHP, tableRT, mysqlHost, mysqlPort, mysqlDB, mysqlUser, mysqlPass, tablePM, daysBack, hotLevel, numParts) = ("", "", "", "", "", "", "", "", "", "", "", "", "")
    if (args.size == 13) {
      cassandraHost = args(0)
      keySpace = args(1)
      tableHP = args(2)
      tableRT = args(3)
      mysqlHost = args(4)
      mysqlPort = args(5)
      mysqlDB = args(6)
      mysqlUser = args(7)
      mysqlPass = args(8)
      tablePM = args(9)
      daysBack = args(10)
      hotLevel = args(11)
      numParts = args(12)
    } else {
      cassandraHost = "10.167.94.105"
      keySpace = "demo"
      tableHP = "historical_prices"
      tableRT = "real_time_market_prices"
      mysqlHost = "10.167.94.105"
      mysqlPort = "3306"
      mysqlDB = "demo"
      mysqlUser = "core_user"
      mysqlPass = "papoogay"
      tablePM = "prod_metrics"
      daysBack = "-1"
      hotLevel = "1,2"
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

    try {
      val innerMysqlHost = AWSUtils.getPrivateIp(mysqlHost)
      mysqlHost = innerMysqlHost
    } catch {
      case e: Exception => {
        println("#?#?#?#?#?#?#  Couldn't get inner mysql IP, using : " + mysqlHost +
          "\n#?#?#?#?#?#?#  ExceptionMessage : " + e.getMessage +
          "\n#?#?#?#?#?#?#  ExceptionStackTrace : " + e.getStackTraceString)
      }
    }

    conf.set("spark.cassandra.connection.host", cassandraHost)
    val sc = new SparkContext(conf)

    try {
      val today = Calendar.getInstance().getTime()
      val yesterday = Utils.getDateFromToday(daysBack.toInt)
      //counters and accumulators
      val hpMetricsCounter = sc.accumulator(0L)
      val rtMetricsCounter = sc.accumulator(0L)
      val joinedMetricsCounter = sc.accumulator(0L)
      val successfulWritesCounter = sc.accumulator(0L)
      val failedWritesCounter = sc.accumulator(0L)

      val partitioner = new HashPartitioner(numParts.toInt)

      val hp = sc.cassandraTable(keySpace, tableHP).where("tmsp > ?", yesterday.getTime).map { row =>
        val store_id = row.get[String]("store_id")
        val sys_prod_id = row.get[String]("sys_prod_id")
        val tmsp = row.get[java.util.Date]("tmsp")
        val price = row.get[Double]("price")
        val sys_prod_title = row.get[String]("sys_prod_title")
        ((sys_prod_id, store_id), (store_id, sys_prod_id, tmsp, price, sys_prod_title))
      }
      val deltas = hp.groupByKey(partitioner).map {
        case ((sys_prod_id, store_id), iter) =>
          val sortedList = iter.toList.map {
            case (store_id, sys_prod_id, tmsp, price, sys_prod_title) =>
              (tmsp, (store_id, sys_prod_id, tmsp, price, sys_prod_title))
          }.sorted.reverse
          val (store_id, sys_prod_id, tmsp, price, sys_prod_title) = sortedList.head._2
          val currentPrice = price


          /* A piece of code for calculating last change
          if (iter.count(_ => true) > 1) {
            val previousPrice = sortedList.tail.head._2._4
            //////
            var lastPrice=price
            var prevPrices =  sortedList.tail//.head._2._4
            while (lastPrice==prevPrices.head._2._4 && prevPrices.tail!=Nil){
               lastPrice =  prevPrices.head._2._4
               prevPrices = prevPrices.tail
             }
           val lastChange = if (((lastPrice-prevPrices.head._2._4).toDouble/prevPrices.head._2._4).isNaN ||
                             ((lastPrice-prevPrices.head._2._4).toDouble/prevPrices.head._2._4).isInfinity) 0.0
                            else ((lastPrice-prevPrices.head._2._4).toDouble/prevPrices.head._2._4)*100
               
            val delta = currentPrice - previousPrice
            val relativeChange = if ((delta / previousPrice).isNaN || (delta / previousPrice).isInfinity) 0.0
            else (delta / previousPrice) * 100
            (sys_prod_id, (store_id, sys_prod_id, tmsp, price, sys_prod_title, delta, relativeChange,lastChange))
          } else {
            (sys_prod_id, (store_id, sys_prod_id, tmsp, price, sys_prod_title, 0.0, 0.0,0.0))
          }                
            */

          if (iter.count(_ => true) > 1) {
            val previousPrice = sortedList.tail.head._2._4
            val delta = currentPrice - previousPrice
            val relativeChange = if ((delta / previousPrice).isNaN || (delta / previousPrice).isInfinity) 0.0
            else (delta / previousPrice) * 100
            (sys_prod_id, (store_id, sys_prod_id, tmsp, price, sys_prod_title, delta, relativeChange))
          } else {
            (sys_prod_id, (store_id, sys_prod_id, tmsp, price, sys_prod_title, 0.0, 0.0))
          }
      }

      val deltaData = deltas.groupByKey(partitioner).flatMap {
        case (sys_prod_id, iter) =>
          val sourceList = iter.toList
          val sortedByRelativeChange = sourceList.map {
            case (store_id, sys_prod_id, tmsp, price, sys_prod_title, delta, relativeChange) =>
              (relativeChange, (store_id, sys_prod_id, tmsp, price, sys_prod_title, delta, relativeChange))
          }.sorted
          val (maxIncrease, maxIncreaseTo, maxIncreaseFrom, maxIncStoreId, maxIncProdId) = {
            val increases = sortedByRelativeChange.filter { l => (l._1 >= 0) && (!(l._1.isNaN() || l._1.isInfinite())) }
            if (increases.size > 0) {
              val (relativeChange, (store_id, sys_prod_id, tmsp, currentPrice, sys_prod_title, delta, relativeChang)) = increases.sorted.reverse.head
              (relativeChange, currentPrice, currentPrice - delta, store_id, sys_prod_id)
            } else (0.0, 0.0, 0.0, "", "")
          }
          val (maxDecrease, maxDecreaseTo, maxDecreaseFrom, maxDecStoreId, maxDecProdId) = {
            val decreases = sortedByRelativeChange.filter { l => (l._1 <= 0) && (!(l._1.isNaN() || l._1.isInfinite())) }
            if (decreases.size > 0) {
              val (relativeChange, (store_id, sys_prod_id, tmsp, currentPrice, sys_prod_title, delta, relativeChang)) = decreases.sorted.head
              (relativeChange, currentPrice, currentPrice - delta, store_id, sys_prod_id)
            } else (0.0, 0.0, 0.0, "", "")
          }
          //addition


          val incDec = (maxIncrease, maxIncreaseTo, maxIncreaseFrom, maxIncStoreId, maxIncProdId, maxDecrease, maxDecreaseTo, maxDecreaseFrom, maxDecStoreId, maxDecProdId)
          val stores = sourceList.map {
            case ((store_id, sys_prod_id, tmsp, price, sys_prod_title, delta, relativeChange)) =>
              (store_id, sys_prod_title)
          }.toList.sorted
          val results = stores.map {
            case (store_id, sys_prod_title) =>
              ((store_id.replace(" ", ""), sys_prod_id), (sys_prod_title, maxIncrease, maxIncreaseTo, maxIncreaseFrom, maxIncStoreId, maxIncProdId, maxDecrease, maxDecreaseTo, maxDecreaseFrom, maxDecStoreId, maxDecProdId))
          }
          hpMetricsCounter += results.size
          results
      }

      val RtData = sc.cassandraTable(keySpace, tableRT).map { row =>
        val sys_prod_id = row.get[String]("sys_prod_id")
        val store_id = row.get[String]("store_id")
        val price = row.get[Double]("price")
        val url = row.get[String]("url")
        val hot = row.get[Option[String]]("hot_level")
        (sys_prod_id, (store_id, price, url, hot))
      }
      val FilteredRtData = RtData.filter { case (sys_prod_id, (store_id, price, url, hot)) => hot.isDefined }
      val hotLevelSet = hotLevel.split(",").toSet
      val FinalRtData = FilteredRtData.filter { case (sys_prod_id, (store_id, price, url, hot)) => hotLevelSet.contains(hot.get) }

      val varPosData = FinalRtData.groupByKey(partitioner).flatMap {
        case (sys_prod_id, iter) =>
          var cnt = 0
          val NewTuple = iter.map {
            case (store_id, price, url, hot) => (price, (store_id, url, hot))
          }.toList.sorted.map {
            case (price, (store_id, url, hot)) =>
              cnt += 1
              (sys_prod_id, price, store_id, url, hot, cnt)
          }
          val sze = NewTuple.size
          val priceList = NewTuple.map { case (sys_prod_id, price, store_id, url, hot, cnt) => price }
          val std = Math.sqrt(StatCounter(priceList).variance).toDouble
          val meanPrice = StatCounter(priceList).mean
          val maxPrice = priceList.max
          val minPrice = priceList.min
          val priceDelta = (maxPrice - minPrice).toDouble / minPrice

          val FinalTuples = NewTuple.map {
            case (sys_prod_id, price, store_id, url, hot, cnt) =>
              val relPlace = (cnt.toDouble / sze)
              val cv = (std.toDouble / meanPrice)
              val cvRank = {
                if (cv >= 0 && cv <= 0.2) 1
                else if (cv > 0.2 && cv <= 0.4) 2
                else if (cv > 0.4 && cv <= 0.6) 3
                else if (cv > 0.6 && cv <= 0.85) 4
                else 5
              }
              val relPlaceRank = {
                if (relPlace >= 0 && relPlace <= 0.05) 5
                else if (relPlace > 0.05 && relPlace <= 0.1) 10
                else if (relPlace > 0.1 && relPlace <= 0.2) 20
                else if (relPlace > 0.2 && relPlace <= 0.3) 30
                else if (relPlace > 0.3 && relPlace <= 0.4) 40
                else if (relPlace > 0.4 && relPlace <= 0.5) 50
                else if (relPlace > 0.5 && relPlace <= 0.6) 60
                else if (relPlace > 0.6 && relPlace <= 0.7) 70
                else if (relPlace > 0.7 && relPlace <= 0.8) 80
                else if (relPlace > 0.8 && relPlace <= 0.9) 90
                else if (relPlace > 0.9 && relPlace <= 0.95) 95
                else 100
              }
              if (meanPrice > 0.0)
                ((store_id.replace(" ", ""), sys_prod_id), (price, url, hot, cnt, relPlace * 100, relPlaceRank, cv, cvRank, meanPrice, minPrice, maxPrice, priceDelta, sze))
              else
                ((store_id.replace(" ", ""), sys_prod_id), (price, url, hot, cnt, relPlace * 100, relPlaceRank, 0.0, 1, meanPrice, minPrice, maxPrice, 0.0, sze))

          }
          if (priceDelta < 0.3 && sze > 1)
            rtMetricsCounter += FinalTuples.size
          FinalTuples
      }

      // val varPosDataFinal = varPosData.filter(l=> l._2._12 < 0.3 && l._2._13 > 1).map{
      val varPosDataFinal = varPosData.filter(l => l._2._13 > 1).map {
        case ((store_id, sys_prod_id), (price, url, hot, cnt, relPlace, relPlaceRank, cv, cvRank, meanPrice, minPrice, maxPrice, priceDelta, sze)) =>
          ((store_id, sys_prod_id), (price, url, hot, cnt, relPlace, relPlaceRank, cv, cvRank, meanPrice, minPrice, maxPrice))
      }
      val t = (deltaData.join(varPosDataFinal)).map {
        case ((store_id, sys_prod_id), ((sys_prod_title, maxIncrease, maxIncreaseTo, maxIncreaseFrom, maxIncStoreId, maxIncProdId, maxDecrease, maxDecreaseTo, maxDecreaseFrom, maxDecStoreId, maxDecProdId),
        (price, url, hot_level, abs_position, relative_position, position_level, var_val, var_level, meanPrice, minPrice, maxPrice))) =>
          joinedMetricsCounter += 1
          ((store_id, sys_prod_id), ((sys_prod_title, maxIncrease, maxIncreaseTo, maxIncreaseFrom, maxIncStoreId, maxIncProdId, maxDecrease, maxDecreaseTo, maxDecreaseFrom, maxDecStoreId, maxDecProdId),
            (price, url, hot_level, abs_position, relative_position, position_level, var_val, var_level,
              meanPrice, minPrice, maxPrice)), today)
      }
      t.foreachPartition { it =>
        Class.forName("com.mysql.jdbc.Driver").newInstance
        val conn = DriverManager.getConnection("jdbc:mysql://" + mysqlHost + ":" + mysqlPort + "/",
          mysqlUser,
          mysqlPass)
        val del = conn.prepareStatement("INSERT INTO " + mysqlDB + "." + tablePM +
          " (store_id,hot_level,var_level,position_level,tmsp,sys_prod_id,abs_position,price," +
          "relative_position,sys_prod_title,url,var_val,mean_price,min_price,max_price," +
          "max_increase,max_increase_from_to,max_inc_merged_id," +
          "max_decrease,max_decrease_from_to,max_dec_merged_id) " +
          "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CONCAT(?,' -> ',?),CONCAT(?,'|||',?),?,CONCAT(?,' -> ',?),CONCAT(?,'|||',?))" +
          " ON DUPLICATE KEY UPDATE hot_level = values(hot_level),var_level= values(var_level),position_level= values(position_level)," +
          "tmsp= values(tmsp),abs_position= values(abs_position),price = values(price),relative_position= values(relative_position)," +
          "var_val = values(var_val),mean_price= values(mean_price),min_price= values(min_price),max_price= values(max_price)," +
          "max_increase= values(max_increase),max_increase_from_to= values(max_increase_from_to),max_inc_merged_id= values(max_inc_merged_id)," +
          "max_decrease= values(max_decrease),max_decrease_from_to= values(max_decrease_from_to),max_dec_merged_id= values(max_dec_merged_id)")
        for (tuple <- it) {
          try {
            val (store_id, sys_prod_id) = tuple._1
            val (sys_prod_title, max_increase, max_increase_to, max_increase_from, max_inc_store_id, max_inc_prod_id, max_decrease, max_decrease_to, max_decrease_from, max_dec_store_id, max_dec_prod_id) = tuple._2._1
            val (price, url, hot_level, abs_position, relative_position, position_level, var_val, var_level, mean_price, min_price, max_price) = tuple._2._2
            val tmsp = tuple._3
            del.setString(1, store_id)
            del.setInt(2, hot_level.get.toInt)
            del.setInt(3, var_level)
            del.setInt(4, position_level)
            del.setDate(5, new java.sql.Date(tmsp.getTime))
            del.setString(6, sys_prod_id)
            del.setInt(7, abs_position)
            del.setDouble(8, price)
            del.setDouble(9, relative_position)
            del.setString(10, sys_prod_title)
            del.setString(11, url)
            del.setDouble(12, var_val)
            del.setDouble(13, mean_price)
            del.setDouble(14, min_price)
            del.setDouble(15, max_price)
            del.setDouble(16, max_increase)
            del.setDouble(17, max_increase_from)
            del.setDouble(18, max_increase_to)
            del.setString(19, max_inc_store_id)
            del.setString(20, max_inc_prod_id)
            del.setDouble(21, max_decrease)
            del.setDouble(22, max_decrease_from)
            del.setDouble(23, max_decrease_to)
            del.setString(24, max_dec_store_id)
            del.setString(25, max_dec_prod_id)

            del.executeUpdate
            successfulWritesCounter += 1
          } catch {
            case e: Exception => {
              failedWritesCounter += 1
              println("########  Somthing went wrong :( ")
              println("#?#?#?#?#?#?#  ExceptionMessage : " + e.getMessage +
                "\n#?#?#?#?#?#?#  ExceptionStackTrace : " + e.getStackTraceString)
              println(tuple)
            }
          }
        }
        conn.close()
      }
      println("!@!@!@!@!   hpMetricsCounter : " + hpMetricsCounter.value +
        "\n!@!@!@!@!   rtMetricsCounter : " + rtMetricsCounter.value +
        "\n!@!@!@!@!   joinedMetricsCounter : " + joinedMetricsCounter.value +
        "\n!@!@!@!@!   successfulWritesCounter : " + successfulWritesCounter.value +
        "\n!@!@!@!@!   failedWritesCounter : " + failedWritesCounter.value)
    } catch {
      case e: Exception => {
        println("########  Somthing went wrong :( ")
        println("#?#?#?#?#?#?#  ExceptionMessage : " + e.getMessage +
          "\n#?#?#?#?#?#?#  ExceptionStackTrace : " + e.getStackTraceString)
      }
    }
  }
}