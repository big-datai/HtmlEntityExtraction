package um.re.analytics

import java.util.Calendar

import com.datastax.spark.connector._
import com.utils.aws.AWSUtils
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.util.StatCounter
import um.re.utils.Utils

object ProdMetricsV5 {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName(getClass.getSimpleName)
    var (cassandraHost, keySpace, tableHP, tableRT, tableRTM, tableHSM, daysBack, hotLevel, numParts) = ("", "", "", "", "", "", "", "", "")
    if (args.size == 9) {
      cassandraHost = args(0)
      keySpace = args(1)
      tableHP = args(2)
      tableRT = args(3)
      tableRTM = args(4)
      tableHSM = args(5)
      daysBack = args(6)
      hotLevel = args(7)
      numParts = args(8)
    } else {
      cassandraHost = "107.20.157.48"
      keySpace = "demo"
      tableHP = "historical_prices"
      tableRT = "real_time_market_prices"
      tableRTM = "stream_hotspots_prod_metrics"
      tableHSM = "batch_hotspots_prod_metrics"
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

    conf.set("spark.cassandra.connection.host", cassandraHost)
    val sc = new SparkContext(conf)

    try {

      //########################################Real Time Metrics################################################
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
        val store_id = row.get[String]("store_id").replace(" ", "")
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
          if (iter.count(_ => true) > 1) {
            val previousPrice = sortedList.tail.head._2._4

            var prevPrices = sortedList.tail
            while (currentPrice == prevPrices.head._2._4 && prevPrices.tail != Nil) {
              prevPrices = prevPrices.tail
            }
            val lastChange = if (((currentPrice - prevPrices.head._2._4).toDouble / prevPrices.head._2._4).isNaN ||
              ((currentPrice - prevPrices.head._2._4).toDouble / prevPrices.head._2._4).isInfinity) 0.0
            else ((currentPrice - prevPrices.head._2._4).toDouble / prevPrices.head._2._4) * 100
            val lastPrice = currentPrice
            val prvPrice = prevPrices.head._2._4
            val delta = currentPrice - previousPrice
            val relativeChange = if ((delta / previousPrice).isNaN || (delta / previousPrice).isInfinity) 0.0
            else (delta / previousPrice) * 100
            (sys_prod_id, (store_id, sys_prod_id, tmsp, price, sys_prod_title, delta, relativeChange, lastChange, lastPrice, prvPrice))
          } else {
            (sys_prod_id, (store_id, sys_prod_id, tmsp, price, sys_prod_title, 0.0, 0.0, 0.0, 0.0, 0.0))
          }
      }

      val deltaData = deltas.groupByKey(partitioner).flatMap {
        case (sys_prod_id, iter) =>
          val sourceList = iter.toList

          val sortedByRelativeChange = sourceList.map {
            case (store_id, sys_prod_id, tmsp, price, sys_prod_title, delta, relativeChange, lastChange, lastPrice, prvPrice) =>
              (relativeChange, (store_id, sys_prod_id, tmsp, price, sys_prod_title, delta, relativeChange))
          }.sorted

          val (maxIncrease, maxIncreaseTo, maxIncreaseFrom, maxIncStoreId, maxIncProdId) = {
            val increases = sortedByRelativeChange.filter { l => (l._1 >= 0) && (!(l._1.isNaN() || l._1.isInfinite())) }
            if (increases.size > 0) {
              val (relativeChange, (store_id, sys_prod_id, tmsp, currentPrice, sys_prod_title, delta, relativeChang)) = increases.sorted.reverse.head
              if (relativeChange != 0.0) {
                (relativeChange, currentPrice, currentPrice - delta, store_id, sys_prod_id)
              } else {
                (0.0, 0.0, 0.0, "<>", "<>")
              }
            } else (0.0, 0.0, 0.0, "<>", "<>")
          }

          val (maxDecrease, maxDecreaseTo, maxDecreaseFrom, maxDecStoreId, maxDecProdId) = {
            val decreases = sortedByRelativeChange.filter { l => (l._1 <= 0) && (!(l._1.isNaN() || l._1.isInfinite())) }
            if (decreases.size > 0) {
              val (relativeChange, (store_id, sys_prod_id, tmsp, currentPrice, sys_prod_title, delta, relativeChang)) = decreases.sorted.head
              if (relativeChange != 0.0) {
                (relativeChange, currentPrice, currentPrice - delta, store_id, sys_prod_id)
              } else {
                (0.0, 0.0, 0.0, "<>", "<>")
              }
            } else (0.0, 0.0, 0.0, "<>", "<>")
          }

          val sortedByLastChange = sourceList.map {
            case (store_id, sys_prod_id, tmsp, price, sys_prod_title, delta, relativeChange, lastChange, lastPrice, prvPrice) =>
              (lastChange, (store_id, sys_prod_id, tmsp, sys_prod_title, lastChange, lastPrice, prvPrice))
          }

          val (maxIncLChange, maxIncLChangeTo, maxIncLChangeFrom, maxIncLChangeStoreId, maxIncLChangeProdId) = {
            val increasesLChange = sortedByLastChange.filter { l => (l._1 > 0) && (!(l._1.isNaN() || l._1.isInfinite())) }
            if (increasesLChange.size > 0) {
              val forIncrease = increasesLChange.map { case (lastChange, (store_id, sys_prod_id, tmsp, sys_prod_title, lastChang, lastPrice, prvPrice)) =>
                val tm = tmsp
                tm.setHours(0)
                tm.setMinutes(0)
                tm.setSeconds(0)
                (tm, (store_id, sys_prod_id, tm, sys_prod_title, lastChang, lastPrice, prvPrice))
              }.sorted.reverse.head._1

              println(forIncrease)

              val increasesLChangeRevised = increasesLChange.map { case (lastChange, (store_id, sys_prod_id, tmsp, sys_prod_title, lastChang, lastPrice, prvPrice)) =>
                val tm = tmsp
                tm.setHours(0)
                tm.setMinutes(0)
                tm.setSeconds(0)
                (lastChange, (store_id, sys_prod_id, tm, sys_prod_title, lastChang, lastPrice, prvPrice))
              }


              val TopIncreasesLChange = increasesLChangeRevised.filter(l => l._2._3 == forIncrease).map { case (lastChange, (store_id, sys_prod_id, tm, sys_prod_title, lastChang, lastPrice, prvPrice)) =>
                (lastChange, (store_id, sys_prod_id, sys_prod_title, lastChang, lastPrice, prvPrice))
              }
              val (lastChange, (store_id, sys_prod_id, sys_prod_title, lastChang, lastPrice, prvPrice)) = TopIncreasesLChange.sorted.reverse.head
              if (lastChange != 0.0) {
                (lastChange, lastPrice, prvPrice, store_id, sys_prod_id)
              } else {
                (0.0, 0.0, 0.0, "<>", "<>")
              }
            } else (0.0, 0.0, 0.0, "<>", "<>")
          }

          val (maxDecLChange, maxDecLChangeTo, maxDecLChangeFrom, maxDecLChangeStoreId, maxDecLChangeProdId) = {
            val decreasesLChange = sortedByLastChange.filter { l => (l._1 < 0) && (!(l._1.isNaN() || l._1.isInfinite())) }
            if (decreasesLChange.size > 0) {
              val forDecrease = decreasesLChange.map { case (lastChange, (store_id, sys_prod_id, tmsp, sys_prod_title, lastChang, lastPrice, prvPrice)) =>
                val tm = tmsp
                tm.setHours(0)
                tm.setMinutes(0)
                tm.setSeconds(0)
                (tm, (store_id, sys_prod_id, tm, sys_prod_title, lastChang, lastPrice, prvPrice))
              }.sorted.reverse.head._1
              println(forDecrease)

              val decreasesLChangeRevised = decreasesLChange.map { case (lastChange, (store_id, sys_prod_id, tmsp, sys_prod_title, lastChang, lastPrice, prvPrice)) =>
                val tm = tmsp
                tm.setHours(0)
                tm.setMinutes(0)
                tm.setSeconds(0)
                (lastChange, (store_id, sys_prod_id, tm, sys_prod_title, lastChang, lastPrice, prvPrice))
              }
              val TopDecreaseLChange = decreasesLChangeRevised.filter(l => l._2._3 == forDecrease).map { case (lastChange, (store_id, sys_prod_id, tm, sys_prod_title, lastChang, lastPrice, prvPrice)) =>
                (lastChange, (store_id, sys_prod_id, sys_prod_title, lastChang, lastPrice, prvPrice))
              }

              val (lastChange, (store_id, sys_prod_id, sys_prod_title, lastChang, lastPrice, prvPrice)) = TopDecreaseLChange.sorted.head
              if (lastChange != 0.0) {
                (lastChange, lastPrice, prvPrice, store_id, sys_prod_id)
              } else {
                (0.0, 0.0, 0.0, "<>", "<>")
              }
            } else (0.0, 0.0, 0.0, "<>", "<>")
          }

          val stores = sourceList.map {
            case ((store_id, sys_prod_id, tmsp, price, sys_prod_title, delta, relativeChange, lastChange, lastPrice, prvPrice)) =>
              (store_id, sys_prod_title)
          }.toList.sorted
          val results = stores.map {
            case (store_id, sys_prod_title) =>
              (sys_prod_id, store_id.replace(" ", ""), sys_prod_title,
                maxIncrease, maxIncreaseTo, maxIncreaseFrom, maxIncStoreId,
                maxDecrease, maxDecreaseTo, maxDecreaseFrom, maxDecStoreId,
                maxIncLChange, maxIncLChangeTo, maxIncLChangeFrom, maxIncLChangeStoreId,
                maxDecLChange, maxDecLChangeTo, maxDecLChangeFrom, maxDecLChangeStoreId)
          }
          hpMetricsCounter += results.size
          results
      }

      //#############################Saving to Cassandra#################################################################
      deltaData.saveToCassandra(keySpace, tableRTM, SomeColumns("sys_prod_id", "store_id", "sys_prod_title", "max_increase", "max_increase_to", "max_increase_from", "max_inc_store_id",
        "max_decrease", "max_decrease_to", "max_decrease_from", "max_dec_store_id", "max_last_inc", "max_last_inc_to", "max_last_inc_from", "max_last_inc_store_id",
        "max_last_dec", "max_last_dec_to", "max_last_dec_from", "max_last_dec_store_id"))

      //#######################################HotSpot Metrics####################################################
      val RtData = sc.cassandraTable(keySpace, tableRT).map { row =>
        val sys_prod_id = row.get[String]("sys_prod_id")
        val store_id = row.get[String]("store_id")
        val sys_prod_title = row.get[String]("sys_prod_title")
        val price = row.get[Double]("price")
        val url = row.get[String]("url")
        val hot = row.get[Option[String]]("hot_level")
        (sys_prod_id, (store_id, price, url, sys_prod_title, hot))
      }
      val FilteredRtData = RtData.filter { case (sys_prod_id, (store_id, price, url, sys_prod_title, hot)) => hot.isDefined }
      val hotLevelSet = hotLevel.split(",").toSet
      val FinalRtData = FilteredRtData.filter { case (sys_prod_id, (store_id, price, url, sys_prod_title, hot)) => hotLevelSet.contains(hot.get) }

      val varPosData = FinalRtData.groupByKey(partitioner).flatMap {
        case (sys_prod_id, iter) =>
          var cnt = 0
          val NewTuple = iter.map {
            case (store_id, price, url, sys_prod_title, hot) => (price, (store_id, url, sys_prod_title, hot))
          }.toList.sorted.map {
            case (price, (store_id, url, sys_prod_title, hot)) =>
              cnt += 1
              (sys_prod_id, price, store_id, url, sys_prod_title, hot, cnt)
          }
          val sze = NewTuple.size
          val priceList = NewTuple.map { case (sys_prod_id, price, store_id, url, sys_prod_title, hot, cnt) => price }
          val std = Math.sqrt(StatCounter(priceList).variance).toDouble
          val meanPrice = StatCounter(priceList).mean
          val maxPrice = priceList.max
          val minPrice = priceList.min
          val priceDelta = (maxPrice - minPrice).toDouble / minPrice

          val FinalTuples = NewTuple.map {
            case (sys_prod_id, price, store_id, url, sys_prod_title, hot, cnt) =>
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
                ((store_id.replace(" ", ""), sys_prod_id), (price, url, sys_prod_title, hot, cnt, relPlace * 100, relPlaceRank, cv, cvRank, meanPrice, minPrice, maxPrice, priceDelta, sze))
              else
                ((store_id.replace(" ", ""), sys_prod_id), (price, url, sys_prod_title, hot, cnt, relPlace * 100, relPlaceRank, 0.0, 1, meanPrice, minPrice, maxPrice, 0.0, sze))

          }
          if (priceDelta < 0.3 && sze > 1)
            rtMetricsCounter += FinalTuples.size
          FinalTuples
      }

      val varPosDataFinal = varPosData.filter(l => l._2._13 < 0.3 && l._2._14 > 1).map {
        case ((store_id, sys_prod_id), (price, url, sys_prod_title, hot, cnt, relPlace, relPlaceRank, cv, cvRank, meanPrice, minPrice, maxPrice, priceDelta, sze)) =>
          ((sys_prod_id, store_id, price, url, sys_prod_title, hot, cnt, relPlace, relPlaceRank, cv, cvRank, meanPrice, minPrice, maxPrice, sze))
      }

      //#############################Saving to Cassandra#################################################################
      varPosDataFinal.saveToCassandra(keySpace, tableHSM, SomeColumns("sys_prod_id", "store_id", "price", "url", "sys_prod_title", "hot_level", "abs_position_in_market", "relative_place", "relative_place_rank", "cv", "cv_rank", "mean_price", "min_price", "max_price", "num_competitors"))


      println("!@!@!@!@!   hpMetricsCounter : " + hpMetricsCounter.value +
        "\n!@!@!@!@!   rtMetricsCounter : " + rtMetricsCounter.value
      )
    } catch {
      case e: Exception => {
        println("########  Somthing went wrong :( ")
        println("#?#?#?#?#?#?#  ExceptionMessage : " + e.getMessage +
          "\n#?#?#?#?#?#?#  ExceptionStackTrace : " + e.getStackTraceString)
      }
    }
  }
}

/*
CREATE TABLE demo.batch_hotspots_prod_metrics (
    sys_prod_id text,
    store_id text,
    price double,
    url text,
    sys_prod_title text,
    hot_level text,
    num_competitors int,
    abs_position_in_market int,
    relative_place double,
    relative_place_rank int,
    cv double,
    cv_rank int,
    mean_price double,
    min_price double,
    max_price double,
    PRIMARY KEY (sys_prod_id, store_id)
) WITH CLUSTERING ORDER BY (store_id ASC);
    
 */

/*
CREATE TABLE demo.stream_hotspots_prod_metrics (
    sys_prod_id text,
    store_id text,
    sys_prod_title text,
    max_increase double,
    max_increase_from double,
    max_increase_to double,
    max_inc_store_id text,
    max_decrease double,
    max_decrease_from double,
    max_decrease_to double,
    max_dec_store_id text,
    max_last_inc double,
    max_last_inc_to double,
    max_last_inc_from double,
    max_last_inc_store_id text,
    max_last_dec double,
    max_last_dec_to double,
    max_last_dec_from double,
    max_last_dec_store_id text,
    PRIMARY KEY (sys_prod_id, store_id)
) WITH CLUSTERING ORDER BY (store_id ASC); 

 */