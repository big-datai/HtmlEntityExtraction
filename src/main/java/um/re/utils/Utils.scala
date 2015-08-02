package um.re.utils
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.collection.JavaConversions._
import play.api.libs.json._
import java.util.regex.Pattern
import scala.util.control.Exception
import java.net.URI
import java.io.PrintWriter
import java.io.File
import com.gargoylesoftware.htmlunit.WebClient
import com.gargoylesoftware.htmlunit.html.HtmlPage
import com.gargoylesoftware.htmlunit.WebResponseData
import org.apache.spark.mllib.tree.GradientBoostedTrees
import com.utils.messages.MEnrichMessage
import org.apache.spark.streaming.dstream.DStream
import java.util.Properties
import kafka.producer.ProducerConfig
import kafka.producer.Producer
import kafka.producer.KeyedMessage
object Utils {
  val S3STORAGE = "s3:/"
  val HDFSSTORAGE = "hdfs://"
  val DCANDIDS = "/rawd/objects/dcandids/"
  val DMODELS = "/rawd/objects/dmodels/"
  val DSCORES = "/rawd/objects/dscores/"
  val MODELS = "/dpavlov/models/Models/"
  val DMAP = "/dpavlov/models/dmap/part-00000"
  val ANALDATA = "/analysis/data/"
  val SEEDS2S3 = "/dpavlov/seeds20150709"
  val FULLR2S3 = "/dpavlov/es/full_river"
  val DEBUGFLAG = false

  def getDomain(input: String) = {
    var url = input
    try {
      if (url.startsWith("http:/")) {
        if (!url.contains("http://")) {
          url = url.replaceAll("http:/", "http://")
        }
      } else {
        url = "http://" + url
      }
      var uri: URI = new URI(url)
      var domain = uri.getHost();
      if (domain.startsWith("www.")) domain.substring(4) else domain
    } catch { case _: Exception => "www.failed.com" }
  }
  def parseDouble(s: String, language: String = "en", country: String = "US"): Option[Double] = try {
    val locale = new java.util.Locale(language, country)
    val formatter = java.text.NumberFormat.getNumberInstance(locale)
    Some(formatter.parse(s).doubleValue())
  } catch { case _: Throwable => None }
  /**
   * This function splits data into n-grams strings
   */
  def gramsByN(data: String, number: Int): List[String] = {
    val chrData = data.toCharArray
    var i = 0
    var grams: List[String] = List()
    val lenght = chrData.length
    for (i <- 1 until lenght) {
      if (i + number < lenght) {
        val str = data.substring(i, i + number)
        grams = str :: grams
      }
    }
    grams
  }

  /**
   * this function replaces all characters and number with space and trip multiple spaces
   */
  def textOnly(text: String) = {
    text.replaceAll("[^A-Za-z]+", " ").replaceAll("[\\p{Blank}]{1,}?", " ")
  }
  def textNum(text: String) = {
    text.replaceAll("[^0-9A-Za-z]+", " ").replaceAll("[\\p{Blank}]{1,}?", " ")
  }
  /**
   * Tokenazer
   */
  def tokenazer(text: String) = {
    textOnly(text).split(" ").toSeq
  }
  def tokenazerTextNum(text: String) = {
    textNum(text).split(" ").toSeq
  }
  def bySpace(text: String) = {
    text.replaceAll("[\\p{Blank}]{1,}?", " ")
  }

  /**
   * Tokenazer by space
   */
  def tokenazerSpace(text: String) = {
    bySpace(text).split(" ").toSeq
  }

  def getTags(data: String): Seq[String] = {

    null
  }
  def map2JsonString(map: Map[String, String]) = {
    val asJson = Json.toJson(map)
    Json.stringify(asJson)
  }

  def json2Map(js: JsValue): Map[String, String] = {
    try {
      val map = js.as[Map[String, String]]
      map //Json.stringify(asJson)
    } catch {
      case e: Exception => {
        println("Could not parse jsontMap")

        try {
          println(js.toString())
        } catch {
          case _: Exception => {}
        }
        println("#?#?#?#?#?#?#  ExceptionLocalizedMessage : " + e.getLocalizedMessage +
          "\n#?#?#?#?#?#?#  ExceptionMessage : " + e.getMessage +
          "\n#?#?#?#?#?#?#  ExceptionStackTrace : " + e.getStackTraceString)
        null
      }
    }
  }
  /**
   * This function takes ES source and transforms it to format of R candidates
   */
  def getCandidates(source2: RDD[(String, Map[String, String])]) = {
    val candid = source2.map { l =>
      try {
        val nf = PriceParcer
        val id = l._2.get("url").toString
        val h = l._2.get("html").toString
        val res = nf.find(id, h)
        res
      } catch {
        case _: Exception => { "[{\"no\":\"data\"}]" }
      }
    }
    candid
  }
  /**
   * This method checks if candidate is true and
   */
  def isTrueCandid(map_pat: Map[String, String], cand: Map[String, String]): Boolean = {
    (map_pat.get("price") != None && map_pat.get("updatedPrice") != None && cand.get("priceCandidate") != None &&
      Utils.parseDouble(cand.get("priceCandidate").get.toString) != None && Utils.parseDouble(map_pat.get("updatedPrice").get.toString) != None &&
      Utils.parseDouble(map_pat.get("price").get.toString) != None &&
      Utils.parseDouble(cand.get("priceCandidate").get.toString).get == Utils.parseDouble((map_pat.get("price").get.toString)).get &&
      Utils.parseDouble(map_pat.get("updatedPrice").get.toString).get == Utils.parseDouble(map_pat.get("price").get.toString).get)
  }

  def htmlsToCandidsPipe(source: RDD[(Array[Byte], Map[String, String])]): RDD[(Array[Byte], List[Map[String, String]])] = {
    val res = Utils.getCandidatesPatternsHtmlTrimed(source)
    if (DEBUGFLAG)
      res.count

    /* this filter was relevant for learning phase
    val resFiltered = res.filter { case(msg,l) =>
      val map_pat = l.head
      val count = l.tail.filter { cand =>
        (Utils.isTrueCandid(map_pat, cand))
      }.size
      l != null && count > 0
    }
    if (DEBUGFLAG)
      resFiltered.count
    */
    val db = res /*Filtered*/ .map {
      case (msg, l) =>
        try {
          val map_pat = l.head
          val pat = map_pat.get("patterns").get.toString
          val html = map_pat.get("html").get.toString
          val length = html.size
          val location_pattern: Map[String, String] = Map.empty //Utils.allPatterns(pat, html, 150)
          //add to each candidate pattern
          (msg, l.tail.map { cand =>
            cand + ("updatedPrice" -> map_pat.get("updatedPrice").get.toString) + ("price" -> map_pat.get("price").get.toString) +
              ("patterns" -> Utils.map2JsonString(location_pattern)) + ("length" -> length.toString)
          })
        } catch {
          case _: Exception => null
        }
    }
    if (DEBUGFLAG)
      db.count
    val dbFiltered = db.filter(l => (l != null) && !l._2.isEmpty)
    if (DEBUGFLAG)
      dbFiltered.count

    dbFiltered
  }

  def getCandidatesPatternsHtmlTrimed(source2: RDD[(Array[Byte], Map[String, String])]): RDD[(Array[Byte], List[Map[String, String]])] = {
    val candid = source2.map {
      case (msg, l) =>
        try {
          val nf = PriceParcer
          nf.snippetSize = 150
          val id = l.get("url").get
          val price = l.get("price").get
          val updatedPrice = l.get("updatedPrice").get
          val html = shrinkString(l.get("html").get)
          /*
        val html_to=l.get("html").get
        val m_webClient = new WebClient()
        val p=m_webClient.getPage(id)
        */
          val patterns = shrinkString(l.get("patternsHtml").get)
          val res = nf.findFast(id, html)
          val p_h = Map("patterns" -> patterns, "html" -> html, "price" -> price, "updatedPrice" -> updatedPrice)
          (msg, p_h :: res)
        } catch {
          case _: Exception => (new Array[Byte](0), Nil)
        }
    }
    candid
  }
  /**
   * allPatterns method gets a string of "|||" separated patterns and returns an array of maps[location, ext_pattern]
   *
   */
  def allPatterns(patterns: String, html: String, new_length: Int) = {
    patterns.split("\\|\\|\\|").filter(p => p != null && !p.isEmpty() && !p.equals("")).map { p =>
      extPatternLocationPair(p, html, new_length)
    }.toMap
  }

  def string2Json(jsonString: String) = {
    Json.parse(jsonString)
  }
  /**
   * extPatternLocationPair this method returns a pair (pattern location, extended pattern)
   *
   */
  def extPatternLocationPair(pattern: String, html: String, new_length: Int) = {
    val size = html.length
    val res = Utils.skipSpecialCharsInPattern(pattern).r.findAllMatchIn(html).
      map { m =>
        println(m.start + " the end: " + m.end)
        // println(" html sub string :"+html.substring(math.max(m.start - length, 0)))
        println(html.substring(math.max(m.start(1) - new_length, 0), math.min(m.end(1) + new_length, size)))
        (m.start(1).toString, html.substring(math.max(m.start(1) - new_length, 0), math.min(m.end(1) + new_length, size)))
      }.toMap.head
    res
  }

  /**
   * This method take a pattern and hides special characters beside (.*?) so we can find the price in a pattern
   *
   */
  def skipSpecialCharsInPattern(price_pattern: String) = {
    val wildcard = "(.*?)"
    val wild_index = price_pattern.indexOf(wildcard)
    val text_before = price_pattern.substring(0, wild_index)
    val text_after = price_pattern.substring(wild_index + wildcard.length)
    "(?:" + Pattern.quote(text_before) + ")" + wildcard + "(?:" + Pattern.quote(text_after) + ")"

  }

  def hideSpecialChars(price_pattern: String) = {
    price_pattern.replaceAll("[\\p{Blank}]{1,}", " ").replaceAll("[\t\n\r,]", "").replaceAll("\\(", "\\\\(")
      .replaceAll("\\)", "\\\\)").replaceAll("\\[", "\\\\[").replaceAll("\\]", "\\\\]").replaceAll("\\$", "\\\\\\$")
      .replaceAll("\\.", "\\\\.").replaceAll("\\*", "\\\\*").replaceAll("\\?", "\\\\?").replaceAll("\\+", "\\\\+")
      .replace("\\(\\.\\*\\?\\)", "(.*?)")

  }
  /**
   * shrinkString replaces multiple tabs and spaces 3 and more, comma in numbers 1,000.00 =>1000.00 and new lines
   *
   */
  def shrinkString(str: String): String = {
    str.replaceAll("[\\p{Blank}]{3,}", " ").replaceAll("(?<=[\\d])(,)(?=[\\d])", "").replaceAll("[\t\n\r,]", "")
  }

  /**
   * this function removes more then 3 spaces from string
   */
  def threePlusTrim(str: String): String = {
    "\\p{Blank}{3,}+".r.replaceAllIn(str, " ");
  }

  /**
   * this function removes all spaces around string
   */
  def trimFromSides(str: String): String = {
    null
  }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

  def write2File(text: String, sc: SparkContext) {
    val rddRes = sc.makeRDD(Seq(text))
    rddRes.saveAsTextFile("hdfs:///user/res/" + text)
  }
  // helper function to convert Map to a Writable
  //http://loads.pickle.me.uk/2013/11/12/spark-and-elasticsearch.html
  def toWritable(map: Map[String, String]) = {
    val m = new MapWritable
    for ((k, v) <- map)
      m.put(new Text(k), new Text(v))
    m
  }

  def mapWritableToInput(in: MapWritable): Map[String, String] = {
    in.map { case (k, v) => (k.toString, v.toString) }.toMap
  }
  /**
   * Only in client mode
   */
  def saveModel(path: String, model: GradientBoostedTrees) {
    //save model 
    import java.io.FileOutputStream
    import java.io.ObjectOutputStream
    val fos = new FileOutputStream("/home/hadoop/modelAll")
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(model)
    oos.close
  }
  /**
   * Only in client mode
   */
  def loadModel() {
    import java.io.FileInputStream
    import java.io.ObjectInputStream
    var model: org.apache.spark.mllib.tree.model.GradientBoostedTreesModel = null
    val fos = new FileInputStream("/home/hadoop/modelAll")
    val oos = new ObjectInputStream(fos)
    model = oos.readObject().asInstanceOf[org.apache.spark.mllib.tree.model.GradientBoostedTreesModel]
  }

  /**
   * Method for choosing domains with more then minCandNum candidates .
   * @  minCandNum is minimum number of candidates per domain
   * @  allData is parsed data with all domains
   */
  def domainsList(allData: RDD[(String, Map[String, String])], minCandNum: Int, minGrpNum: Int): RDD[(String, Long)] = {
    val domain = allData.map { l => Utils.getDomain(l._2.apply("url")) }
    val words = domain.flatMap(x => x.split(","))
    val countDomain = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)
    val domainList = countDomain.filter(d => d._2 >= minCandNum)
    val indexedDomainList = domainList.map(n => n._1).zipWithIndex
    def domNameGrp(indexedDomainList: RDD[(String, Long)]): RDD[(String, Long)] = {
      val domainGrp = indexedDomainList.count / minGrpNum
      indexedDomainList.map(n => (n._1, n._2 % domainGrp))
    }
    domNameGrp(indexedDomainList)
  }

  /**
   * Method for printing domains to file by row. => for example printDom2File(all,sc,80)
   * @  minCandNum is minimum number of candidates per domain
   * @  allData is parsed data with all domains
   */
  def printDom2File(allData: RDD[(String, Map[String, String])], sc: SparkContext, minCandNum: Int, minGrpNum: Int) = {
    val domainNameGrp = domainsList(allData, minCandNum, minGrpNum).groupBy(_._2).map { t =>
      t._2.toList.map(_._1).mkString(",")
    }.collect().mkString("\n")
    sc.parallelize(List(domainNameGrp), 1).saveAsTextFile("hdfs:///pavlovout/dscores/test/")
  }

  def parseMEnrichMessage(dstream: DStream[(String, Array[Byte])]): DStream[(Array[Byte], Map[String, String])] = {
    dstream.map {
      case (s, msgBytes) =>
        val msg = MEnrichMessage.string2Message(msgBytes)
        //println(msg.toJson())
        val parsedMsg: (Array[Byte], Map[String, String]) = (msgBytes, Utils.json2Map(Json.parse(msg.toJson().toString())))
        parsedMsg
    }
  }
  def getPriceFromMsgMap(msgMap: Map[String, String]): Double = {
    //TODO this method created to handle logic to decide which price to push on
    //at the moment we use model price if the pattern failed(==0) or the distance betwen both of them smaller than 10% 
    val updatedPrice = parseDouble(msgMap.apply("updatedPrice")).get
    val modelPrice = parseDouble(msgMap.apply("modelPrice")).get
    val status = msgMap.apply("issue")
    status match {
      case "modeledPatternEqualsCounter" => modelPrice
      case "minorModelPatternConflict" => modelPrice
      case "majorModelPatternConflict" => updatedPrice
      case "patternFailed" => modelPrice
      case "missingModel" => updatedPrice
      case "allFalseCandids" => updatedPrice
    }
  }
  def gcd(a: Int, b: Int): Int = if (b == 0) a.abs else gcd(b, a % b)
  def lcm(a: Int, b: Int) = (a * b).abs / gcd(a, b)
  def lcm(numSet: Set[Int]): Long = {
    numSet.reduce(lcm(_, _))
  }

  def pushByteRDD2Kafka(rdd: RDD[Array[Byte]], outputTopic: String,  brokers: String,logTopic: String = "logs") = {
    rdd.foreachPartition { p =>
      val props = new Properties()
      props.put("metadata.broker.list", brokers)
      props.put("serializer.class", "kafka.serializer.DefaultEncoder")

      @transient val config = new ProducerConfig(props)
      @transient val producer = new Producer[String, Array[Byte]](config)
      p.foreach { rec =>
        if (MEnrichMessage.string2Message(rec).getM_errorMessage.equals("")&&(!outputTopic.equals("")))
          producer.send(new KeyedMessage[String, Array[Byte]](outputTopic, rec))
        else
          producer.send(new KeyedMessage[String, Array[Byte]](logTopic, rec))
      }

      producer.close()
    }
  }

}   

