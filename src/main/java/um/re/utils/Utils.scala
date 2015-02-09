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
import um.re.es.emr.PriceParcer
import java.net.URI
object Utils {

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
  /**
   * this function replaces all characters and number with space and trip multiple spaces
   */
  def textOnly(text: String) = {
    text.replaceAll("[^A-Za-z]+", " ").replaceAll("[\\p{Blank}]{1,}?", " ")
  }
  /**
   * Tokenazer
   */
  def tokenazer(text: String) = {
    textOnly(text).split(" ").toSeq
  }

  def map2JsonString(map: Map[String, String]) = {
    val asJson = Json.toJson(map)
    Json.stringify(asJson)
  }

  def json2Map(map: Map[String, String]) = {
    val asJson = Json.toJson(map)
    Json.stringify(asJson)
  }
  /**
   * This function takes ES source and transforms it to format of R candidates
   */
  def getCandidates(source2: RDD[(String, Map[String, String])]) = {
    val candid = source2.map { l =>
      try {
        val nf = PriceParcer
        val id = l._2.get("url").toString
        val h = l._2.get("price_prop1").toString
        val res = nf.find(id, h)
        res
      } catch {
        case _: Exception => { "[{\"no\":\"data\"}]" }
      }
    }
    candid
  }
  def getCandidatesPatternsHtmlTrimed(source2: RDD[(String, Map[String, String])]): RDD[List[Map[String, String]]] = {
    val candid = source2.map { l =>
      try {
        val nf = PriceParcer
        nf.snippetSize = 150
        val id = l._2.get("url").get
        val price = l._2.get("price_updated").get
        val html = shrinkString(l._2.get("price_prop1").get)
        val patterns = shrinkString(l._2.get("price_patterns").get)
        val res = nf.findM(id, html)
        val p_h = Map("patterns" -> patterns, "html" -> html, "price" -> price)
        p_h :: res
      } catch {
        case _: Exception => Nil
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
      extPatternLocationPair(shrinkString(p), shrinkString(html), new_length)
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
}