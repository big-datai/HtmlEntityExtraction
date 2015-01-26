package um.re.es.emr;

import scala.math
import scala.io.Source
import scala.util.matching.Regex
import play.api.libs.json.{ Json, JsValue, JsObject, JsArray }
import scala.io.Codec

/*class NumberFinder2(val CURRENCY_SYMBOLS :Regex = "\\p{Sc}".r,
  				   val TEXT_NEAR_PRICE :Regex = "(price)|(Price)|(PRICE)".r,
  				   val NUM_PATTERN :Regex = "([0-9,\\.]*[0-9])(?:[^0-9,\\.])".r,
  				   val snippetSize :Int = 50) extends Serializable {
  
  */
object NumberFinder2 extends Serializable {

  val CURRENCY_SYMBOLS: Regex = "\\p{Sc}".r
  val TEXT_NEAR_PRICE: Regex = "(price)|(Price)|(PRICE)".r
  val NUM_PATTERN: Regex = "([0-9,\\.]*[0-9])(?:[^0-9,\\.])".r
  var snippetSize: Int = 50

  def findM(url: String, html: String) : List[Map[String,String]]= {
    val candidates = fetchPriceCandidates(html)
    createMap(candidates,url)
  }
  def find(url: String, html: String) : JsValue= {
    // this is the main method to use, Input : URL and HTML , Output : JSON object with price candidates and their features  
    val candidates = fetchPriceCandidates(html)
    val jsonRespond = createJSON(candidates, url)
    jsonRespond
  }
  def fetchPriceCandidates(html: String): Iterator[Regex.Match] = {
    // this method contains the logic by which it finds price candidates
    NUM_PATTERN.findAllMatchIn(html).filterNot { candidate =>
      val snip = candidate.before.subSequence(math.max(candidate.before.length() - snippetSize, 0), candidate.before.length) +
        candidate.toString + candidate.after.subSequence(0, math.min(snippetSize - 1, candidate.after.length))
      ((!candidate.matched.contains(".")) && (!candidate.matched.contains(","))) ||
        (candidate.matched.contains(",,") || candidate.matched.contains(".,") ||
          candidate.matched.contains(",.") || candidate.matched.contains("..")) ||
          ((!CURRENCY_SYMBOLS.findFirstIn(snip).isDefined) && (!TEXT_NEAR_PRICE.findFirstIn(snip).isDefined))
    }
  }
  def createMap(iter: Iterator[Regex.Match], url: String): List[Map[String,String]] = {
    val mapList = iter.map { i =>
      	Map("url" -> url,
          "priceCandidate" -> i.group(1)) ++
          extractFeaturesM(i)
    }.toList
    mapList
  }
  def createJSON(iter: Iterator[Regex.Match], url: String): JsValue = {
    // this method forms the cnadidates into a JSON object 
    val jsonList = iter.map { i =>
      JsObject(
        (Map("URL" -> Json.toJson(url),
          "priceCandidate" -> Json.toJson(i.group(1))) ++
          extractFeatures(i)).toSeq)
    }.toList

    //Json.toJson
    Json.toJson(jsonList)

  }
  
  def extractFeaturesM(m: Regex.Match): Map[String, String] = {
    // this method extracts relevant features for a price candidate , such as snippet from HTML and its location
    val str_before: String = m.before.subSequence(math.max(m.before.length() - snippetSize, 0), m.before.length).toString()
    val str_after: String =  m.toString.substring(m.toString.length-1) + m.after.subSequence(0, math.min(snippetSize - 1, m.after.length)).toString()
    Map("text_before" -> str_before,
        "text_after" -> str_after,
        "location" -> m.start.toString)
  }
  def extractFeatures(m: Regex.Match): Map[String, JsValue] = {
    // this method extracts relevant features for a price candidate , such as snippet from HTML and its location
    val str: String = m.before.subSequence(math.max(m.before.length() - snippetSize, 0), m.before.length) +
      m.toString + m.after.subSequence(0, math.min(snippetSize - 1, m.after.length))
    Map("text" -> Json.toJson(str),
      "location" -> Json.toJson(m.start.toString))
  }
  def test(url: String, html: String, price: String): String = {
    // this method made for testing , it checks if the price given is included in the candidates it finds
    val cand = fetchPriceCandidates(html).map(p => p.matched.substring(0, p.matched.length - 1)).toList
    val result = url + "\t" + price + "\t" + cand + "\t" + cand.contains(price)
    result
  }
  def readFromURL(url: String): String = {
    try { Source.fromURL(url).mkString }
    catch { case _: Throwable => "NA" }
  }
  def readFromFile(path: String): String = Source.fromFile(path).mkString

}