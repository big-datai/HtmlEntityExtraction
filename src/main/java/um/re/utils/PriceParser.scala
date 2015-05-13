package um.re.utils
import scala.io.Source
import scala.util.matching.Regex
import play.api.libs.json.{ Json, JsValue, JsObject }

object PriceParser extends Serializable {

  val CURRENCY_SYMBOLS: Regex = "\\p{Sc}".r
  val TEXT_NEAR_PRICE: Regex = "(price)|(Price)|(PRICE)".r
  val NUM_PATTERN: Regex = "([0-9,\\.]*[0-9])(?:[^0-9,\\.])".r
  var snippetSize: Int = 50

  def findFast(url: String, html: String) /*: List[Map[String, String]] */= {
    val candidates = NUM_PATTERN.findAllMatchIn(html).map{m =>
    val price = m.group(1)  
    val str_before: String = m.source.subSequence(math.max(m.start - snippetSize, 0), m.start).toString() 
    val str_after: String = m.source.subSequence(m.end-1, math.min(m.end + snippetSize - 1, m.source.length)).toString()
    val location = m.start.toString
    (url,price,str_before,str_after,location)
    }.filterNot{case(url,price,str_before,str_after,location) =>
      val snip = str_before+price+str_after
      ((!price.contains(".")) && (!price.contains(","))) ||
        (price.contains(",,") || price.contains(".,") ||
          price.contains(",.") || price.contains("..")) ||
          ((!CURRENCY_SYMBOLS.findFirstIn(snip).isDefined) && (!TEXT_NEAR_PRICE.findFirstIn(snip).isDefined))
      }.map{case(url,price,str_before,str_after,location) =>
        val candidMap = Map("url" -> url ,
        "priceCandidate" -> price,
        "text_before" -> str_before,
        "text_after" -> str_after,
        "location" -> location)
        candidMap
        }.toList
    candidates
  }
  def findM(url: String, html: String): List[Map[String, String]] = {
    val candidates = fetchPriceCandidates(html)
    createMap(candidates, url)
    List.empty
  }
  /*def find(url: String, html: String): JsValue = {
    // this is the main method to use, Input : URL and HTML , Output : JSON object with price candidates and their features  
    val candidates = fetchPriceCandidates(html)
    val jsonRespond = createJSON(candidates, url)
    jsonRespond
  }*/
  def fetchPriceCandidates(html: String) = {
    // this method contains the logic by which it finds price candidates
    // it also extracts what it needs from the Match type while it is in top Memory for performance
    NUM_PATTERN.findAllMatchIn(html).filterNot { candidate =>
      val snip = candidate.before.subSequence(math.max(candidate.before.length() - snippetSize, 0), candidate.before.length) +
        candidate.toString + candidate.after.subSequence(0, math.min(snippetSize - 1, candidate.after.length))
      ((!candidate.matched.contains(".")) && (!candidate.matched.contains(","))) ||
        (candidate.matched.contains(",,") || candidate.matched.contains(".,") ||
          candidate.matched.contains(",.") || candidate.matched.contains("..")) ||
          ((!CURRENCY_SYMBOLS.findFirstIn(snip).isDefined) && (!TEXT_NEAR_PRICE.findFirstIn(snip).isDefined))
    }.map{m => (m.group(1),m.start,m.end,m.source) }
  }
  def createMap(iter: Iterator[(String,Int,Int,CharSequence)], url: String): List[Map[String, String]] = {
    val mapList = iter.map { case(candid,start,end,source)  =>
      Map("url" -> url , "priceCandidate" -> candid) ++ extractFeaturesM(start,end,source)
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

  def extractFeaturesM(start:Int ,end:Int ,source:CharSequence): Map[String, String] = {
    // this method extracts relevant features for a price candidate , such as snippet from HTML and its location
    val str_before: String = source.subSequence(math.max(start - snippetSize, 0), start).toString() 
    val str_after: String = source.subSequence(end-1, math.min(end + snippetSize - 1, source.length)).toString() 
      
    Map("text_before" -> str_before,
      "text_after" -> str_after,
      "location" -> start.toString)
  }
  def extractFeatures(m: Regex.Match): Map[String, JsValue] = {
    // this method extracts relevant features for a price candidate , such as snippet from HTML and its location
    val str: String = m.before.subSequence(math.max(m.before.length() - snippetSize, 0), m.before.length) +
      m.toString + m.after.subSequence(0, math.min(snippetSize - 1, m.after.length))
    Map("text" -> Json.toJson(str),
      "location" -> Json.toJson(m.start.toString))
  }
  
}