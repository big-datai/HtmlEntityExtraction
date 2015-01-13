package um.re.es.emr;

import scala.math
import scala.io.Source
import scala.util.matching.Regex
import play.api.libs.json.{Json,JsValue,JsObject}
import scala.io.Codec

/*class NumberFinder(val CURRENCY_SYMBOLS :Regex = "\\p{Sc}".r,
  				   val TEXT_NEAR_PRICE :Regex = "(price)|(Price)|(PRICE)".r,
  				   val NUM_PATTERN :Regex = "([0-9,\\.]*[0-9])(?:[^0-9,\\.])".r,
  				   val snippetSize :Int = 50) extends Serializable {
  
  */
object NumberFinder {
  val CURRENCY_SYMBOLS :Regex = "\\p{Sc}".r
  val TEXT_NEAR_PRICE :Regex = "(price)|(Price)|(PRICE)".r
  val NUM_PATTERN :Regex = "([0-9,\\.]*[0-9])(?:[^0-9,\\.])".r
  val snippetSize :Int = 50
  
  def find(url:String,html:String)={
    // this is the main method to use, Input : URL and HTML , Output : JSON object with price candidates and their features  
    val candidates = fetchPriceCandidates(html)
    val jsonRespond = createJSON(candidates, url)
    jsonRespond
  }
  def fetchPriceCandidates(str:String):Iterator[Regex.Match] = {
    // this method contains the logic by which it finds price candidates
    NUM_PATTERN.findAllMatchIn(str).filterNot{s=> 
      val snip = s.before.subSequence(math.max(s.before.length()-snippetSize,0), s.before.length)+
          s.toString+s.after.subSequence(0, math.min(snippetSize-1,s.after.length))
      ((!s.matched.contains("."))&&(!s.matched.contains(",")))||
      (s.matched.contains(",,")||s.matched.contains(".,")||
      s.matched.contains(",.")||s.matched.contains(".."))||
      ((!CURRENCY_SYMBOLS.findFirstIn(snip).isDefined)&&(!TEXT_NEAR_PRICE.findFirstIn(snip).isDefined))}
    }
  def createJSON(iter:Iterator[Regex.Match],url:String):JsObject={
    // this method forms the cnadidates into a JSON object 
    val candidates = Json.toJson(iter.map{ i => 
		  Json.toJson(
		      Map(i.group(1)->extractFeatures(i)
		      )
		  )
	}.toSeq)
	
    Json.obj("URL"->url,
    		 "priceCandidates"->candidates)
     
  }
  def extractFeatures(m:Regex.Match):JsValue = {
    // this method extracts relevant features for a price candidate , such as snippet from HTML and its location
    Json.toJson(
      Map("text"->Json.toJson(m.before.subSequence(math.max(m.before.length()-snippetSize,0), m.before.length)+
          m.toString+m.after.subSequence(0, math.min(snippetSize-1,m.after.length))),
		  "location"->Json.toJson(m.start))
	)
  }
  def test(url:String,html:String,price:String):String ={
    // this method made for testing , it checks if the price given is included in the candidates it finds
    val cand = fetchPriceCandidates(html).map(p=> p.matched.substring(0,p.matched.length-1)).toList
    val result = url+"\t"+price+"\t"+cand+"\t"+cand.contains(price)
    result
  }
  def readFromURL(url:String):String = {try{Source.fromURL(url).mkString}
  										catch{ case _ : Throwable=> "NA"}}
  def readFromFile(path:String):String = Source.fromFile(path).mkString
  
}