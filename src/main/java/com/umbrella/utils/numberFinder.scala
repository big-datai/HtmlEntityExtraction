package com.umbrella.utils

import scala.math
import scala.io.Source
import scala.util.matching.Regex
import play.api.libs.json.{Json,JsValue,JsObject}

object numberFinder {
  //val pricePatternUS:Regex ="([1-9][0-9]{0,2}(,[0-9]{3})*|[0-9]+)(\\.[0-9]{1,9})?".r
  //val pricePatternEU:Regex ="([1-9][0-9]{0,2}(\\.[0-9]{3})*|[0-9]+)(,[0-9]{1,9})?".r
  //val pricePattern:Regex = ("(?:"+pricePatternEU.pattern.pattern+")|(?:"+pricePatternUS.pattern.pattern+")").r
  //val pat:Regex = "(?:[^0-9,\\.A-Za-z])([0-9,\\.]*[0-9])(?:[^0-9,\\.A-Za-z])".r 
  val CURRENCY_SYMBOLS = "\\p{Sc}".r
  val pat:Regex = "([0-9,\\.]*[0-9])(?:[^0-9,\\.])".r
  val snippetSize:Int = 50
  def main(args:Array[String]){
	/*//val html = args(1)
	val url = "http://www.buyarearugs.com/rugs-delphine-mr64-xgx.html"//args(2)
    val source = readFromFile("C:\\test\\prodPage5.html")
    //val source = readFromFile("C:\\test\\test.txt")
	val matches = fetchPriceCandidates(pat,source)
    //val jsonResult = createJSON(matches,url)
    //println(Json.prettyPrint(jsonResult))
    val m = matches.toList
	println(m)
    println(m.size)*/
    test("C:\\test\\Mwetx7Or_URL_PRICE.txt")
  }
  
  def readFromURL(url:String):String = {try{Source.fromURL(url).mkString}
  										catch{ case _ : Throwable=> "NA"}}
  def readFromFile(path:String):String = Source.fromFile(path).mkString
  
  def fetchPriceCandidates(regex:Regex,str:String):Iterator[Regex.Match] = {
    regex.findAllMatchIn(str).filterNot(s=> 
      ((!s.matched.contains("."))&&(!s.matched.contains(",")))||
      (s.matched.contains(",,")||s.matched.contains(".,")||
      s.matched.contains(",.")||s.matched.contains(".."))||
      (!CURRENCY_SYMBOLS.findFirstIn(s.before.subSequence(math.max(s.before.length()-snippetSize,0), s.before.length)+
          s.toString+s.after.subSequence(0, math.min(snippetSize-1,s.after.length))).isDefined))
    }
  def createJSON(iter:Iterator[Regex.Match],url:String):JsObject={
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
    Json.toJson(
      Map("text"->Json.toJson(m.before.subSequence(math.max(m.before.length()-snippetSize,0), m.before.length)+
          m.toString+m.after.subSequence(0, math.min(snippetSize-1,m.after.length))),
		  "location"->Json.toJson(m.start))
	)
  }
  def test(filePath:String)={
    val tests = Source.fromFile(filePath).getLines.map(line => (line.split("\t")(0),line.split("\t")(1))).toList
    var htmlMap: Map[String,String] = Map.empty
    tests.foreach{test =>htmlMap= htmlMap++Map(test._1-> readFromURL(test._1)) }
    val results =  tests.map{test => 
      if (htmlMap.get(test._1).get.equals("NA"))
    	  test._1+"\t"+test._2+"\t"+"NA"+"\t"+"NA"
    	else {
    		val cand = fetchPriceCandidates(pat, htmlMap.get(test._1).get).map(p=> p.matched.substring(0,p.matched.length-1)).toList
    		test._1+"\t"+test._2+"\t"+cand+"\t"+cand.contains(test._2)
    	} 
      }
    println(results.mkString("\r\n"))
    
    
  }
}