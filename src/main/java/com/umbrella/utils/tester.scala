package com.umbrella.utils



object tester {
	def main(args:Array[String]){
		val nf = new NumberFinder
		val jsonStr = scala.io.Source.fromFile("C:\\test\\jsonArray.txt")("ISO_8859-1").getLines.toList.filter{l=>
		  l.startsWith("""			"url": """)||
		  l.startsWith("""			"price_prop1": """)||
		  l.startsWith("""			"price_updated": """)}
		.map{l=> 
		  val trimFrom = l.indexOf("""": """")
		  l.substring(trimFrom+4, l.length()-1)}.toArray
		for(i <- 0 to 49){
		  var id = i*3
		  println(nf.find(jsonStr(id), jsonStr(id+1)))
		  
		}
		
	}
}