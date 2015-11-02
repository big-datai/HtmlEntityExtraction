package um.re.test

import com.utils.messages.BigMessage
import um.re.utils.Utils
import play.api.libs.json._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import um.re.transform.Transformer
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.Vectors

object HtmlsToPredictedPipeTest extends App {
  val conf_s = new SparkConf().setMaster("local[*]").setAppName("HTPTest")
  val sc = new SparkContext(conf_s)
  
  val dMapPath = "/Users/dmitry/umbrella/rawd/objects/dMap.txt"
  val modelsPath = "/Users/dmitry/umbrella/rawd/objects/Models/"
    
  val dMap = sc.broadcast(sc.textFile((dMapPath), 1).collect().mkString("\n").split("\n").map(l => (l.split("\t")(0), l.split("\t")(1))).toMap)
  val msgStr =  """{"url":"http://www.lnt.com/product/hardware/796131-2860234/wooden-mallet-wooden-mallet-dw2-4-valley-series-four-seat-sofa.html?utm_source=googleproductads&utm_medium=cpc","title":"Waiting Room Furniture-Quadruple Sled-Base Sofa, Light Oak-CB","patternsHtml":"://schema.org/Offer\">\r\n <span itemprop=\"price\">\r\n (.*?)\r\n </span>\r\n <meta itemprop=\"priceCurrency\" |||_sign\">\r\n $\r\n </span>\r\n <span itemprop=\"price\">\r\n (.*?)\r\n </span>\r\n <meta itemprop=\"priceCurrency\" |||_sign\">\r\n $\r\n </span>\r\n <span itemprop=\"price\">\r\n (.*?)\r\n </span>\r\n <meta itemprop=\"priceCurrency\" |||_sign\">\r\n $\r\n </span>\r\n <span itemprop=\"price\">\r\n (.*?)\r\n </span>\r\n <meta itemprop=\"priceCurrency\" |||_sign\">\r\n $\r\n </span>\r\n <span itemprop=\"price\">\r\n (.*?)\r\n </span>\r\n <meta itemprop=\"priceCurrency\" |||_sign\">\r\n $\r\n </span>\r\n <span itemprop=\"price\">\r\n (.*?)\r\n </span>\r\n <meta itemprop=\"priceCurrency\" |||],\necomm_pagetype: 'product',\necomm_totalvalue: [\"(.*?)\"]};\n\r\n//]]>\r\n </script>\r\n <script type=\"tex|||","price":"710.0","html":"no","patternsText":"p via: White Glove Delivery: Room of Choice\nPrice:$(.*?)\n$118.33 per month*\nSpread payments over si|||olor & Quantity\nItem\nQuantity\n0 +\nSelect Color...\n$(.*?)\n$118.33 per month*\nSpread payments over si||| for a \"Bill Me Later\" account.\n$1004.00\nSave 29%\n$(.*?)\n$118.33 per month*\nSpread payments over si||| for a \"Bill Me Later\" account.\n$1004.00\nSave 29%\n$(.*?)\n$118.33 per month*\nSpread payments over si||| for a \"Bill Me Later\" account.\n$1004.00\nSave 29%\n$(.*?)\n$118.33 per month*\nSpread payments over si||| for a \"Bill Me Later\" account.\n$1004.00\nSave 29%\n$(.*?)\n$118.33 per month*\nSpread payments over si|||","shipping":"0.0","prodId":"2816313.0","domain":"lnt.com","updatedPrice":"710.0"}"""
  val msg = BigMessage.string2Message(msgStr)
  msg.sethtml("""<div class="pricex" itemprop="offers" itemscope>
		<span class="dollar_sign">$</span><span itemprop="price">710.00</span><meta itemprop="priceCurrency" content="USD">	</div>""")
  val msgObj = msg.toJson().toString().getBytes() 
  val msgMap = Utils.json2Map(Json.parse(msg.toJson().toString()))
  //println(msgMap.get())
  val rdd = sc.parallelize(Seq((msgObj,msgMap)), 1)
  println( "a : " + Utils.getCandidatesPatternsHtmlTrimed(rdd).take(1)(0)._2.mkString("\n"))
  val candidates = Utils.htmlsToCandidsPipe(rdd)
  //candidates.collect().foreach(l=>println(l._2))
  
  val predictions = candidates.map{case(msg,candidList) =>
	  //TODO test path
    val url = candidList.head.apply("url")
    val domain = Utils.getDomain(url)
    val domainCode = dMap.value.apply(domain)  
    //val (model, idf, selected_indices) = sc.objectFile[(GradientBoostedTreesModel, Array[Double], Array[Int])]("/Users/dmitry/umbrella/rawd/objects/Models/" + domainCode + "/part-00000", 1).first
    val (model, idf, selected_indices) = sc.objectFile[(GradientBoostedTreesModel,Array[Double],Array[Int])](modelsPath + domainCode+"/part-00000",1).first

      val modelPredictions = candidList.map { candid =>
        val priceCandidate = candid.apply("priceCandidate")
        val location = Integer.valueOf(candid.apply("location")).toDouble
        val text_before = Utils.tokenazer(candid.apply("text_before"))
        val text_after = Utils.tokenazer(candid.apply("text_after"))
        val text = text_before ++ text_after
        
        val hashingTF = new HashingTF(1000)
        val tf = Transformer.projectByIndices(hashingTF.transform(text).toArray, selected_indices)
        val tfidf = (tf, idf).zipped.map((tf, idf) => tf * idf)

        val features = tfidf ++ Array(location)
        val values = features.filter { l => l != 0 }
        val index = features.zipWithIndex.filter { l => l._1 != 0 }.map { l => l._2 }
        val featureVec = Vectors.sparse(features.length, index, values)

        val prediction = model.predict(featureVec)
        val confidence = Transformer.confidenceGBT(model, featureVec)
        (confidence, prediction, priceCandidate)
      }
      //TODO deal with extreme case e.g. all candidates are negative
      var selectedCandid = (0.0,0.0,"0")
      if (modelPredictions.filter(c => c._2 == 1).size >=1)
    	  selectedCandid = modelPredictions.filter(c => c._2 == 1).sorted.reverse.head
    	  else
    	    selectedCandid = (0,0,"-1")

      val predictedPrice = selectedCandid._3
      val msgObj : BigMessage = BigMessage.string2Message(msg) 
      msgObj.setModelPrice(predictedPrice)
      msgObj.toJson().toString().getBytes()
  }
 
  println("r : "+ rdd.count)
  
  println("c : "+ candidates.count)
  
  println("p : "+predictions.count)
  
  
}