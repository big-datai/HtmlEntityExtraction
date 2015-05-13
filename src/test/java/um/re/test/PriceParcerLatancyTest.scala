package um.re.test

import org.apache.commons.lang3.CharSequenceUtils

object PriceParcerLatancyTest extends App {
  //val urls = List("http://midhardware.com/hardware/product_info.php/hand-soap-p-7953698?osCsid=9081fe938fb19da4c27236595dcb34c7")
  /* "http://www.nationalbuildersupply.com/elkay-gourmet-undermount-steel-kitchen-sink-eluhaqd32179-stainless-steel/p144957",
      "http://www.atgstores.com/toilet-bowls/nameeks-gsi-mcity1811-city-toilet_g1054918.html?ProductSlot=2",
      "http://www.faucet.com/moen-yb2224-chrome-24-towel-bar-from-the-brantford-collection/f1205867",
      "http://www.build.com/quoizel-trg1716-ceiling-light/s917333",
      "http://www.efaucets.com/detail.asp?Product_Id=YB5186BN",
      "http://www.homeclick.com/toto-ss114-softclose-toilet-seat-elongated/p-194959.aspx",
      "http://www.faucetdepot.com/prod/Kohler-K-72218-VS-Sensate-Touchless-Pull-down-Kitchen-Faucet-with-DockNetik-Magnetic-Docking-System-and-3-Function-Sprayhead---Vibrant-Stainless-149249.asp",
      //"http://www.plumbingdepot.com/brands/toto/toto-thu068-cp-trip-lever-polished-chrome-for-drake-except-r-suffix-toilet",
      "http://www.hayneedle.com/product/esschertdesigngardenworkbench.cfm",
      "http://www.wayfair.com/Anchor-Hocking-2.5-Quart-Crystal-Mixing-Bowl-81575L11-HOH1086.html",
      "http://www.wayfairsupply.com/Boss-Office-Products-High-Back-Mesh-Task-Chair-with-Arms-B6706-BOP1594.html")
      */
  val urls = scala.io.Source.fromFile("/Users/dmitry/untitled1000.txt").mkString.replaceAll("\"\n\"", "\"\r\"").replaceAll("\n", "").
    split("\r").filter(l => l.startsWith(""""full_river","data","""")).
    map { l =>
      val l1 = l.substring(""""full_river","data","""".length())
      l1.substring(0, l1.indexOf("\""))
    }.filter(!_.startsWith("http://www.the-house.com"))
  val nf = um.re.utils.PriceParser
  val nfo = um.re.utils.PriceParcer
  nf.snippetSize = 150
  nfo.snippetSize = 150
  var counter = 0
  var letancy :List[Long]= List.empty
  urls.foreach { url =>
    println(counter + "\t" + url)
    counter += 1

    var html = ""
    var res: Long = -1
    try {
      html = scala.io.Source.fromURL(url).mkString
    } catch {
      case _: Throwable => html = ""
    }
    if (!(html.equals(""))) {
      val start = System.currentTimeMillis()
      val res = nf.findFast(url, html)
      val reso = nfo.findM(url, html)
      /*"([0-9,\\.]*[0-9])(?:[^0-9,\\.])".r.findAllMatchIn(html).filter(c=> c.end> 300).
      map{m => 
        val s = m.source
        val start = m.start
        val end = m.end
        (m.group(1),
          s.subSequence(math.max(start - 150, 0), start).toString(),
          s.subSequence(end, math.min(end+150 - 1, s.length)).toString(),
          m.start.toString)}.toList*/
      //html.replaceAll("([0-9,\\.]*[0-9])(?:[^0-9,\\.])", "")
      //"(?:[^0-9,\\.])([0-9,\\.]*[0-9])(?:[^0-9,\\.])".r.findAllMatchIn(html).toList
      //"[^0-9,\\.]([0-9,\\.]*[0-9])[^0-9,\\.]".r.findAllMatchIn(html).toList
      val end = System.currentTimeMillis()
      letancy = letancy ++ List(end - start)
      println(end - start)
      println("---")
      println(res.mkString("\n").equals(res.mkString("\n")))
      println("---")
    }
    println(letancy.size)
    println(letancy.sum.toDouble / letancy.size.toDouble)

  }

}