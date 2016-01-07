package um.re.emr
import scala.xml.XML
import java.io.FileWriter
/**
 * @author mike
 */
object ParseGglXML extends App {

  val csvFW = new FileWriter("/Users/mike/Documents/deepricerKwaliteitparketFULL.csv", true)
  val xml = XML.loadFile("/Users/mike/Documents/google_feed_NL.xml")
  val items = (xml \\ "channel" \ "item")
  val data = items.filter { item => (item \ "gtin").size > 0 }
  val output = data.map { item =>
    val id = (item \ "id").text
    val cleanCategory = (item \ "product_type").text.replaceAll(",", ".")
    val title = (item \ "title").text
    val cleanTitle = title.replaceAll(",", ".")
    //val brand = (item \ "brand").text
    //val cleanBrand = brand.replaceAll(",", ".")
    val gtin = (item \ "gtin").text
    val link = (item \ "link").text
    val cleanLink = link.substring(0, link.indexOf("?source="))
    val price = {
      if ((item \ "sale_price").size > 0)
        (um.re.utils.Utils.parseDouble((item \ "sale_price").text.substring(0, (item \ "sale_price").text.indexOf(" EUR"))).get)
      else
        (um.re.utils.Utils.parseDouble((item \ "price").text.substring(0, (item \ "price").text.indexOf(" EUR"))).get)
    }
    val cleanPrice = "%.2f" format price
    

    val row = Array(id, cleanCategory, cleanTitle, cleanLink, cleanPrice, "", gtin, "Kwaliteitparket.nl", "NONE", "nl_NL").mkString(",")
    row
  }
  csvFW.write("ID,Category,Title,URL,Price,Brand,MPN,UPC,SKU,LOCALE\n" + output.mkString("\n"))
  csvFW.close()
}