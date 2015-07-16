package um.re.bin

import org.apache.commons.io.output.ByteArrayOutputStream
import java.io.ObjectOutputStream
import java.io.ByteArrayInputStream
import java.io.ObjectInputStream

@SerialVersionUID(100L)
class Msg(var url: String,var html: String ,var price: Double,var priceSource: String) extends Serializable {
  def getBytes(): Array[Byte] = {
    val b = new ByteArrayOutputStream()
    val o = new ObjectOutputStream(b)
    o.writeObject(this)
    b.toByteArray()
  }
  override def toString():String={
    this.url+"---"+this.html+"---"+this.price+"---"+this.priceSource
  }
  def this(bytes :Array[Byte]) = this((new ObjectInputStream(new ByteArrayInputStream(bytes))).readObject().asInstanceOf[Msg].url,
      (new ObjectInputStream(new ByteArrayInputStream(bytes))).readObject().asInstanceOf[Msg].html,
      (new ObjectInputStream(new ByteArrayInputStream(bytes))).readObject().asInstanceOf[Msg].price,
      (new ObjectInputStream(new ByteArrayInputStream(bytes))).readObject().asInstanceOf[Msg].priceSource)    
    
}