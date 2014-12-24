/*
package rtb.streaming

import java.io.File
import java.util.Calendar
import org.jets3t.service.impl.rest.httpclient.RestS3Service
import org.jets3t.service.security.AWSCredentials
import org.jets3t.service.model.S3Object
import org.jets3t.service.acl.{ AccessControlList, GroupGrantee, Permission }
import java.io.InputStream
import java.io.PrintWriter
import org.apache.commons.io.IOUtils
//import com.wiser.rabbit.UCsv2Queue

object Rabbit2S3 extends App {
  //val classPath = "/"

  // upload a simple text file
   val textFilename = Calendar.getInstance().getTime().toGMTString().replaceAll("[\" \":]", "-") + ".txt"
  // println(textFilename)
  val linkForText = store(textFilename, "Hello to s3")
  println(s"Url for the text file is $linkForText")
  var mess_consumer: UCsv2Queue = new UCsv2Queue("elasticsearch", 300); //define queue
  var size = 32
  var messages: String = ""
  var count:Int=0                  
  while (size < 120000000) {
    var delivery = mess_consumer.consumer.nextDelivery();
    var ms: String = null
    if (delivery != null) {
      ms = new String(delivery.getBody(), "UTF-8")
    }
    messages = messages.concat(ms)
    // println(messages)
    size = messages.length() * 2
    println(size)
    count=count+1
    println(count)
    mess_consumer.channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false)
  }
  store(messages)
  def store(source: String) {
    store(Calendar.getInstance().getTime().toGMTString().replaceAll("[\" \":]", "-") + ".txt", source)
  }
  def store(key: String, source: String, contentType: String = "text/plain") = {
    // replace the values with the access keys you get from Amazon(refer to the beginning of this tutorial)   
    val inputStream = IOUtils.toInputStream(source, "UTF-8");
    val awsAccessKey = "AKIAIWRROHVN7YNY6DSQ"
    val awsSecretKey = "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"
    val awsCredentials = new AWSCredentials(awsAccessKey, awsSecretKey)
    val s3Service = new RestS3Service(awsCredentials)
    val bucketName = "pavlov-ml"
    val bucket = s3Service.getOrCreateBucket(bucketName)
    val fileObject = s3Service.putObject(bucket, {
      // public access is disabled by default, so we have to manually set the permission to allow read access to the uploaded file
      val acl = s3Service.getBucketAcl(bucket)
      acl.grantPermission(GroupGrantee.ALL_USERS, Permission.PERMISSION_READ)
      val tempObj = new S3Object(key)
      tempObj.setDataInputStream(inputStream)
      tempObj.setAcl(acl)
      tempObj.setContentType(contentType)
      tempObj
    })

    s3Service.createUnsignedObjectUrl(bucketName,
      fileObject.getKey,
      false, false, false)
  }
}
*/