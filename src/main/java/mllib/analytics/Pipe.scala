package mllib.analytics

object Pipe extends App {
  import org.apache.spark.{ SparkConf, SparkContext }
  import org.apache.spark.ml.Pipeline
  import org.apache.spark.ml.classification.LogisticRegression
  import org.apache.spark.ml.feature.{ HashingTF, Tokenizer }
  import org.apache.spark.sql.{ Row, SQLContext }

  // Labeled and unlabeled instance types.
  // Spark SQL can infer schema from case classes.
  case class LabeledDocument(id: Long, text: String, label: Double)
  case class Document(id: Long, text: String)

  // Set up contexts.  Import implicit conversions to SchemaRDD from sqlContext.
  val conf = new SparkConf().setAppName("SimpleTextClassificationPipeline")
  val sc = new SparkContext(conf)
  
  val sqlContext = new SQLContext(sc)
  import sqlContext._

  // Prepare training documents, which are labeled.
  val training = sparkContext.parallelize(Seq(
    LabeledDocument(0L, "a b c d e spark", 1.0),
    LabeledDocument(1L, "b d", 0.0),
    LabeledDocument(2L, "spark f g h", 1.0),
    LabeledDocument(3L, "hadoop mapreduce", 0.0)))

  // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
  val tokenizer = new Tokenizer()
    .setInputCol("text")
    .setOutputCol("words")
  val hashingTF = new HashingTF()
    .setNumFeatures(1000)
    .setInputCol(tokenizer.getOutputCol)
    .setOutputCol("features")
  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.01)
  val pipeline = new Pipeline()
    .setStages(Array(tokenizer, hashingTF, lr))

  // Fit the pipeline to training documents.
  val model = pipeline.fit(training)

  // Prepare test documents, which are unlabeled.
  val test = sparkContext.parallelize(Seq(
    Document(4L, "spark i j k"),
    Document(5L, "l m n"),
    Document(6L, "mapreduce spark"),
    Document(7L, "apache hadoop")))

  // Make predictions on test documents.
  model.transform(test)
    .select('id, 'text, 'score, 'prediction)
    .collect()
    .foreach {
      case Row(id: Long, text: String, score: Double, prediction: Double) =>
        println("(" + id + ", " + text + ") --> score=" + score + ", prediction=" + prediction)
    }
}

