import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, IndexToString}
import org.apache.spark.ml.{Pipeline}
import org.apache.spark.sql.functions._

object train extends App {
  val spark = SparkSession.builder().appName("train").getOrCreate()
  import spark.implicits._

  val input_train_path = spark.conf.get("spark.mlproject.input_train_path")
  val output_train_path = spark.conf.get("spark.mlproject.output_train_path")

  val input = spark.read.json(input_train_path)

  val training = input
    .select('uid, 'gender_age, explode('visits).alias("visit"))
    .withColumn("host", lower(callUDF("parse_url", $"visit.url", lit("HOST"))))
    .withColumn("domain", regexp_replace($"host", "www.", ""))
    .dropDuplicates(Seq("uid", "gender_age", "domain"))
    .groupBy('uid, 'gender_age).agg(collect_list('host).alias("domains"))
    .select('uid, 'domains, 'gender_age)

  val labelIndexer = new StringIndexer()
    .setInputCol("gender_age")
    .setOutputCol("indexedLabel")
    .fit(training)

  val cv = new CountVectorizer()
    .setInputCol("domains")
    .setOutputCol("features")

  val indexer = new StringIndexer()
    .setInputCol("gender_age")
    .setOutputCol("label")

  val lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.001)

  val indexToStringEstimator = new IndexToString()
    .setInputCol("prediction")
    .setOutputCol("category")
    .setLabels(labelIndexer.labels)

  val pipeline = new Pipeline()
    .setStages(Array(labelIndexer, cv, indexer, lr, indexToStringEstimator))

  val model = pipeline.fit(training)

  model.write.overwrite().save(output_train_path)
}
