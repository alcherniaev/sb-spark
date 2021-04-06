import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.{PipelineModel}

object test extends App {
  val spark = SparkSession.builder().appName("test").getOrCreate()
  import spark.implicits._

  val input_topic_name = spark.conf.get("spark.mlproject.input_topic_name")
  val output_topic_name = spark.conf.get("spark.mlproject.output_topic_name")
  val output_train_path = spark.conf.get("spark.mlproject.output_train_path")

  val schema = StructType(
    StructField("uid",StringType,true) ::
      StructField("visits",ArrayType(StructType(StructField("timestamp",LongType,true) ::
        StructField("url",StringType,true) :: Nil),true),true) :: Nil)

  val model = PipelineModel.load(output_train_path)

  val sdf = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "spark-master-1:6667")
    .option("subscribe", input_topic_name)
    .load

  val testing = sdf
    .select(from_json($"value".cast("string"), schema).alias("value"))
    .select(col("value.*"))
    .select('uid, explode('visits).alias("visit"))
    .withColumn("host", lower(callUDF("parse_url", $"visit.url", lit("HOST"))))
    .withColumn("domain", regexp_replace($"host", "www.", ""))
    .dropDuplicates(Seq("uid", "domain"))
    .groupBy('uid).agg(collect_list('host).alias("domains"))
    .select('uid, 'domains)

  val dfFinal = model.transform(testing)

  val sink = createKafkaSink(dfFinal.select('uid, 'category.alias("gender_age")))
  val sq = sink.start()
  sq.awaitTermination()

  def createKafkaSink(df: DataFrame) = {
    df.toJSON.writeStream
      .outputMode("update")
      .format("kafka")
      .option("checkpointLocation", "chk/alexey_chernyaev2")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("topic", output_topic_name)
      .trigger(Trigger.ProcessingTime("5 seconds"))
  }
}
