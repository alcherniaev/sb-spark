import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}


object filter {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("lab04a")
      .getOrCreate()

    import spark.implicits._

    val topic = spark.conf.get("spark.filter.topic_name")
    var offset = spark.conf.get("spark.filter.offset")
    val path = spark.conf.get("spark.filter.output_dir_prefix")

//    if (offset != "earliest") {
//      offset = s"""{"${topic}":{"0":${offset}}}"""
//    }

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    val df_initial = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribe", topic)
      .option("startingOffset", offset)
      .option("endingOffsets", "latest")
      //.option("checkpointLocation", "s/tmp/chk/$chkName")
      .load()

    val df = df_initial.select($"value".cast("string").as[String])

    val schema = StructType(Seq(
      StructField("category", StringType, true),
      StructField("event_type", StringType, true),
      StructField("item_id", StringType, true),
      StructField("item_price", StringType, true),
      StructField("timestamp", LongType, true),
      StructField("uid", StringType, true)
    ))
    val processedDF = df
      .withColumn("jsonData", from_json($"value", schema)).select("jsonData.*")
      .withColumn("date", date_format(($"timestamp" / 1000).cast(TimestampType), "YYYYMMDD"))
      .withColumn("date_part", $"date")

    val ViewDF = processedDF.filter($"event_type" === "view")
    val BuyDF = processedDF.filter($"event_type" === "buy")

    ViewDF.write.format("json").mode("overwrite").partitionBy("date_part").save(path + "/view/")
    BuyDF.write.format("json").mode("overwrite").partitionBy("date_part").save(path + "/buy/")

  }


}
