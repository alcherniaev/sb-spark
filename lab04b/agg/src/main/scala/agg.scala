import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.{col, from_json, lit, struct, sum, to_json, when, window}
import org.apache.spark.sql.streaming.Trigger


object agg {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder
      .appName("lab04b")
      .getOrCreate()

    import spark.implicits._

    val initialDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribe", "alexey_chernyaev2")
      .load()

    val df = initialDF.select($"value".cast("string").as[String])

    val schema = StructType(Seq(
      StructField("category", StringType, true),
      StructField("event_type", StringType, true),
      StructField("item_id", StringType, true),
      StructField("item_price", StringType, true),
      StructField("timestamp", LongType, true),
      StructField("uid", StringType, true)
    ))

    val processedDF = df.withColumn("jsonData", from_json(col("value"), schema)).select("jsonData.*")
                        .withColumn("date", ($"timestamp" / 1000).cast(TimestampType))

    val dfW = processedDF.groupBy(window(col("date"), "1 hour")).agg(
      sum(when($"event_type" === "buy", col("item_price")).otherwise(0)).alias("revenue"),
      sum(when($"uid".isNotNull, 1).otherwise(0)).alias("visitors"),
      sum(when($"event_type" === "buy", 1).otherwise(0)).alias("purchases"))
      .withColumn("aov", $"revenue" / $"purchases")
      .withColumn("start_ts", $"window.start".cast("long"))
      .withColumn("end_ts", $"window.end".cast("long")).drop(col("window"))

    val query = dfW
      .selectExpr("CAST(start_ts AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .format("kafka")
      .option("checkpointLocation", "lab04b_checkpoint_alexey_chernyaev2")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("topic", "alexey_chernyaev2_lab04b_out")

      .option("maxOffsetsPerTrigger", 200)
      .outputMode("update")
      .start()

    query.awaitTermination()

//    dfW.writeStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
//      .option("topic", "updates")
//      .start()

//    dfW
//      .select($"start_ts".cast("string").alias("key"), to_json(struct("*")).alias("value"))
//      .writeStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "spark-master-1:6667")
///      .option("topic", "alexey_chernyaev2_lab04b_out")
//      .option("checkpointLocation", "/user/alexey.chernyaev2/lab04b_checkpoint")
//      .outputMode("update")
//      .trigger(Trigger.ProcessingTime("5 seconds"))
//      .start()


//    dfW
//      .selectExpr("CAST(start_ts AS STRING) AS key", "to_json(struct(*)) AS value")
//     .writeStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "spark-master-1:6667")
//      .option("topic", "alexey_chernyaev2_lab04b_out")
//      .option("checkpointLocation", "lab04b_checkpoint_alexey_chernyaev2")
//      .outputMode("update")
//      .start()

  }
}
