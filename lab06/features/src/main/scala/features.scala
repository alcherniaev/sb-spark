import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.CountVectorizerModel

object features extends App {
  val spark = SparkSession.builder().appName("filter").getOrCreate()
  spark.conf.set("spark.sql.session.timeZone", "UTC")

  import spark.implicits._

  val asArray = udf((v: org.apache.spark.ml.linalg.Vector) => v.toDense.toArray)
  val outputPath = "/user/alexey.chernyaev2/features"
  val wind = Window.partitionBy('uid)

  val dfLogs = spark.read.json("hdfs:///labs/laba03/weblogs.json")
  val dfUsersItems = spark.read.parquet("/user/alexey.chernyaev2/users-items/20200429")

  val dfLogsDomain = dfLogs.select('uid, explode('visits).alias("visit"))
    .withColumn("host", lower(callUDF("parse_url", $"visit.url", lit("HOST"))))
    .withColumn("domain", regexp_replace($"host", "www.", ""))
    .withColumn("timestamp", $"visit.timestamp"/1000)
    .withColumn("hour", from_unixtime($"timestamp", "H"))
    .withColumn("dayOfWeek", concat(lit("web_day_"), lower(date_format('timestamp.cast("timestamp"), "E"))))
    .filter('domain.isNotNull)
    .filter('uid.isNotNull)
    .select('uid, 'domain, 'dayOfWeek, 'hour)

  val dfWorkOrNot = dfLogsDomain
    .withColumn("allVisits", count('domain).over(wind))
    .withColumn("web_fraction_work_hours", sum(when('hour >= lit(9) && 'hour < lit(18), 1).otherwise(0)).over(wind) / 'allVisits)
    .withColumn("web_fraction_evening_hours", sum(when('hour >= lit(18) && 'hour < lit(24), 1).otherwise(0)).over(wind) / 'allVisits)
    .select('uid, 'web_fraction_work_hours, 'web_fraction_evening_hours).distinct()

  val dfDayOfWeek = dfLogsDomain
    .groupBy('uid)
    .pivot('dayOfWeek).agg(count("domain").alias("cnt_domain"))
    .na.fill(0)

  val dfHour = dfLogsDomain
    .withColumn("hour", concat(lit("web_hour_"), 'hour))
    .groupBy('uid)
    .pivot('hour).agg(count("domain").alias("cnt_domain"))
    .na.fill(0)

  val Vocabulary = dfLogsDomain
    .groupBy('domain)
    .agg(count('uid).alias("countVisits"))
    .orderBy(desc("countVisits"), asc("domain"))
    .limit(1000)
    .orderBy(asc("domain"))
    .select('domain).as[String].collect

  val dfForVector = dfLogsDomain
    .groupBy('uid).agg(collect_list('domain).alias("domains"))

  val cvm = new CountVectorizerModel(Vocabulary)
    .setInputCol("domains")
    .setOutputCol("domain_features")

  val dfVector = cvm.transform(dfForVector)
    .withColumn("domain_features", asArray('domain_features))
    .drop('domains)

  val dfPart1 = dfWorkOrNot.join(dfDayOfWeek, Seq("uid"), "inner")
    .join(dfHour, Seq("uid"), "inner")
    .join(dfVector, Seq("uid"), "inner")

  val dfFinal = dfPart1.join(dfUsersItems, Seq("uid"), "outer")

  dfFinal
    .write
    .mode("overwrite")
    .parquet(outputPath)
}
