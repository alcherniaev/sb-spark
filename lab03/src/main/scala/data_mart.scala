
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame



object data_mart {
  val hostname = "10.0.0.5"
  val username = "alexey_chernyaev2"
  val password = "fuiBpd2S"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("lab03")
      .getOrCreate()
    spark.conf.set("spark.cassandra.connection.host", hostname)

    val webLogsDF = spark.read.json("/labs/laba03/weblogs.parquet")
    val visitsDF = getElasticSearchData(spark, "visits")
    val clientsDF = getCassandraData(spark, "clients", "labdata")
    val domainCatsDF = getPostgresData(spark, "domain_cats", "labdata")

    val finalDF = getFinalData(spark, webLogsDF, visitsDF, clientsDF, domainCatsDF)
    writeToPostgres(spark, finalDF, "clients", "overwrite", username)
  }

  def getElasticSearchData(spark: SparkSession, tableName: String): DataFrame = {
    val elastic = spark.read
      .format("org.elasticsearch.spark.sql")
      .option("es.nodes.wan.only", "true")
      .option("es.port", "9200")
      .option("es.nodes", hostname)
      .option("es.net.http.auth.user", "alexey.chernyaev2")
    //.option("es.net.http.auth.pass", "")

    elastic.load(tableName)
  }

  def getCassandraData(spark: SparkSession, tableName: String, keySpace: String): DataFrame = {
    val tableOpts = Map("table" -> "clients", "keyspace" -> "labdata")

    spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(tableOpts)
      .load
  }

  def getPostgresData(spark: SparkSession, tableName: String, db: String): DataFrame = {

    val postgreUrl = s"jdbc:postgresql://$hostname:5432" + db + s"?user=${username}&password=${password}"
    spark
      .read
      .format("jdbc")
      .option("url", postgreUrl)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", tableName)
      .load()
  }

  def getFinalData(spark: SparkSession, webLogsDF: DataFrame, visitsDF: DataFrame, clientsDF: DataFrame, domainCatsDF: DataFrame): DataFrame = {
    import spark.implicits._

    val dfLogsExploded = webLogsDF.select('uid, explode('visits).alias("visits"))
    val dfLogsURL = dfLogsExploded.select('uid, dfLogsExploded.col("visits.url"))
    dfLogsURL.createOrReplaceTempView("dfLogsURLSql")
    val dfUserCategory = spark.sql("select distinct uid, parse_url(url, 'HOST') as domain from dfLogsURLSql")


    val dfWebCat = domainCatsDF.join(dfUserCategory, Seq("domain"), "inner")
      .withColumn("category", concat(lit("web_"), 'category))
      .groupBy('uid)
      .pivot('category)
      .agg(count("domain").alias("cnt_web_cat"))
      .na.fill(0)

    val dfAgeGender = clientsDF.withColumn("age_cat",
      when('age.between(18, 24), "18-24")
        .when('age.between(25, 34), "25-34")
        .when('age.between(35, 44), "35-44")
        .when('age.between(45, 54), "45-54")
        .otherwise(">=55"))
      .drop('age)

    val dfShopCat = visitsDF.filter("uid is not null")
      .withColumn("category", lower('category))
      .withColumn("category", regexp_replace('category, "[-]", "_"))
      .withColumn("category", concat(lit("shop_"), 'category))
      .groupBy('uid)
      .pivot('category)
      .agg(count("timestamp").alias("cnt_shop_cat"))
      .na.fill(0)

    //make final table
    dfAgeGender.join(dfShopCat, Seq("uid"), "left")
      .join(dfWebCat, Seq("uid"), "left")


  }

  def writeToPostgres(spark: SparkSession, df: DataFrame, tableDistanceName: String, mode: String, db: String): Unit = {
    val postgreUrl = s"jdbc:postgresql://$hostname:5432" + db + s"?user=${username}&password=${password}"

    df.write
      .mode(mode)
      .format("jdbc")
      .option("url", postgreUrl)
      .option("dbtable", tableDistanceName)
      .option("driver", "org.postgresql.Driver")
      .save()
  }
}
