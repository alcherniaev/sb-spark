import org.apache.spark.sql.functions._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import java.io.File


object users_items extends App {
  val spark = SparkSession.builder().appName("users_items").getOrCreate()
  import spark.implicits._

  spark.conf.set("spark.sql.session.timeZone", "UTC")

  val update = spark.conf.get("spark.users_items.update")
  val output_dir = spark.conf.get("spark.users_items.output_dir")
  val input_dir = spark.conf.get("spark.users_items.input_dir")

  val dfView = spark.read.json(input_dir + "/view")
  val dfBuy = spark.read.json(input_dir + "/buy")
  val dfVisit = dfView.union(dfBuy)

  val maxDate = dfVisit.agg(max("date")).as[String].collect
  val outputPath = output_dir + "/" + maxDate(0)

  val matrix = dfVisit
    .select('uid, 'item_id, 'event_type)
    .filter('uid.isNotNull)
    .withColumn("item_id", lower('item_id))
    .withColumn("item_id", regexp_replace('item_id, "[/ /-]", "_"))
    .withColumn("item_id", when('event_type === "buy", concat(lit("buy_"), 'item_id)).otherwise(concat(lit("view_"), 'item_id)))
    .groupBy('uid)
    .pivot('item_id)
    .agg(count("uid").alias("cnt_uid"))
    .na.fill(0)

  if (update.toInt == 1) {
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

      print("$$$$$$$")
      print(output_dir.substring(7))
      println("$$$$$$$")

      print("$$$$$$$")
      print(new File(output_dir.substring(7)).listFiles)
      print("$$$$$$$")

      val maxFolder =
        if (output_dir.startsWith("file")) {
          new File(output_dir.substring(7)).listFiles.toList
            .map(_.toString)
            .map(_.split("users-items/")(1))
            .filter(_.head.isDigit)
            .map(_.toInt)
            .reduceLeft(_ max _)
        }
        else {
          fs.listStatus(new Path(s"${output_dir}"))
            .map(_.getPath)
            .map(_.toString)
            .map(_.split("users-items/")(1))
            .map(_.toInt)
            .reduceLeft(_ max _)
        }

      val pathToLastVersion = output_dir + "/" + maxFolder

      val lastDF = spark.read.parquet(pathToLastVersion)

      val cols1 = matrix.columns.toSet
      val cols2 = lastDF.columns.toSet
      val total = cols1 ++ cols2

      val finalDF = lastDF.select(addColumns(cols2, total):_*).union(matrix.select(addColumns(cols1, total):_*))

      finalDF
        .write
        .mode("overwrite")
        .parquet(outputPath)
    }
  else {
      matrix
        .write
        .mode("overwrite")
        .parquet(outputPath)
  }

  def addColumns(myCols: Set[String], allCols: Set[String]) = {
    allCols.toList.map(x => x match {
      case x if myCols.contains(x) => col(x)
      case _ => lit(null).as(x)
    })
  }

}
