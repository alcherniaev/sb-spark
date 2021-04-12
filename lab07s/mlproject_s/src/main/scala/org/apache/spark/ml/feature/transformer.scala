package org.apache.spark.ml.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

class Url2DomainTransformer(override val uid: String) extends Transformer
  with DefaultParamsWritable
{

  def this() = this(Identifiable.randomUID("org.apache.spark.ml.feature.Url2DomainTransformer"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    try {
      dataset.
        withColumn("visits", explode(col("visits"))).
        withColumn("url", col("visits.url")).
        drop(col("visits")).
        withColumn("url", lower(callUDF("parse_url", col("url"), lit("HOST")))).
        withColumn("url", regexp_replace(col("url"), "www.", "")).
        withColumn("url", regexp_replace(col("url"), "[.]", "-")).
        groupBy(col("gender_age"), col("uid")).agg(collect_list(col("url")).alias("domains"))

    } catch {
      case _: Throwable => {
        dataset.
          withColumn("visits", explode(col("visits"))).
          withColumn("url", col("visits.url")).
          drop(col("visits")).
          withColumn("url", lower(callUDF("parse_url", col("url"), lit("HOST")))).
          withColumn("url", regexp_replace(col("url"), "www.", "")).
          withColumn("url", regexp_replace(col("url"), "[.]", "-")).
          groupBy(col("uid")).agg(collect_list(col("url")).alias("domains"))

      }
    }
  }

  override def copy(extra: ParamMap): Url2DomainTransformer = defaultCopy(extra)
  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(StructField("gender_age",StringType,true),
      StructField("uid",StringType,true),
      StructField("domains",ArrayType(StringType,true),true)))


  }
}

object Url2DomainTransformer extends DefaultParamsReadable[Url2DomainTransformer] {
  override def load(path: String): Url2DomainTransformer = super.load(path)
}