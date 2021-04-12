package org.apache.spark.ml.feature

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.hadoop.fs.Path
import org.apache.spark.ml.feature.SklearnEstimatorModel.SklearnEstimatorModelWriter
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.{ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsReader, DefaultParamsWritable, DefaultParamsWriter, Identifiable, MLReadable, MLReader, MLWritable, MLWriter}
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Dataset}

class SklearnEstimator(override val uid: String) extends Estimator[SklearnEstimatorModel]
  with DefaultParamsWritable
{
  override def fit(dataset: Dataset[_]): SklearnEstimatorModel = {

    val trainPy = "#!/opt/anaconda/envs/bd9/bin/python3\n\nimport numpy as np\nimport pandas as pd\nimport pickle\nimport sys\nimport base64\nimport re\n\nfrom sklearn.linear_model import LogisticRegression\n\nrows = [] #here we keep input data to Dataframe constructor\n\nfor line in sys.stdin:\n    line = line.replace('[', '')\n    line = line.replace(']', '')\n    line = line.replace('\\n', '')\n    line = re.split('[,]', line)\n\n    line_dict = {}\n    for i, value in enumerate(line):\n        if i != len(line) - 1:\n            name = \"features_\" + str(i)\n        else:\n            name = \"label\"\n        line_dict[name] = value\n    rows.append(line_dict)\n\n#initialize a dataframe from the list\ndf = pd.DataFrame(rows)\n\nfeature_columns = []\nfor i in range(0, len(df.columns) - 1):\n    feature_columns.append(\"features_\" + str(i))\nlabel_column = \"label\"\n\nmodel = LogisticRegression()\nmodel.fit(df[feature_columns], df[label_column])\nmodel_string = base64.b64encode(pickle.dumps(model)).decode('utf-8')\n\n# Output to stdin, so that rdd.pipe() can return the string to pipedRdd.\nprint(model_string)"
    val file = new File("train.py")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(trainPy)
    bw.close()
    import dataset.sqlContext.implicits._

    var df = dataset
    val vecToSeq = udf((v: Vector) => v.toArray).asNondeterministic
    df = df.select(df("label"), vecToSeq(df("features")).alias("features"))
    val size = df.withColumn("size", org.apache.spark.sql.functions.size(col("features")))
    val sizeNum = size.select(size("size")).collect
    val sizeNumInt = sizeNum(0)(0).asInstanceOf[Int]

    def getColAtIndex(id:Int): Column = col(s"features")(id).as(s"features_${id+1}")
    val columns: IndexedSeq[Column] = (0 to sizeNumInt).map(getColAtIndex) :+ col("label") //Here, instead of 2, you can give the value of n
    df = df.select(columns: _*)
    df = df.na.fill(0)
    df = df.select(df("features_1"), df("features_2"), df("features_3"), df("features_4"), df("features_5"), df("label"))
    val rddModel = df.rdd.pipe(s"./train.py")
    val modelDF = rddModel.toDF()
    val text = modelDF.select(modelDF("value")).collect
    val textStr = text(0)(0).asInstanceOf[String]

    val model = new SklearnEstimatorModel(uid, textStr)

    model
  }

  override def copy(extra: ParamMap): SklearnEstimator = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(StructField("preds",DoubleType,true)))
  }

  def this() = this(Identifiable.randomUID("SklearnEstimator"))
}

object SklearnEstimator extends DefaultParamsReadable[SklearnEstimator] {
  override def load(path: String): SklearnEstimator = super.load(path)
}

class SklearnEstimatorModel(override val uid: String, val model: String) extends Model[SklearnEstimatorModel]
  with MLWritable
{

  override def copy(extra: ParamMap): SklearnEstimatorModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    import java.io._

    import dataset.sqlContext.implicits._

    val textStr = model
    val file = new File("lab07.model")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(textStr)
    bw.close()
    dataset.sparkSession.sparkContext.addFile("lab07.model", true)


    val testPy = "#!/opt/anaconda/envs/bd9/bin/python3\n\nimport numpy as np\nimport pandas as pd\nimport pickle\nimport sys\nimport base64\nimport re\n\nfrom sklearn.linear_model import LogisticRegression\n\npd.set_option('display.max_rows', None)\npd.set_option('display.max_columns', None)\npd.set_option('display.width', None)\npd.set_option('display.max_colwidth', -1)\n\n#read the model, deserialize and unpickle it.\n\nmodel = pickle.loads(\n          base64.b64decode(\n            open(\"lab07.model\").read().encode('utf-8')\n          )\n        )\n\n# print(\"success\")\nrows = [] #here we keep input data to Dataframe constructor\n\n# iterate over standard input\nfor line in sys.stdin:\n    line = line.replace('[', '')\n    line = line.replace(']', '')\n    line = line.replace('\\n', '')\n    line = re.split('[,]', line)\n\n    line_dict = {}\n    for i, value in enumerate(line):\n        if i != len(line) - 1:\n            name = \"features_\" + str(i)\n            line_dict[name] = value\n        else:\n            name = \"uid\"\n            line_dict[name] = value\n\n    rows.append(line_dict)\n\n\n\n#initialize a dataframe from the list\ndf = pd.DataFrame(rows)\nif (len(df) != 0):\n    uid = df['uid']\n    df = df.drop('uid', 1)\n\n    #run inference\n    pred = model.predict(df)\n\n\n    df['preds'] = pred\n    df['uid'] = uid\n    res = df[['uid', 'preds']]\n\n    # Output to stdin, so that rdd.pipe() can return the strings to pipedRdd.\n    #print(df.to_list())\n    print(res)"
    val file2 = new File("test.py")
    val bw2 = new BufferedWriter(new FileWriter(file2))
    bw2.write(testPy)
    bw2.close()


    var df = dataset
    val vecToSeq = udf((v: Vector) => v.toArray).asNondeterministic
    df = df.select(df("uid"), vecToSeq(df("features")).alias("features"))
    val size = df.withColumn("size", org.apache.spark.sql.functions.size(col("features")))
    val sizeNum = size.select(size("size")).collect
    val sizeNumInt = sizeNum(0)(0).asInstanceOf[Int]

    def getColAtIndex(id:Int): Column = col(s"features")(id).as(s"features_${id+1}")
    val columns: IndexedSeq[Column] = (0 to sizeNumInt).map(getColAtIndex) :+ col("uid") //Here, instead of 2, you can give the value of n

    df = df.select(columns: _*)
    df = df.na.fill(0)
    df = df.select(df("features_1"), df("features_2"), df("features_3"), df("features_4"), df("features_5"), df("uid"))
    val rddRes = df.rdd.pipe(s"./test.py")

    var res = rddRes.toDF()
    val split_col = split(res("value"), "[ ]+")

    res = res.withColumn("uid", split_col.getItem(1))
    res = res.withColumn("preds", split_col.getItem(2))
    res = res.filter(res("uid") =!= "uid")
    res = res.select(res("uid"), res("preds").cast(DoubleType))
    res

  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(Seq(StructField("preds",DoubleType,true)))
  }

  override def write: MLWriter = new SklearnEstimatorModelWriter(this)
}

object SklearnEstimatorModel extends MLReadable[SklearnEstimatorModel] {
  private[SklearnEstimatorModel]
  class SklearnEstimatorModelWriter(instance: SklearnEstimatorModel) extends MLWriter {

    private case class Data(model: String)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.model)
      val dataPath = new Path(path, "data").toString
      sparkSession.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class SklearnEstimatorModelReader extends MLReader[SklearnEstimatorModel] {

    private val className = classOf[SklearnEstimatorModel].getName

    override def load(path: String): SklearnEstimatorModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val data = sparkSession.read.parquet(dataPath)
        .select("model")
        .head()
      val modelStr = data.getAs[String](0)
      val model = new SklearnEstimatorModel(metadata.uid, modelStr)
      metadata.getAndSetParams(model)
      model
    }
  }

  override def read: MLReader[SklearnEstimatorModel] = new SklearnEstimatorModelReader

  override def load(path: String): SklearnEstimatorModel = super.load(path)
}