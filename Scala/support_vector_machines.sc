import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.feature.{LabeledPoint, StringIndexer}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import tabmo_project.cleaningData.{Spark, readData}
val spark: SparkSession = Spark()

/*

val data: DataFrame = readData(spark, fileToRead="/home/joseph/IdeaProjects/data_science/ressources/data_one_week_cleaned.csv", form="csv")
  .drop(colNames="auction_id","Timestamp", "win_price","win_price_loc")

val indexes = data.columns.map { colName =>
  new StringIndexer()
    .setInputCol(colName)
    .setOutputCol(colName + "Index")
    .fit(data)
}

val pipeline = new Pipeline().setStages(indexes)
val df_r: DataFrame = pipeline.fit(data).transform(data)
val filteredDF: DataFrame = df_r.select(df_r.columns.filter(_ .contains("Index"))
  .map(colName => new Column(colName)): _*)

val filteredRdd: RDD[Row] = filteredDF.rdd
val features = filteredDF.columns

val d: RDD[LabeledPoint] = filteredRdd.map(line => {
  val label = line.getAs[Double](fieldName = "clickIndex")
  val feats = line.getValuesMap(features.filterNot(_.contains("click"))).map(_._2.toString.toDouble).toArray
  LabeledPoint(label = label, features = org.apache.spark.ml.linalg.Vectors.dense(feats))
})

val svm_df: DataFrame =  spark.createDataFrame(d).toDF(colNames ="label","features")

MLUtils.convertVectorColumnsToML(svm_df).coalesce(numPartitions = 1)
  .write.format(source="libsvm")
  .save(path="/home/joseph/IdeaProjects/data_science/ressources/data_in_svm_format")
*/


val training: DataFrame = spark.read.format("libsvm").load("/home/joseph/IdeaProjects/data_science/ressources/data_in_svm_format/data.libsvm")

val lsvc = new LinearSVC()
  .setMaxIter(20)
  .setRegParam(0.1)
  .setTol(0.00005)
  .setStandardization(true)
  .setLabelCol("label")
  .setFeaturesCol("features")

// Fit the model
val lsvcModel = lsvc.fit(training)

val predictionAndLabels = training.rdd .map (row => {
  val prediction = lsvcModel.predict(row.getAs[org.apache.spark.ml.linalg.Vector](fieldName="features"))
  (prediction, row.getAs[Double](fieldName="label"))
})


val metrics = new MulticlassMetrics(predictionAndLabels)
val accuracy = metrics.accuracy
val confusion_matrix = metrics.confusionMatrix
