import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import tabmo_project.cleaningData.{Spark, readData}
val spark: SparkSession = Spark()
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.SingularValueDecomposition
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.RowMatrix


val data: DataFrame = readData(spark, fileToRead="/home/joseph/IdeaProjects/data_science/ressources/data_one_week_cleaned.csv", form="csv")
    .drop("auction_id","Timestamp", "win_price","win_price_loc")

val indexes = data.columns.map { colName =>
  new StringIndexer()
    .setInputCol(colName)
    .setOutputCol(colName + "Index")
    .fit(data)
}

val pipeline = new Pipeline().setStages(indexes)
val df_r: DataFrame = pipeline.fit(data).transform(data)
val filteredDF: DataFrame = df_r.select(df_r.columns .filter(_ .contains("Index")).map(colName => new Column(colName)): _*)

val rows = filteredDF .rdd.map(row => {
  val values = row.getValuesMap(filteredDF.columns.filterNot(_ .contains("click"))).map(_._2.toString.toDouble).toArray
    Vectors.dense(values)
})


val mat: RowMatrix = new RowMatrix(rows)

// Compute the top 5 singular values and corresponding singular vectors.
val svd: SingularValueDecomposition[RowMatrix, Matrix] = mat.computeSVD(5, computeU = true)
val U: RowMatrix = svd.U  // The U factor is a RowMatrix.
val s: Vector = svd.s     // The singular values are stored in a local dense vector.
val V: Matrix = svd.V     // The V factor is a local dense matrix.

val collect = U.rows.collect()
println("U factor is:")
collect.foreach { vector => println(vector) }
println(s"Singular values are: $s")
println(s"V factor is:\n$V")


/*
  val lsvc = new LinearSVC()
    .setMaxIter(10)
    .setRegParam(0.1)

  // Fit the model
  val lsvcModel = lsvc.fit(filteredDF)

  // Print the coefficients and intercept for linear svc
  println(s"Coefficients: ${lsvcModel.coefficients} Intercept: ${lsvcModel.intercept}")
*/