package tabmo_project
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, posexplode, regexp_replace, split, substring, when}

object featureEngineering {
  def main(args: Array[String]): Unit = {}

  def iabTransformation(spark:SparkSession, finalDataOneRow:DataFrame):DataFrame = {
    // Load implicits module in order to use the $ operator
    import spark.implicits._

    val iabDF:DataFrame = finalDataOneRow.select(cols = $"auction_id",posexplode(split(str = $"site_or_app_categories", pattern = ",")))
      .select(cols=$"auction_id", substring(col(colName="col"),pos=0,len=5).alias("iabValues"))
      .withColumn(colName = "iabValues",regexp_replace($"iabValues","-",""))
      .groupBy(col1="auction_id")
      .pivot(pivotColumn="iabValues")
      .count()
    val iabDF2:DataFrame = iabDF.na.fill(value=0,iabDF.columns)
    val tempIABDF:DataFrame = iabDF2.columns.slice(1,iabDF2.columns.length).foldLeft(iabDF2){
      (data ,columnName) =>
        data.withColumn(columnName, when(col(columnName) > 1, value=1).otherwise(col(columnName)))}

    // Merging data in order to get all informations about app & sites
    val finalDataIAB:DataFrame = finalDataOneRow
      .drop(colName="site_or_app_categories")
      .join(tempIABDF,Seq("auction_id"), joinType = "left")

    return finalDataIAB
  }
}
