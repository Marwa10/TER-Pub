package tabmo_project

import org.apache.spark.sql.{DataFrame, SparkSession}
import tabmo_project.cleaningData.{Spark, cleanData, getListOfFiles}
import org.apache.spark.sql.functions.when

object Sampling {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession =  Spark(numPartitions = 4)
    import spark.implicits._

    val listOfFiles: List[String] = getListOfFiles(dir="/media/joseph/OS/Users/joaka/Documents/Master MIASHS 1/" +
      "TER/terWork/Data/ter_tbo")

    val df: DataFrame = cleanData(spark=spark, fileToRead = listOfFiles(0)).where(condition = $"click".>=(1))

    val output_df: DataFrame = getClickedData(spark = spark, df= df, listOfFiles = listOfFiles).cache()

    output_df
      .withColumn(colName="click", when($"click".>(other=1),value = 1).otherwise(value=$"click"))
      .write.format(source="csv")
      //.option("compression","gzip")
      .option("header","true")
      .csv(path="/media/joseph/OS/Users/joaka/Documents/Master MIASHS 1/TER/terWork/Data/" +
        "exportedData/clicked_data_one_month")
  }

  def getClickedData(spark: SparkSession, i: Int = 1, df: DataFrame, listOfFiles: List[String]): DataFrame = {
    import spark.implicits._
    val df_cleaned: DataFrame = cleanData(spark, listOfFiles(i))
    val df_cleaned_click = df_cleaned.where(condition = $"click" === 1)
    val df_cleaned_no_click = df_cleaned.where(condition = $"click" === 0).sample(fraction=1D*0.00212121)
    if (i.equals(listOfFiles.length-1)) {
      return df.union(df_cleaned_click.union(df_cleaned_no_click))
    } else {
      val output_df: DataFrame = df.union(df_cleaned_click.union(df_cleaned_no_click))

        return getClickedData(i = i + 1, df = output_df, spark = spark, listOfFiles = listOfFiles)
    }
    }

}
