package tabmo_project
import cleaningData.{Spark, concatAllData, getListOfFiles}
import org.apache.spark.sql.{DataFrame, SparkSession}

object exportFullData {
  def main(args: Array[String]): Unit = {
    val spark:SparkSession = Spark(numPartitions = 280)
    val listOfFiles:List[String] = getListOfFiles(dir = "/home/joseph/IdeaProjects/data_science/ressources")
    val df:DataFrame = concatAllData(spark, listOfFiles)
    // val df:DataFrame = readData(spark,listOfFiles(0),form="csv")

    df.coalesce(numPartitions = 1)
      .write.format(source="csv")
      .option("compression","gzip")
      .option("header","true")
      .csv(path="/home/joseph/Bureau/Master/Ter/data")
  }
}

