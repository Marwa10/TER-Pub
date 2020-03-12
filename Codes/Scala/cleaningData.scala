package tabmo_project

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.File
import org.apache.spark.sql.functions.{col, when}
import util.Try


object cleaningData {

  def main(args: Array[String]): Unit = {
  }

  def Spark(numPartitions:Int = 4): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(name="TabMoData")
      .config("spark.master", "local[8]")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", numPartitions)
    spark.conf.set("spark.executor.memory",-1)
    spark.conf.set("spark.driver.memory",-1)
    spark.conf.set("spark.sql.broadcastTimeout",-1)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark
  }

  def readData(spark: SparkSession, fileToRead: String, form: String): DataFrame = {
    // Reading the data
    val df:DataFrame = spark.read.format(source=form)
      .option("header","true")
      .load(path = fileToRead)
    df
  }

  def removeColumns(spark: SparkSession,df: DataFrame): DataFrame = {
    // Load implicits module in order to use the $ operator
    val df_temp: DataFrame = df.drop(colName="creative_size")
    import spark.implicits._
    val columnsNames: List[String] = df_temp  .columns.toList
    val newColumnsNames: List[String] = columnsNames.map(v => v.replace("event_",""))

    // Dropping useless columns
    val output_df: DataFrame = df_temp.toDF(newColumnsNames: _*)
      .filter(condition = $"type" === "win" || $"type" === "click")
      .drop(colNames ="date","geo_name","city", "advertiser_id","line_id","zip_code","deal_cpm",
        "imp_multiplier","hash_ip","media_type","device_model","hash_ifa",
        "app_site_name")
    output_df
  }

  def mappingNaValues(df: DataFrame): DataFrame = {
    // Mapping null values for boolean
    val map = Map("click" -> "0",
      "Device_language" -> "unknown",
      "connection_type" -> "unknown"//,
    //  "hash_deal_identifier" ->"0"
    )

    val output_df: DataFrame = df.na.fill(map)
    output_df
  }

  def oneRowPerAuction(spark: SparkSession,df: DataFrame): DataFrame = {
    // Load implicits module in order to use the $ operator
    import spark.implicits._
    // Pivoting data to get a unique row per auction id
    val pivotDF: DataFrame= df
      .groupBy(cols= $"auction_id")
      .pivot(pivotColumn="type")
      .count()

    //Merging data in order to get the information about auction
    val output_df: DataFrame = pivotDF.filter(condition = $"win" === 1 )
      .drop(colName="win")
      .join(df,Seq("auction_id"),joinType="left")
      .filter(condition= $"type" ==="win")
      .drop(colNames="win","type")
      //.withColumn(colName = "hash_deal_identifier",when($"hash_deal_identifier".isNotNull ,value=1)
      //  .otherwise(col(colName="hash_deal_identifier"))
      //)
    output_df
  }

  def getListOfFiles(dir: String): List[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile)
              .filter(_.getName.contains("ter"))
              .map(_.getPath)
              .toList
    } else {
        List[String]()
      }
  }

  def renameFile(oldName: String, newName: String): Unit = {
    Try(new File(oldName).renameTo(new File(newName))).getOrElse(false)
  }

  def csvToGzip(listOfFiles: List[String]): Unit = {
    for (i <- listOfFiles){
      renameFile(i,i.replace("csv","gz"))
    }
  }

  def concatAllData(spark: SparkSession, listOfFiles: List[String]): DataFrame = {
    var i = 1
    var df: DataFrame = readData(spark, listOfFiles.head,form="csv")
    for (currentFile <- listOfFiles.slice(1,listOfFiles.length)){
      println("Ajout du tableau : " + i)
      val temp: DataFrame = df.union(readData(spark, currentFile,form="csv"))
      df = temp
      println("Number of lines : " + df.count())
      i += 1
    }
    df
  }

  def cleanData(spark: SparkSession, fileToRead: String = "/home/joseph/IdeaProjects/data_science/ressources/df_tabmo_oneWeek.csv", form: String = "csv"): DataFrame = {
    // val listOfFiles: List[String] = getListOfFiles(dir = "/home/joseph/IdeaProjects/data_science/ressources")
    val new_df: DataFrame = readData(spark,fileToRead, form= form)
    val new_df_cleaning: DataFrame = removeColumns(spark,new_df)
    val new_df_one_row_per_id: DataFrame = oneRowPerAuction(spark: SparkSession, new_df_cleaning)
    val output_df: DataFrame = mappingNaValues(new_df_one_row_per_id)
    output_df
  }

}




