package tabmo_project

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.File
import org.apache.spark.sql.functions.{col, when}
import util.Try

object cleaningData {

  def main(args: Array[String]):Unit = {}

  def Spark(numPartitions:Int = 8):SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(name="TabMoData")
      .config("spark.master", "local")
      .getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", numPartitions)
    spark
  }

  def readData(spark:SparkSession, fileToRead:String, form:String):DataFrame = {
    // Reading the data
    val df:DataFrame = spark.read.format(source=form)
      .option("header","true")
      .load(path = fileToRead)
    df
  }

  def removeColumns(spark:SparkSession,df:DataFrame):DataFrame = {
    // Load implicits module in order to use the $ operator
    import spark.implicits._
    val columnsNames:List[String] = df.columns.toList
    val newColumnsNames:List[String] = columnsNames.map(v => v.replace("event_",""))

    // Dropping useless columns
    val outputDF: DataFrame = df.toDF(newColumnsNames: _*)
      .filter(condition = $"type" === "win" || $"type" === "click")
      .drop(colNames ="date","geo_name","city", "advertiser_id","line_id","zip_code","creative_size","deal_cpm",
        "imp_multiplier","hash_ip","media_type","device_model","hash_ifa",
        "app_site_name")
    outputDF
  }

  def oneRowPerAuction(spark:SparkSession,df:DataFrame):DataFrame = {
    // Load implicits module in order to use the $ operator
    import spark.implicits._

    // Pivoting data to get a unique row per auction id
    val pivotDF:DataFrame= df
      .groupBy(cols= $"auction_id")
      .pivot(pivotColumn="type")
      .count()

    // Mapping null values for boolean
    val map = Map("click" -> "0",
      "connection_type" -> "unknown",
      "deal_identifier" ->"0")

    //Merging data in order to get the information about auction
    val outputDF:DataFrame = pivotDF.filter(condition = $"win" === 1 )
      .drop(colName="win")
      .join(df,Seq("auction_id"),joinType="left")
      .filter(condition= $"type" ==="win")
      .drop(colNames="win","type")
      .na.fill(map)
      .withColumn(colName = "deal_identifier",when($"deal_identifier".isNotNull ,value=1)
        .otherwise(col(colName="deal_identifier"))
      )
    outputDF
  }

  def getListOfFiles(dir:String):List[String] = {
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

  def renameFile(oldName:String, newName:String):Unit = {
    Try(new File(oldName).renameTo(new File(newName))).getOrElse(false)
  }

  def csvToGzip(listOfFiles:List[String]):Unit = {
    for (i <- listOfFiles){
      renameFile(i,i.replace("csv","gz"))
    }
  }

  def concatAllData(spark:SparkSession, listOfFiles:List[String]):DataFrame = {
    var i = 1
    var df:DataFrame = readData(spark, listOfFiles.head,form="csv")
    for (currentFile <- listOfFiles.slice(1,listOfFiles.length)){
      println("Ajout du tableau : " + i)
      val temp : DataFrame = df.union(readData(spark, currentFile,form="csv"))
      df = temp
      println("Number of lines : " + df.count())
      i += 1
    }
    df
  }

  def fullCleaningData(spark:SparkSession):DataFrame = {
    val listOfFiles:List[String] = getListOfFiles(dir = "/home/joseph/IdeaProjects/data_science/ressources")
    val newDF:DataFrame = readData(spark, listOfFiles.head,form= "csv")
    val finalDataOneRow:DataFrame = oneRowPerAuction(spark,newDF)
    finalDataOneRow
  }

}




