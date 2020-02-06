import tabmo_project.cleaningData.{Spark, getListOfFiles, readData}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

val spark:SparkSession = Spark(numPartitions = 4)
spark.conf.set("spark.sql.shuffle.partitions", 4)

// import implicit in order to use $ operator
import spark.implicits._
// Getting the list of files to read
val listOfFiles:List[String] = getListOfFiles(dir = "C:\\Users\\joaka\\Documents\\Master MIASHS 1\\" +
  "TER\\terWork\\Data\\ter_tbo")


// Reading data from file(s)
val df:DataFrame = readData(spark, listOfFiles(0),form= "csv")


// remove "event" character from variable names
val columnsNames:List[String] = df.columns.toList
val newColumnsNames:List[String] = columnsNames.map(v => v.replace("event_",""))

// Select win & click auctions and remove useless variables
val newDF: DataFrame = df.toDF(newColumnsNames: _*)
  .filter(condition =($"type" === "win" || $"type" === "click"))
  .drop(colNames ="date","geo_name","city", "advertiser_id","line_id","zip_code","creative_size","deal_cpm",
    "device_language","imp_multiplier","hash_ip","media_type","device_model","creative_type","hash_ifa",
    "app_site_name")



// Get the first number of version
val map = Map("os_version" -> "unknown")

val testOsVersion:DataFrame = newDF
  .withColumn(colName="os_version",when($"os_version".isNotNull, value = substring_index(str=$"os_version",delim=".",count=1))
    .otherwise(value="unknown"))
  .withColumn(colName="fullOsInfo",concat(exprs=$"os", lit(literal="_"), $"os_version"))


newDF
  .withColumn(colName="timestamp", unix_timestamp(col(colName="timestamp"),
      "yyyy-MM-dd HH:mm:ss").cast(to="timestamp"))
  .withColumn(colName="schedule_start", unix_timestamp(col(colName="schedule_start"),
      "yyyy-MM-dd HH:mm:ss").cast(to="timestamp"))
  .withColumn(colName="schedule_end", unix_timestamp(col(colName="schedule_end"),
      "yyyy-MM-dd HH:mm:ss").cast(to="timestamp"))
  .withColumn(colName="second", second(col(colName="timestamp")))
  .withColumn(colName="minute", minute(col(colName="timestamp")))
  .withColumn(colName="Hour", hour(col(colName="timestamp")))
  .withColumn(colName="DayOfWeek", dayofweek(col(colName="timestamp")))
  .withColumn(colName="DayOfMonth", dayofmonth(col(colName="timestamp")))
  .withColumn(colName="Month", month( col(colName="timestamp")))
  .withColumn(colName="Month", month(col(colName="timestamp")))
  .withColumn(colName = "timeFromStart", datediff(end=$"timestamp",start=$"schedule_start"))
  .withColumn(colName = "timeBeforeEnd", datediff(end=$"schedule_end",start=$"timestamp"))
  .withColumn(colName = "timeDuration", datediff(end=$"schedule_end",start=$"schedule_start"))
  .drop(colNames="timestamp","schedule_start","schedule_end")
  .show()
