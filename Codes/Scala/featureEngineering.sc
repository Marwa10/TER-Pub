import cleaningData.{Spark, fullCleaningData}
import org.apache.spark.sql.functions._
import featureEngineering.iabTransformation
import org.apache.spark.sql.{DataFrame, SparkSession}

val spark:SparkSession = Spark(numPartitions = 4)
// import implicit in order to use $ operator
import spark.implicits._
val df:DataFrame = fullCleaningData(spark)
// IAB vectorization
val finalDataIAB:DataFrame = iabTransformation(spark, df)

// Add day, month, hour
val finalDataDate:DataFrame = finalDataIAB
  .withColumn(colName="timestamp", unix_timestamp(col(colName="timestamp"),
    "yyyy-MM-dd HH:mm:ss").cast(to="timestamp"))
  .withColumn(colName="schedule_start", unix_timestamp(col(colName="schedule_start"),
    "yyyy-MM-dd HH:mm:ss").cast(to="timestamp"))
  .withColumn(colName="schedule_end", unix_timestamp(col(colName="schedule_end"),
    "yyyy-MM-dd HH:mm:ss").cast(to="timestamxp"))
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


// Get the first number of version
val finalDataOsInfo:DataFrame = finalDataDate
  .withColumn(colName="os_version",when($"os_version".isNotNull, value = substring_index(str=$"os_version",delim=".",count=1))
    .otherwise(value="unknown"))
  .withColumn(colName="fullOsInfo",concat(exprs=$"os", lit(literal="_"), $"os_version"))
  .drop(colNames="os","os_version")

finalDataOsInfo.show()