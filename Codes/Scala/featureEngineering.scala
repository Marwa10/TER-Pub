package tabmo_project

import java.text.SimpleDateFormat
import net.liftweb.json.parse
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, posexplode, regexp_replace, split, substring, when, _}
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import tabmo_project.cleaningData.{Spark, cleanData}
import java.net.{HttpURLConnection, URL}

object featureEngineering {
  def main(args: Array[String]): Unit = {
    // val lines:Int= 1000
    val spark:SparkSession = Spark()
    // import implicit in order to use $ operator
    import spark.implicits._
    // Clean the input data
    val df:DataFrame = cleanData(spark).repartition(numPartitions = 4)
      .filter(condition=$"creative_type" === "banner")
      .cache()
    val df_temp:DataFrame = apiPreparation(spark, df)

    // IAB vectorization
    val rdd:RDD[Row] = getApisDataRDD(spark,df_temp)

    // a merger ensemble
    val df_sortie: DataFrame = fromRDDtoDF(spark, rdd)
    val df_os_info: DataFrame = getFullOsInfo(spark, df)
    val df_iab: DataFrame = getIabTranspose(spark, df)
    val df_other_features: DataFrame = df.select(cols=$"auction_id", $"click", $"exchange",
      $"app_or_site",$"has_gps", $"device_type", $"connection_type", $"creative_size", $"has_ifa",$"win_price",$"win_price_loc",
      $"bidder_name",$"schedule_start",$"schedule_end")


    // Join all the features
    val df_joined_data:DataFrame = df_other_features
      .join(df_sortie,Seq("auction_id"))
      .join(df_iab,Seq("auction_id"))
      .join(df_os_info,Seq("auction_id"))
    // Final operation on date data
    val output_df: DataFrame = getDateFeatures(spark, df_joined_data)
    val t0 = System.nanoTime()
    output_df.coalesce(numPartitions = 4)
      .write.format(source="csv")
      .option("header","true")
      .mode("overWrite")
      .csv(path="/home/joseph/Bureau/Master/Ter/data/final_example_data.csv")

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/ 1000000000  + "s")
  }

  def get(url: String,
          connectTimeout: Int = 30000,
          readTimeout: Int =  30000,
          requestMethod: String = "GET"):String=
  {
    val connection = new URL(url).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(connectTimeout)
    connection.setReadTimeout(readTimeout)
    connection.setRequestMethod(requestMethod)
    val inputStream = connection.getInputStream
    val content = scala.io.Source.fromInputStream(inputStream).mkString
    if (inputStream != null) inputStream.close()
    content
  }

  @annotation.tailrec
  def recursiveAPIRequest(url_link: String): String = {
    try
      {
          val result:String = get(url_link)
          result
      }
    catch
      {
        case _: java.io.IOException =>
          recursiveAPIRequest(url_link)
      }
  }


  def getIabTranspose(spark: SparkSession, df: DataFrame): DataFrame = {
    // Load implicits module in order to use the $ operator
    import spark.implicits._
    val iab_df: DataFrame = df.select(cols = $"auction_id",posexplode(split(str = $"site_or_app_categories", pattern = ",")))
      .select(cols=$"auction_id", substring(col(colName="col"),pos=0,len=5).alias("iabValues"))
      .withColumn(colName = "iabValues",regexp_replace($"iabValues","-",""))
      .groupBy(col1="auction_id")
      .pivot(pivotColumn="iabValues")
      .count()
    val iab_df_clean: DataFrame = iab_df.na.fill(value=0,iab_df.columns)
    val output_df: DataFrame = iab_df_clean.columns.slice(1,iab_df_clean.columns.length).foldLeft(iab_df_clean){
      (data ,columnName) =>
        data.withColumn(columnName, when(col(columnName) > 1, value=1).otherwise(col(columnName)))}

    output_df
  }

  def apiPreparation(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val output_df: DataFrame = df.select(cols=$"auction_id", $"longitude", $"latitude", $"timestamp",
      $"device_language")
      .withColumn(colName="timestamp", unix_timestamp(col(colName="timestamp")))
    output_df
  }

  def getDateFeatures(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val output_df: DataFrame = df.withColumn(colName="schedule_start", unix_timestamp(col(colName="schedule_start"),
      "yyyy-MM-dd HH:mm:ss").cast(to="timestamp"))
      .withColumn(colName="schedule_end", unix_timestamp(col(colName="schedule_end"),
        "yyyy-MM-dd HH:mm:ss").cast(to="timestamp"))
      .withColumn(colName="Timestamp_second", second(col(colName="Timestamp")))
      .withColumn(colName="Timestamp_minute", minute(col(colName="Timestamp")))
      .withColumn(colName="Timestamp_hour", hour(col(colName="Timestamp")))
      .withColumn(colName="Timestamp_DayOfWeek", dayofweek(col(colName="Timestamp")))
      .withColumn(colName="Timestamp_DayOfMonth", dayofmonth(col(colName="Timestamp")))
      .withColumn(colName="Timestamp_Month", month( col(colName="Timestamp")))
      .withColumn(colName="Timestamp_Month", month(col(colName="Timestamp")))
      .withColumn(colName = "Days_Since_Campaign_Starts", datediff(end=$"Timestamp",start=$"schedule_start"))
      .withColumn(colName = "Days_Before_Campaign_Ends", datediff(end=$"schedule_end",start=$"Timestamp"))
      .withColumn(colName = "Campaign_Duration", datediff(end=$"schedule_end",start=$"schedule_start"))
      .drop(colNames="schedule_start","schedule_end")
    output_df
  }

  def getApisDataRDD(spark: SparkSession, df: DataFrame): RDD[Row] = {

    val output_rdd:RDD[Row] = df.coalesce(numPartitions=4).rdd.map(row => {
      val fetch1: String =String.format("http://vip.timezonedb.com//v2.1/get-time-zone?" +
        "key=UV0J0ZY60BVC&format=json&by=position&lat=%s&lng=%s",
        row.getAs[String](fieldName="latitude"), row.getAs[String](fieldName="longitude"))
      val result1: String = recursiveAPIRequest(fetch1)
      val country_code:String = (parse(result1) \\ "countryCode").values.toString
      val country_name:String = (parse(result1) \\ "countryName").values.toString
      val zone_name:String = (parse(result1) \\ "zoneName").values.toString
      val continent:String = zone_name.split("/")(0)
      val city:String = if (zone_name.split("/").length >1) zone_name.split("/")(1) else
        zone_name.split("/")(0)
      val fetch2: String = String.format("http://vip.timezonedb.com//v2.1/convert-time-zone?key=UV0J0ZY60BVC&format=" +
        "json&from=Europe/Paris&to=%s&time=%s",
        zone_name, row.getAs[String](fieldName="timestamp"))
      val result2: String = recursiveAPIRequest(fetch2)
      val df = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss")
      val unix_time:String = (parse(result2) \\ "toTimestamp").values.toString
      val fetch3: String = if (country_name.equals("South Korea")) "https://restcountries.eu/rest/v2/name/Korea" else  String.format("https://restcountries.eu/rest/v2/name/%s", country_name)
      val result3: String = recursiveAPIRequest(fetch3)
      val country_language:String = (parse(result3)(0) \\ "languages" \\ "iso639_1").values.toString
        Row(row.getAs[String](fieldName="auction_id"),
            row.getAs[String](fieldName="device_language"),
            country_language,
            if (country_language.equals(row.getAs[String](fieldName="device_language"))) true else false,
            continent,
            country_code,
            country_name,
            city,
            df.format(unix_time.toInt * 1000L)
        )
    })
    output_rdd
  }

def fromRDDtoDF(spark: SparkSession, rdd: RDD[Row]): DataFrame = {

  val schema = new StructType()
    .add(StructField("Auction_id", dataType =  StringType, nullable = true))
    .add(StructField("Device_language", dataType = StringType, nullable = true))
    .add(StructField("Country_language", dataType =  StringType, nullable = true))
    .add(StructField("Device_lg_Equals_country_lg", dataType =  BooleanType, nullable = true))
    .add(StructField("Continent", dataType =  StringType, nullable = true))
    .add(StructField("Country_code", dataType =  StringType, nullable = true))
    .add(StructField("Country_name", dataType =  StringType, nullable = true))
    .add(StructField("City", dataType =  StringType, nullable = true))
    .add(StructField("Timestamp", dataType =  StringType, nullable = true))


  val output_df:DataFrame = spark.createDataFrame(rdd, schema)
  output_df
}

  def getFullOsInfo(spark:SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._
    val output_df:DataFrame = df.select(cols=$"auction_id", $"os_version", $"os")
      .withColumn(colName="os_version",when($"os_version".isNotNull, value = substring_index(str=$"os_version",delim=".",count=1))
        .otherwise(value="unknown"))
      .withColumn(colName="fullOsInfo",concat(exprs=$"os", lit(literal="_"), $"os_version"))
      .drop(colNames="os","os_version")
    output_df
  }
}
