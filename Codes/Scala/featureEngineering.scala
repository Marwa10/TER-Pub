package tabmo_project
import java.text.SimpleDateFormat

import net.liftweb.json.{JsonAST, parse}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, posexplode, regexp_replace, split, substring, when, _}
import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spray.json._
import tabmo_project.cleaningData.{Spark, cleanData}
import java.net.{HttpURLConnection, URL}

object featureEngineering {
  def main(args: Array[String]): Unit = {
    val lines:Int= 1000
    val spark:SparkSession = Spark(4)
    // import implicit in order to use $ operator
    import spark.implicits._
    // Clean the input data
    val df:DataFrame = cleanData(spark)

    val df_temp:DataFrame = apiPreparation(spark, df)
    // IAB vectorization
    val rdd:RDD[Row] = getApisDataRDD(df_temp.limit(lines))

    // a merger ensemble

    val df_sortie: DataFrame = fromRDDtoDF(spark, rdd)
    val df_os_info: DataFrame = getFullOsInfo(spark, df.limit(lines))
    val df_iab: DataFrame = getIabTranspose(spark, df.limit(lines))
    val df_other_features: DataFrame = df.limit(lines).select($"auction_id", $"click", $"exchange",
      $"app_or_site",$"has_gps", $"device_type", $"connection_type", $"has_ifa",$"win_price",$"win_price_loc",
      $"bidder_name",$"schedule_start",$"schedule_end")


    // Join all the features
    val df_joined_data:DataFrame = df_other_features
      .join(df_sortie,Seq("auction_id"), joinType = "full")
      .join(df_iab,Seq("auction_id"), joinType = "full")
      .join(df_os_info,Seq("auction_id"), joinType = "full")

    // Final operation on date data
    val output_df: DataFrame = getDateFeatures(spark, df_joined_data)

    output_df.coalesce(numPartitions = 1)
      .write.format(source="csv")
      .option("header","true")
      .csv(path="/home/joseph/Bureau/Master/Ter/data/final_example_data.csv")

  }

  def get(url: String,
          connectTimeout: Int = 5000,
          readTimeout: Int =  5000,
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
  def recursiveAPIRequest(url_link: String,nbTry:Int): spray.json.JsObject = {
    try
      {
        if (nbTry>0){
          val fetch:String = get(url_link)
          val result:spray.json.JsObject = fetch.parseJson.asJsObject()
          result
        }
        else{
          val error_result = """{"countryCode":"error","countryName":"error","zoneName":"error","toTimestamp":1582123375}"""
          error_result.parseJson.asJsObject()
        }
      }
    catch
      {
        case _: java.io.IOException =>
          recursiveAPIRequest(url_link,nbTry=nbTry-1)
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

    // Merging data in order to get all informations about app & sites
    // val finalDataIAB:DataFrame = finalDataOneRow
    //  .drop(colName="site_or_app_categories")
    //  .join(iab_df,Seq("auction_id"), joinType = "left")
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

  def getApisDataRDD(df: DataFrame): RDD[Row] = {
    val if_warning_api_date: JsonAST.JValue  = parse(""" {"warning": "1970-01-01 00:00:00"} """)
    val output_rdd:RDD[Row] = df.rdd.map(row => {
      val fetch1: String =String.format("http://vip.timezonedb.com//v2.1/get-time-zone?" +
        "key=UV0J0ZY60BVC&format=json&by=position&lat=%s&lng=%s",
        row.getAs[String](fieldName="latitude"), row.getAs[String](fieldName="longitude"))
      val result1: spray.json.JsObject = recursiveAPIRequest(fetch1,nbTry = 2)
      val country_code:String = result1.fields("countryCode").toString.dropRight(1).substring(1)
      val country_name:String = result1.fields("countryName").toString.dropRight(1).substring(1)
      val zone_name:String = result1.fields("zoneName").toString.dropRight(1).substring(1)
      val continent:String = zone_name.split("/")(0)
      val city:String = if (zone_name.split("/").length >1) zone_name.split("/")(1) else
        zone_name.split("/")(0)
      val fetch2: String = String.format("http://vip.timezonedb.com//v2.1/convert-time-zone?key=UV0J0ZY60BVC&format=" +
        "json&from=Europe/Paris&to=%s&time=%s",
        zone_name, row.getAs[String](fieldName="timestamp"))
      val result2: spray.json.JsObject = recursiveAPIRequest(fetch2,nbTry = 2)
      val df = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss")
      val unix_time:String = result2.fields("toTimestamp").toString()
      val fetch3: String = String.format("https://restcountries.eu/rest/v2/name/%s", country_name)
      val result3: String = if (zone_name.equals("error")) "error" else get(url=fetch3)
      val json_parsed: JsonAST.JValue = if (result3.equals("error")) if_warning_api_date \\ "warning" else
        parse(result3)(0)
      val country_language:String = if (result3.equals("error")) (json_parsed \\ "warning").values.toString else
        (json_parsed \\ "languages" \\ "iso639_1").values.toString
        Row(row.getAs[String](fieldName="auction_id"),
            row.getAs[String](fieldName="device_language"),
            country_language,
            if (country_language.equals(row.getAs[String](fieldName="device_language"))) true else false,
            continent,
            country_code,
            country_name,
            city,
            if (unix_time.equals("0")) df.format(0) else df.format(unix_time.toInt * 1000L)
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
