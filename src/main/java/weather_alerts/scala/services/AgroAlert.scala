package weather_alerts.scala.services

import org.apache.spark.sql.functions.{avg, col, from_json, struct, sum, to_json}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Encoders, SparkSession}
import weather_alerts.scala.model.{Farm, NormalizedStationCollectedData}
import weather_alerts.scala.services.KafkaManage.getOffsets



object AgroAlert extends App {
  val bootstrapServer = "cnt7-naya-cdh63:9092"
  val weatherData="weather_info_verified_data"
  def calcWeatherAlertsBatch() :Unit= {
    val offsets = KafkaManage.getOffsets("weather_info_verified_data")
    val old_warms = KafkaManage.getOffsets("warm_alerts")
    val dry_warms = KafkaManage.getOffsets("dry_alerts")

    val sparkSession: SparkSession = SparkSession.builder.master("local[*]").appName("example_app").getOrCreate
    val farmSchema = Encoders.product[Farm].schema
    val farms = sparkSession.read.schema(farmSchema).json("farms\\*")

    val schema = Encoders.product[NormalizedStationCollectedData].schema
    val inputDF = sparkSession
      .read.format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("subscribe", weatherData)
      .option("startingOffsets", """{"" + weatherData +"":{"0":375500}}""")
      .load.select(col("value").cast(StringType).alias("value"))
    val DF = inputDF
      .withColumn("parsed_json", from_json(col("value"), schema))
      .select("parsed_json.*")


    val aggDF = DF.groupBy(col("stationId"), col("channel")).agg(avg(col("value")), sum("value"))
    val heatDF = DF.filter(col("channel") === "TG").select(col("stationId"), col("value").as("warm_level"))
    val windDF = DF.filter(col("channel") === "WS").select(col("stationId"), col("value").as("wind_level"))
    val fieldsExtended = heatDF.join(windDF, "stationId").join(farms, "stationId")
    val DfWithDry = fieldsExtended.withColumn("dry_level", col("warm_level") + col("wind_level"))


    val DfWithHandle = DfWithDry.withColumn("dry_handle", col("dry_sensitivity") - col("dry_level")).
      withColumn("warm_handle", col("warm_sensitivity") - col("warm_level"))
    val warmAlertsDF = DfWithHandle.filter(col("warm_handle") < 0).select(col("email"), col("name"), col("crops"))
    val dryAlertsDF = DfWithHandle.filter(col("dry_handle") < 0).select(col("email"), col("name"), col("crops"))

    warmAlertsDF.select(to_json(struct("*")).as("value"))
      .selectExpr("CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("topic", "warm_alerts")
      .save()

    dryAlertsDF.select(to_json(struct("*")).as("value"))
      .selectExpr("CAST(value AS STRING)")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("topic", "dry_alerts")
      .save()

    val dd = KafkaManage.getOffsets("test")
    println("ff")
  }



  def calcWeatherAlertsStream() :Unit= {
    val sparkSession: SparkSession = SparkSession.builder.master("local[*]").appName("example_app").getOrCreate
    val farmSchema = Encoders.product[Farm].schema
    val farms = sparkSession.read.schema(farmSchema).json("farms\\*")

    val schema = Encoders.product[NormalizedStationCollectedData].schema
    val inputDF = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers",bootstrapServer)
      .option("subscribe", "topic1")
      .load()
    val DF = inputDF
      .withColumn("parsed_json", from_json(col("value"), schema))
      .select("parsed_json.*")


    val aggDF = DF.groupBy(col("stationId"), col("channel")).agg(avg(col("value")))
    val tempDF = DF.filter(col("channel") === "TG").select(col("stationId"), col("value").as("temp_level"))
    val fieldsExtended = tempDF.join(tempDF, "stationId").join(farms, "stationId")


    val DfWithHandle = fieldsExtended.withColumn("cool_handle",  col("temp_level")-col("cool_sensitivity") )

    val coolAlertsDF = DfWithHandle.filter(col("cool_handle") < 0).select(col("email"), col("name"), col("crops"))

    coolAlertsDF.select(to_json(struct("*")).as("value"))
      .selectExpr("CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("topic", "cool_alerts")
      .start()
  }
}



