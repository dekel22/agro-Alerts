package weather_alerts.scala.services

import org.apache.spark
import org.apache.spark.sql.functions.{avg, col, from_json, struct, sum, to_json}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Encoders, SparkSession}
import weather_alerts.scala.model.{Farm, NormalizedStationCollectedData}
import weather_alerts.configuration.Configuration

import scala.collection.JavaConversions.collectionAsScalaIterable



object AgroAlert extends App {
  val bootstrapServer = Configuration.bootstrupServer
  val dryAlerts="dry_alerts"
  val  warmAlerts="warm_alerts"


  def calcWeatherAlertsBatch() :Unit= {

    val offsets = KafkaManage.getOffsets(Configuration.weatherTopic)
    val takeFromOffset= offsets(0)-(Configuration.batchPerHour*Configuration.numberOfStation)
    val sparkSession: SparkSession = SparkSession.builder.master("local[*]").appName("calc_alerts").getOrCreate
    val farmSchema = Encoders.product[Farm].schema
    val farms = sparkSession.read.schema(farmSchema).json("farms\\*")

    val schema = Encoders.product[NormalizedStationCollectedData].schema
    val inputDF =  sparkSession
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "weather_info_verified_data")
      .option("startingOffsets", """{"weather_info_verified_data":{"0":""" +takeFromOffset +""" }}""")
      .load().select(col("value").cast(StringType).alias("value"))
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
      .option("topic", dryAlerts)
      .save()

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



