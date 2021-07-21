package weather_alerts.scala.model

import java.sql.Timestamp

case class Farm(stationId: Integer, name:String,lastName:String,email:String,location:String,crops:String,dry_sensitivity:Integer,
                warm_sensitivity:Integer,cold_sensitivity:Integer) extends Serializable



