package weather_alerts.scala.model

import java.sql.Timestamp

case class NormalizedStationCollectedData(stationId: Integer, datetime: Timestamp,
                                          channel: String, value: Double) extends Serializable
