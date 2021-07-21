package weather_alerts.scala.controllers;

import org.apache.spark.sql.SparkSession;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import weather_alerts.scala.services.AgroAlert;


@Service
public class BatchCalcAlerts {

    @Scheduled(fixedDelayString = "36000")
    public void CalcAlerts(){
        AgroAlert.calcWeatherAlertsBatch();
    }
}
