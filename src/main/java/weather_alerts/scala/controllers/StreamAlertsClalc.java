package weather_alerts.scala.controllers;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import weather_alerts.scala.services.AgroAlert;

public class StreamAlertsClalc {
    @EventListener(ApplicationReadyEvent.class)
    public void streamAlerts(){
        AgroAlert.calcWeatherAlertsStream();

    }

}
