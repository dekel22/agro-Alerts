package weather_alerts.scala;

import org.apache.commons.io.FileUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;


@SpringBootApplication
@EnableScheduling
public class WeatherAlertsApp {

    public static void main(String[] args) throws IOException {
        SpringApplication.run(WeatherAlertsApp .class, args);

    }

}

