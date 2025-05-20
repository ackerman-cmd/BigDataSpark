package com.work.bigdatasparkproject.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class SparkConfigForTransform {
    private static final String CONFIG_PATH = "/opt/bitnami/spark/conf/application.yaml";

    public static SparkSession createSparkSession() {
        try {
            // Чтение конфигурации из application.yaml
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            Map<String, Object> config = mapper.readValue(new File(CONFIG_PATH), Map.class);

            // Извлечение параметров Spark
            Map<String, Object> sparkConfig = (Map<String, Object>) config.get("spark");
            String master = (String) sparkConfig.get("master");
            String appName = (String) sparkConfig.get("app-name");
            Boolean uiEnabled = (Boolean) ((Map<String, Object>) sparkConfig.get("ui")).get("enabled");

            // Создание SparkSession
            return SparkSession.builder()
                    .master(master)
                    .appName(appName)
                    .config("spark.ui.enabled", uiEnabled.toString())
                    .getOrCreate();
        } catch (IOException e) {
            throw new RuntimeException("Failed to load application.yaml: " + e.getMessage());
        }
    }
}