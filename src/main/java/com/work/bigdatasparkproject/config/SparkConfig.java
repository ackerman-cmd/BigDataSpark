package com.work.bigdatasparkproject.config;


import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {
    @Value("${spark.master}")
    private String sparkMaster;

    @Value("${spark.app-name}")
    private String appName;

    @Bean
    public SparkSession sparkSession() {
        return SparkSession.builder()
                .master(sparkMaster)
                .appName(appName)
                .getOrCreate();
    }
}
