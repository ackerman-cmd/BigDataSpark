package com.work.bigdatasparkproject.config;

import org.apache.spark.sql.SparkSession;

public class SparkConfig {

    public static SparkSession createSparkSession() {
        return SparkSession.builder()
                .appName("MongoDBReportsJob")
                .master("spark://spark-master:7077")
                .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
    }
}