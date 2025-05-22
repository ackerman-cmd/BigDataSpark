package com.work.bigdatasparkproject.config;

import org.apache.spark.sql.SparkSession;

public class SparkConfigMongo {
    public static SparkSession createSparkSession() {
        return SparkSession.builder()
                .appName("BigDataSparkProject")
                .config("spark.sql.shuffle.partitions", "4")
                .config("spark.mongodb.read.connection.uri", "mongodb://user:pass@mongo-db:27017/sales_db?authSource=sales_db")
                .config("spark.mongodb.write.connection.uri", "mongodb://user:pass@mongo-db:27017/sales_db?authSource=sales_db")
                .config("spark.sql.debug.maxToStringFields", 100)
                .config("spark.driver.memory", "4g")
                .config("spark.executor.memory", "4g")
                .config("spark.logConf", "true")
                .getOrCreate();
    }
}

