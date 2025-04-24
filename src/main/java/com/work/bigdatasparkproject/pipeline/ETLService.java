package com.work.bigdatasparkproject.pipeline;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

@Service
public class ETLService {

    private final SparkSession sparkSession;
    private final String csvPath;
    private final String postgresUrl;
    private final String postgresUser;
    private final String postgresPassword;

    public ETLService(
            SparkSession sparkSession,
            @Value("${input.csv.path}") String csvPath,
            @Value("${spring.datasource.url}") String postgresUrl,
            @Value("${spring.datasource.username}") String postgresUser,
            @Value("${spring.datasource.password}") String postgresPassword) {
        this.sparkSession = sparkSession;
        this.csvPath = csvPath;
        this.postgresUrl = postgresUrl;
        this.postgresUser = postgresUser;
        this.postgresPassword = postgresPassword;
    }


    public Dataset<Row> readData() {
        return sparkSession.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv(csvPath + "/MOCK_DATA.csv");
    }


    public void runPipeline() {
        Dataset<Row> data = readData();




    }


}
