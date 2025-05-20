#!/bin/bash

TASK=$1

if [ "$TASK" == "load-csv" ]; then
    spark-submit --class com.work.bigdatasparkproject.jobs.LoadCSVtoPostgres \
                 --master spark://spark-master:7077 \
                 --jars /opt/bitnami/spark/jars/postgresql-42.7.3.jar,/opt/bitnami/spark/libs/*.jar \
                 /opt/bitnami/spark/jars/BigDataSparkProject-0.0.1-SNAPSHOT.jar
elif [ "$TASK" == "transform" ]; then
    spark-submit --class com.work.bigdatasparkproject.jobs.ETLtoSnowflakeJob \
                 --master spark://spark-master:7077 \
                 --jars /opt/bitnami/spark/jars/postgresql-42.7.3.jar,/opt/bitnami/spark/libs/*.jar \
                 /opt/bitnami/spark/jars/BigDataSparkProject-0.0.1-SNAPSHOT.jar
elif [ "$TASK" == "full-pipeline" ]; then
    # Сначала загрузка CSV
    spark-submit --class com.work.bigdatasparkproject.jobs.LoadCSVtoPostgres \
                 --master spark://spark-master:7077 \
                 --jars /opt/bitnami/spark/jars/postgresql-42.7.3.jar,/opt/bitnami/spark/libs/*.jar \
                 /opt/bitnami/spark/jars/BigDataSparkProject-0.0.1-SNAPSHOT.jar
    # Затем трансформация
    spark-submit --class com.work.bigdatasparkproject.jobs.ETLtoSnowflakeJob \
                 --master spark://spark-master:7077 \
                 --jars /opt/bitnami/spark/jars/postgresql-42.7.3.jar,/opt/bitnami/spark/libs/*.jar \
                 /opt/bitnami/spark/jars/BigDataSparkProject-0.0.1-SNAPSHOT.jar
else
    echo "Укажите задачу: load-csv, transform или full-pipeline"
    exit 1
fi