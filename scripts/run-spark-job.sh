#!/bin/bash

TASK=$1

if [ "$TASK" == "transform" ]; then
    spark-submit --class com.work.bigdatasparkproject.jobs.ETLtoSnowflakeJob \
                 --master spark://spark-master:7077 \
                 --jars /opt/bitnami/spark/jars/postgresql-42.7.3.jar,/opt/bitnami/spark/libs/*.jar \
                 /opt/bitnami/spark/jars/BigDataSparkProject-0.0.1-SNAPSHOT.jar

elif [ "$TASK" == "reports-clickhouse" ]; then
  spark-submit --class com.work.bigdatasparkproject.jobs.ClickHouseReportsJob \
                       --master spark://spark-master:7077 \
                       --jars /opt/bitnami/spark/libs/*.jar,/opt/bitnami/spark/jars/postgresql-42.7.3.jar \
                       /opt/bitnami/spark/jars/BigDataSparkProject-0.0.1-SNAPSHOT.jar

elif [ "$TASK" == "reports-mongo" ]; then
  spark-submit --class com.work.bigdatasparkproject.jobs.MongoDbReportsJob \
               --master spark://spark-master:7077 \
               --jars /opt/bitnami/spark/libs/*.jar, /opt/bitnami/spark/jars/postgresql-42.7.3.jar \
               --conf spark.mongodb.output.uri=mongodb://user:pass@mongo-db:27017/sales_db?authSource=sales_db \
               --conf spark.mongodb.input.uri=mongodb://user:pass@mongo-db:27017/sales_db?authSource=sales_db \
               /opt/bitnami/spark/jars/BigDataSparkProject-0.0.1-SNAPSHOT.jar

elif [ "$TASK" == "reports-cassandra" ]; then
  spark-submit --class com.work.bigdatasparkproject.jobs.CassandraReportsJob \
               --master spark://spark-master:7077 \
               --jars /opt/bitnami/spark/libs/*.jar, /opt/bitnami/spark/jars/postgresql-42.7.3.jar\
               --conf "spark.cassandra.connection.host=cassandra" \
               --conf "spark.cassandra.connection.port=9042" \
               --conf "spark.cassandra.auth.username=cassandra" \
               --conf "spark.cassandra.auth.password=cassandra" \
               /opt/bitnami/spark/jars/BigDataSparkProject-0.0.1-SNAPSHOT.jar


elif [ "$TASK" == "reports-neo4j" ]; then
  spark-submit --class com.work.bigdatasparkproject.jobs.Neo4jReportsJob \
                       --master spark://spark-master:7077 \
                       --jars /opt/bitnami/spark/jars/postgresql-42.7.3.jar,/opt/bitnami/spark/libs/*.jar \
                       /opt/bitnami/spark/jars/BigDataSparkProject-0.0.1-SNAPSHOT.jar


elif [ "$TASK" == "reports-valkey" ]; then
  spark-submit --class com.work.bigdatasparkproject.jobs.ValkeyReportsJob \
                       --master spark://spark-master:7077 \
                       --jars /opt/bitnami/spark/jars/postgresql-42.7.3.jar,/opt/bitnami/spark/libs/*.jar \
                       /opt/bitnami/spark/jars/BigDataSparkProject-0.0.1-SNAPSHOT.jar

else
    echo "Укажите задачу:  transform или reports-clickhouse или reports-mongo или reports-cassandra или reports-neo4j или reports-valkey"
    exit 1
fi