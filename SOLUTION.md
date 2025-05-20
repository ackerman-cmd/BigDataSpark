Порядок запуска:

1) запустите из корня ./mvnw clean package -DskipTests - дождитесь сборки папки target
2) выполните из корня docker-compose up --build -d
3) импортируйте все файлы из mock_data в postgres (через dBeaver)
4) проверьте что все контейнеры запущены (в том числе и отработал контейнер для инициализации cassandra)
5) запустите docker exec -it neo4j-db bash  /docker-entrypoint-initdb.d/init-db.sh
6) bash /opt/bitnami/spark/run-spark-job.sh transform
7) docker exec -it spark-submit /bin/bash
