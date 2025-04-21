#!/bin/bash
echo "Initializing Neo4j database..."

# Ожидание доступности Neo4j с таймаутом (180 секунд)
TIMEOUT=180
ELAPSED=0
while [ $ELAPSED -lt $TIMEOUT ]; do
  cypher-shell -u neo4j -p password-secret -a bolt://localhost:7687 "CALL db.ping()" 2>error.log
  if [ $? -eq 0 ]; then
    echo "Neo4j is ready!"
    rm error.log
    break
  fi
  echo "Waiting for Neo4j to be ready... ($ELAPSED/$TIMEOUT seconds)"
  cat error.log
  neo4j status
  sleep 5
  ELAPSED=$((ELAPSED + 5))
done

if [ $ELAPSED -ge $TIMEOUT ]; then
  echo "Error: Neo4j did not become ready within $TIMEOUT seconds."
  cat error.log
  neo4j status
  exit 1
fi

# Создание Cypher-файла с командами
cat > /tmp/init.cypher <<EOF
CREATE DATABASE sales_db IF NOT EXISTS;
CREATE USER admin IF NOT EXISTS SET PLAIN PASSWORD 'password-secret' CHANGE NOT REQUIRED;
GRANT ROLE admin TO admin;
EOF

# Выполнение Cypher-команд из файла в базе system
cypher-shell -u neo4j -p password-secret -a bolt://localhost:7687 -d system -f /tmp/init.cypher 2>error.log

if [ $? -eq 0 ]; then
  echo "Database sales_db and user admin created."
  rm /tmp/init.cypher
else
  echo "Error: Failed to execute Cypher commands."
  cat error.log
  rm /tmp/init.cypher
  exit 1
fi