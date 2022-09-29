#!/usr/bin/env bash

docker stop containerConSpark
docker container rm containerConSpark

docker build --tag tap:spark .
docker run -e SPARK_ACTION=spark-submit-python -p 4040:4040 --network tapp --name containerConSpark -it tap:spark prova_one_index.py "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.2.0"
