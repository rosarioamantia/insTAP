#!/usr/bin/env bash

docker stop containerConSpark
docker container rm containerConSpark

docker build --tag tap:spark .
docker run -e SPARK_ACTION=spark-submit-python -p 4040:4040 --network tap --name containerConSpark -it tap:spark prova.py org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.5 