#!/bin/bash

printf "\n\n\t\tCleaning JARs...\n\n"
sbt clean
printf "\n\n\t\tBuilding JARs...\n\n"
sbt package
printf "\n\n\t\tDeploying Application through spark-submit...\n\n"
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1 --class main.kafkaStreamingConsumer /home/xs107-bairoy/xenonstack/l2/module4/spark_streaming/target/scala-2.12/spark_streaming_2.12-1.0.jar
