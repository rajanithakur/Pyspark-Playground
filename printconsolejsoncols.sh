#!/bin/bash


#export PYSPARK_PYTHON=./eco_env_p27.zip/eco_env_p27/bin/python
#export PYSPARK_DRIVER_PYTHON=./eco_env_p27.zip/eco_env_p27/bin/python
#export HDP_VERSION=${HDP_VERSION:-2.6.5.0-292}

export HADOOP_PATH=/opt/spark-2.3.0-bin-hadoop2.7
export HADOOP_JARS_PATH=$HADOOP_PATH/jars/

spark-submit \
--master yarn \
--deploy-mode client \
--driver-memory 1G \
--executor-memory 3G \
--executor-cores 1 \
--archives gs://p13n-storage2/r0t00xk/envs/dev27.zip \
--conf spark.yarn.dist.archives=gs://p13n-storage2/r0t00xk/envs/dev27.zip \
--jars ${HADOOP_JARS_PATH}/datanucleus-api-jdo-3.2.6.jar,${HADOOP_JARS_PATH}/datanucleus-rdbms-3.2.9.jar,$HADOOP_JARS_PATH/datanucleus-core-3.2.10.jar \
--conf spark.memory.fraction=0.8 \
--conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35" \
--conf "spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35" \
--conf spark.executor.memoryOverhead=400M \
--conf spark.shuffle.service.enabled=true \
--conf spark.sql.sources.partitionOverwriteMode=dynamic \
--conf spark.rpc.message.maxSize=500 \
--conf spark.rdd.compress=true \
--conf spark.default.parallelism=10000 \
--conf spark.sql.shuffle.partitions=10000 \
--conf spark.network.timeout=14400 \
--conf spark.rpc.askTimeout=3600 \
--conf spark.shuffle.compress=true \
--conf spark.task.maxFailures=20 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0 \
/u/users/r0t00xk/miniconda3/envs/scripts/printconsolejsoncols.py