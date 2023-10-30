#!/bin/sh

${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.master.Master >> ${SPARK_HOME}/logs/spark-master.out &
python3 /app/main.py

