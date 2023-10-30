#!/bin/bash

. /app/cluster/common.sh

echo "$(hostname -i) spark-master" >> /etc/hosts


echo "Starting master & falcon"
${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.master.Master --ip spark-master --port 7077 --webui-port 8080 >> ${SPARK_HOME}/logs/spark-master.out &
python3 /app/main.py

