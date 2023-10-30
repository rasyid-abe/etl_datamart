#!/bin/bash

# Set variables
core=1
memory='1G'
while getopts ":m:c:" option; do
   case $option in
      m) 
         memory="$OPTARG";;
      c) 
         core="$OPTARG";;
     \?) # Invalid option
         echo "Error: Invalid option"
         exit;;
   esac
done


. /app/cluster/common.sh

if ! getent hosts spark-master; then
  sleep 5
  exit 0
fi

${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077 --webui-port 8081 -c $core -m $memory