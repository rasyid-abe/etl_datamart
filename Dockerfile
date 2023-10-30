ARG debian_buster_image_tag=8-jre-slim

FROM openjdk:${debian_buster_image_tag}
# -- Layer: OS + Python 3.7
ARG shared_workspace=/opt/workspace
RUN mkdir -p ${shared_workspace} && mkdir /app && chmod a+rwx /app && \
    apt-get update -y && \
    apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

ENV SHARED_WORKSPACE=${shared_workspace}
# -- Runtime
VOLUME ${shared_workspace}
ENV TZ="Asia/Jakarta"

ARG spark_version=3.1.3
ARG hadoop_version=2.7

RUN apt-get update -y && \
    apt-get install -y curl && \
    curl https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop${hadoop_version}.tgz -o spark.tgz && \
    tar -xf spark.tgz && \
    mv spark-${spark_version}-bin-hadoop${hadoop_version} /usr/bin/ && \
    mkdir /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/logs && \
    rm spark.tgz && \
    curl -L https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-8.0.29.tar.gz -o mysql-connector.tar.gz && \
    tar -xf mysql-connector.tar.gz && \
    mv mysql-connector-java-8.0.29/mysql-connector-java-8.0.29.jar //usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/jars/ && \
    rm mysql-connector.tar.gz && \
    rm -Rf mysql-connector-java-8.0.29 && \
    curl -L https://jdbc.postgresql.org/download/postgresql-42.2.18.jar -o postgresql-42.2.18.jar && \
    mv postgresql-42.2.18.jar //usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}/jars/
    
ENV SPARK_HOME /usr/bin/spark-${spark_version}-bin-hadoop${hadoop_version}
ENV SPARK_MASTER_HOST spark-master
ENV SPARK_MASTER_PORT 7077
ENV PYSPARK_PYTHON python3
ENV APP=/app
# -- Runtime
ADD . /app
WORKDIR ${APP}

RUN pip install --no-cache-dir -r requirements.txt
RUN chmod a+x ./cluster/common.sh ./cluster/spark-master.sh ./cluster/spark-worker.sh

ARG spark_master_web_ui=8080
EXPOSE ${spark_master_web_ui} ${SPARK_MASTER_PORT}

ARG log4j_version=2.17.0
RUN mkdir $SPARK_HOME/jars-backup
RUN mv $SPARK_HOME/jars/log4j-1.2.17.jar $SPARK_HOME/jars-backup
RUN mv $SPARK_HOME/jars/slf4j-log4j12-1.7.30.jar $SPARK_HOME/jars-backup

RUN curl https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-core/${log4j_version}/log4j-core-${log4j_version}.jar -o $SPARK_HOME/jars/log4j-core-${log4j_version}.jar
RUN curl https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-api/${log4j_version}/log4j-api-${log4j_version}.jar -o $SPARK_HOME/jars/log4j-api-${log4j_version}.jar
RUN curl https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-layout-template-json/${log4j_version}/log4j-layout-template-json-${log4j_version}.jar -o $SPARK_HOME/jars/log4j-layout-template-json-${log4j_version}.jar
RUN curl https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-1.2-api/${log4j_version}/log4j-1.2-api-${log4j_version}.jar -o $SPARK_HOME/jars/log4j-1.2-api-${log4j_version}.jar
RUN curl https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-slf4j-impl/${log4j_version}/log4j-slf4j-impl-${log4j_version}.jar -o $SPARK_HOME/jars/log4j-slf4j-impl-${log4j_version}.jar

COPY ./log4j2.properties $SPARK_HOME/conf/log4j2.properties
ADD ./cluster/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# CMD ["./cluster/spark-master.sh"]
CMD ["./cluster/spark-master.sh"]