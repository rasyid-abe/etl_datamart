
---

# Installation

## Tutorial install spark Windows

https://docs.google.com/document/d/1c9I13g8brYcFKzzsmbFJS0LsDjz66VyvISvX3QiyJr4/edit
https://phoenixnap.com/kb/install-spark-on-windows-10

Download This File 
https://drive.google.com/drive/folders/1C3GX4U6UXhtvb4Gj8UR8j4U_qZUYPh_v?usp=sharing

Paste in folder jars (Spark)

## Tutorial install spark MacOs

1. running “xcode-select --install “ in terminal
2. running “/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)”” in terminal
3. running “brew install python” in terminal
4. restart terminal and check "python —version” and show python with version 3.*.*
5. install java8 with “brew install —cask adoptopenjdk8” in your terminal
6. download apache spark 3.0.3 in https://spark.apache.org/downloads.html and choose package type “pre built for apache hadoop 2.7”
7. extract file in your folder ../Documents/Spark
8. run pip install pyspark==3.0.3
9. run in ur terminal “nano .zshrc”
10. copy this text, and paste
    export SPARK_HOME=/Users/macbookpro/Documents/Spark/spark-3.0.3-bin-hadoop2.7
    export PATH=$SPARK_HOME/bin:$PATH
    export JAVA_HOME=/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home
    #For python 3, You have to add the line below or you will get an error
    export PYSPARK_PYTHON=python3
    export PATH=$JAVA_HOME/bin:$PATH
    export PATH 
11. save, and run “source .zshrc”. Restart terminal


# After installation spark & python success

1. Install pip falcon.
2. Install pip falcon middleware.
3. Install pip pyJWT.
4. Install pip falcon-auth.
5. Install pip falcon-cors.
6. Install pip falcon-multipart.
7. pip install pyspark
- feature/etl-busiest-selling-time
``add feature etl busiest selling time``

## Unreleased
 - coldfix/add-cabang-on-join-akunting
 `` Add cabang in Join Query to Optimize Extract from MySQL Akunting``
