from datetime import datetime
from pyspark.sql.types import StructField, StructType, StringType, FloatType, IntegerType
import psycopg2
import mysql.connector
from sqlalchemy import create_engine

def createDataFrameHistoryLog(spark, tsCurrent, sAdd, numrow, tsProcess, sparam):
    dataActLog = [(str(convertToAsiaJakarta(datetime.now())) ,
        convertToAsiaJakarta(tsCurrent), convertToAsiaJakarta(sAdd), numrow, tsProcess, sparam)]
    schemaLog = StructType([
        StructField('actdate', StringType(), True),
        StructField('ts_current', StringType(), True),
        StructField('ts_end', StringType(), True),
        StructField('num_row', IntegerType(), True),
        StructField('time_process', FloatType(), True),
        StructField('params', StringType(), True)
    ])
    rdd = spark.sparkContext.parallelize(dataActLog)
    dfHistory = spark.createDataFrame(rdd, schemaLog)
    return dfHistory

def createDataFrameHistoryLogStreaming(spark, tsCurrent, sAdd, numrow, tsProcess, sparam, martParam):
    dataActLog = [(str(convertToAsiaJakarta(datetime.now())) ,
        convertToAsiaJakarta(tsCurrent), convertToAsiaJakarta(sAdd), numrow, tsProcess, sparam, martParam)]
    schemaLog = StructType([
        StructField('actdate', StringType(), True),
        StructField('ts_current', StringType(), True),
        StructField('ts_end', StringType(), True),
        StructField('num_row', IntegerType(), True),
        StructField('time_process', FloatType(), True),
        StructField('params', StringType(), True),
        StructField('mart_param', StringType(), True)
    ])
    rdd = spark.sparkContext.parallelize(dataActLog)
    dfHistory = spark.createDataFrame(rdd, schemaLog)
    return dfHistory

def createETLLog(tableName, conf_postgres, tsCurrent, sAdd, numrow, tsProcess, sparam, **moreColumns):
    """
        required cols : tableName, config, start, end , numrow, runtime, param.
        - You can add additional columns by passing col=value to this function. 
        - e.g., createETLLog(... , col1=123, col2='this is the value' )
    """
    moreCol = list(moreColumns.keys())
    itsVal = [f"'{val}'" for val in list(moreColumns.values())]
    if len(itsVal) != len(moreCol):
        raise Exception("Additional columns do not have matching value!")

    sql_query = \
    f"""
    INSERT INTO {tableName} 
        (actdate,ts_current,num_row,time_process,ts_end,params{'' if len(moreCol) == 0 else ','+','.join(moreCol)})
    VALUES
        ('{str(convertToAsiaJakarta(datetime.now()))}','{tsCurrent}',{numrow},{tsProcess},'{sAdd}','{sparam}'{'' if len(moreCol) == 0 else ','+','.join(itsVal)})
    """

    executePostgres(sql_query, conf_postgres)


def executePostgres(query, conf_postgres):
    mydb = openPostgresqlConnection(conf_postgres)
    mycursor = mydb.cursor()
    mycursor.execute(query)
    mydb.commit()
    mydb.close()

def executePostgresUpsert(query, row, conf_postgres, method = "single"):
    mydb = openPostgresqlConnection(conf_postgres)
    mycursor = mydb.cursor()
    if method == "single":
        mycursor.execute(query, row)
    elif method == "many":
        mycursor.executemany(query, row)
    mydb.commit()
    mydb.close()

def executeMysql(query, conf_mysql):
    mydb = openMysqlConnection(conf_mysql)
    mycursor = mydb.cursor()
    mycursor.execute(query)
    mydb.commit()
    mydb.close()

def fetchrow(sql_query, conf_postgres, fetchone = True):
    try:
        mydb = openPostgresqlConnection(conf_postgres)
        cursor = mydb.cursor()
        cursor.execute(sql_query)
        # get all records
        row = cursor.fetchone() if fetchone else cursor.fetchall()
    except psycopg2.OperationalError as e:
        raise e

    cursor.close()
    mydb.close()
        
    return row

def executeMySQL(query, conf_mysql):
    mydb = openMysqlConnection(conf_mysql)
    mycursor = mydb.cursor()
    mycursor.execute(query)
    mydb.commit()
    mydb.close()


def executemanyMySQL(query, data, conf_mysql):
    mydb = openMysqlConnection(conf_mysql)
    mycursor = mydb.cursor()
    mycursor.executemany(query, data)
    mydb.commit()
    mydb.close()

def openMysqlConnection(conf_mysql):
    mydb = mysql.connector.connect(
        host=conf_mysql['HOST'],
        user=conf_mysql['USER'],
        password=conf_mysql['PASS'],
        database=conf_mysql['DBNAME'],
        port=conf_mysql['PORT']
    )

    return mydb

def openPostgresqlConnection(conf_postgres):
    mydb = psycopg2.connect(
        host=conf_postgres['HOST'],
        user=conf_postgres['USER'],
        password=conf_postgres['PASS'],
        database=conf_postgres['DBNAME'],
        port=conf_postgres['PORT']
    )

    return mydb

def openPostgresqlConnectionAlchemy(conf_postgres):
    connection = f"postgresql://{conf_postgres['USER']}:{conf_postgres['PASS']}@{conf_postgres['HOST']}:{conf_postgres['PORT']}/{conf_postgres['DBNAME']}"
    mydb = create_engine(connection)

    return mydb

def openMysqlConnectionAlchemy(conf_mysql):
    connection = f"mysql+mysqlconnector://{conf_mysql['USER']}:{conf_mysql['PASS']}@{conf_mysql['HOST']}:{conf_mysql['PORT']}/{conf_mysql['DBNAME']}"
    mydb = create_engine(connection)

    return mydb

def fetchrowMysql(sql_query, conf_mysql):
    try:
        mydb = openMysqlConnection(conf_mysql)
        cursor = mydb.cursor()
        cursor.execute(sql_query)
        # get all records

        row = cursor.fetchall()
    except mysql.connector.Error as e:
        raise e
    if mydb.is_connected():
        cursor.close()
        mydb.close()
    else:
        raise Exception("Cannot connect to target database within the given credentials!")
        
    return row

def convertToAsiaJakarta(timestamp):
    return timestamp.strftime('%Y-%m-%d %H:%M:%S +07:00')
