from datetime import datetime
from datetime import timedelta
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
import json,time
from pprint import pprint
from Utility.sql import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial, reduce
import helper
import pytz

conf_mysql_source = helper.getConfMySQLGCP()
con_mysql_source = helper.generate_mysql_connection_url(conf_mysql_source)

conf_postgres_target = helper.getConfPostgresMart()
con_postgres_target = helper.generate_postgres_connection_url(conf_postgres_target)


def processServiceUtilizationSequencePerMin(seq):
    sql_max_ts_current = "SELECT MAX(ts_end) at time zone 'Asia/Jakarta' FROM mart_prd.raw_datamart_utilization_history"
    row_max_ts_current = fetchrow(sql_max_ts_current,conf_postgres_target)
    sTimeStart = row_max_ts_current[0]
    sql_count_ts_current = "SELECT count(ts_end) FROM mart_prd.raw_datamart_utilization_history WHERE ts_end='"+convertToAsiaJakarta(sTimeStart)+"'"
    row_count_ts_current = fetchrow(sql_count_ts_current,conf_postgres_target)
    nTimeStart = row_count_ts_current[0]
    #sTimeEnd=sTimeStart+timedelta(minutes=(5*nTimeStart))
    sTimeEnd=sTimeStart+timedelta(minutes=(seq*nTimeStart))
    sequence = "sequence"+str(seq)+"min"
    return processServiceUtilizationParamBegEndTime('pyspark-postgresql-'+str(seq)+'minute-sequence-api-item', sTimeStart, sTimeEnd,{'fn': sequence,'start_time':str(sTimeStart),'end_time':str(sTimeEnd)})

def processServiceUtilizationSequence5Min():
    sql_max_ts_current = "SELECT MAX(ts_end) at time zone 'Asia/Jakarta' FROM mart_prd.raw_datamart_utilization_history"
    row_max_ts_current = fetchrow(sql_max_ts_current,conf_postgres_target)
    sTimeStart = row_max_ts_current[0]
    sql_count_ts_current = "SELECT count(ts_end) FROM mart_prd.raw_datamart_utilization_history WHERE ts_end='"+convertToAsiaJakarta(sTimeStart)+"'"
    row_count_ts_current = fetchrow(sql_count_ts_current,conf_postgres_target)
    nTimeStart = row_count_ts_current[0]
    sTimeEnd=sTimeStart+timedelta(minutes=(5*nTimeStart))
    return processServiceUtilizationParamBegEndTime('pyspark-postgresql-5minute-sequence-api-item', sTimeStart, sTimeEnd,{'fn': 'sequence5min','start_time':str(sTimeStart),'end_time':str(sTimeEnd)})

def processServiceUtilizationSequence45Min():
    sql_max_ts_current = "SELECT MAX(ts_end) at time zone 'Asia/Jakarta' FROM mart_prd.raw_datamart_utilization_history"
    row_max_ts_current = fetchrow(sql_max_ts_current,conf_postgres_target)
    sTimeStart = row_max_ts_current[0]
    sql_count_ts_current = "SELECT count(ts_end) FROM mart_prd.raw_datamart_utilization_history WHERE ts_end='"+convertToAsiaJakarta(sTimeStart)+"'"
    row_count_ts_current = fetchrow(sql_count_ts_current,conf_postgres_target)
    nTimeStart = row_count_ts_current[0]
    sTimeEnd=sTimeStart+timedelta(minutes=(45*nTimeStart))
    return processServiceUtilizationParamBegEndTime('pyspark-postgresql-45minute-sequence-api-item', sTimeStart, sTimeEnd,{'fn': 'sequence45min','start_time':str(sTimeStart),'end_time':str(sTimeEnd)})

def processServiceUtilizationSequence120Min():
    sql_max_ts_current = "SELECT MAX(ts_end) at time zone 'Asia/Jakarta' FROM mart_prd.raw_datamart_utilization_history"
    row_max_ts_current = fetchrow(sql_max_ts_current,conf_postgres_target)
    sTimeStart = row_max_ts_current[0]
    sql_count_ts_current = "SELECT count(ts_end) FROM mart_prd.raw_datamart_utilization_history WHERE ts_end='"+convertToAsiaJakarta(sTimeStart)+"'"
    row_count_ts_current = fetchrow(sql_count_ts_current,conf_postgres_target)
    nTimeStart = row_count_ts_current[0]
    sTimeEnd=sTimeStart+timedelta(minutes=(120*nTimeStart))
    return processServiceUtilizationParamBegEndTime('pyspark-postgresql-120minute-sequence-api-item', sTimeStart, sTimeEnd,{'fn': 'sequence120min','start_time':str(sTimeStart),'end_time':str(sTimeEnd)})

def processServiceUtilizationParamEndTimePeriod(end_time,period):
    sql_max_ts_current = "SELECT MAX(ts_end) at time zone 'Asia/Jakarta' FROM mart_prd.raw_datamart_utilization_history"
    row_max_ts_current = fetchrow(sql_max_ts_current,conf_postgres_target)
    sTimeStart = row_max_ts_current[0]
    sTimeEnd=sTimeStart+timedelta(minutes=period)
    if(sTimeEnd < end_time):
        return processServiceUtilizationParamBegEndTime('pyspark-postgresql-period-endtimeperiod-api',sTimeStart,sTimeEnd,{'fn':'endtimeperiod','end_time':str(end_time),'period':str(period)})
    else:
        return processServiceUtilizationParamBegEndTime('pyspark-postgresql-period-endtimeperiod-api', sTimeStart, end_time,
                                                  {'fn': 'endtimeperiod', 'end_time': str(end_time),
                                                   'period': str(period)})

#literally start & end time.
def processServiceUtilizationParamBegEndTime(appName,sCurrent,sAdd,param):
    conf = helper.get_spark_config(appName)

    spark = SparkSession.builder.config(conf=conf).getOrCreate() #init spark
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)

    datenow = datetime.today().astimezone(tz=pytz.timezone('Asia/Jakarta'))

    #for jumlah loop data per 100 data
    n_size = 100
    n_size_insert = 100

    cachedTable = []
    timelog = {}

    mode = "append"
    properties_target = {"user": conf_postgres_target['USER'], "password": conf_postgres_target['PASS'],"driver": "org.postgresql.Driver"}
    # print(sCurrent)
    df_utilizationd_count = 0
    dateStart = (time.time() * 1000)

    # sCurrent = datetime.strptime(sCurrent,"%Y-%m-%d %H:%M:%S")
    # sAdd = datetime.strptime(sAdd,"%Y-%m-%d %H:%M:%S")
    # print(sCurrent)
    # print(sAdd)

    # prevent data be left behind over microseconds
    sCurrent = sCurrent-timedelta(seconds=5)

    sql_query = f"""
    SELECT
    	r.id,
    	r.reservation_no,
    	r.merchant_refno m_user_id_user,
    	r.outlet_refno m_cabang_id_cabang,
    	r.customer_id,
    	r.status,
    	r.updated_at
    FROM
    	reservation r
    WHERE
        r.updated_at BETWEEN '{sCurrent}' and '{sAdd}'
    """
    # print(sql_query)

    df_utilization_name = "df_utilization"
    df_utilization = spark.read.format("jdbc").option("url",con_mysql_source) \
        .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "(" + sql_query + ") sdtable") \
        .option("user", conf_mysql_source['USER']).option("password", conf_mysql_source['PASS']).load()
    df_utilization.createOrReplaceTempView(df_utilization_name)

    spark.catalog.cacheTable(df_utilization_name)
    cachedTable.append(df_utilization_name)

    df_updatedate = spark.sql(
        "SELECT MAX(updated_at) max_update_date, "\
        "MIN(updated_at) min_update_date "\
        "FROM df_utilization"
    )

    sWhereOrDaily = ""
    #handle if record not found updatedate in range between sCurrent n sAdd
    t_startupdatedate = sCurrent

    #handle jika tgl terakhir updatedate + 5 minute di atas tgl skr
    if sAdd.strftime("%Y-%m-%d %H:%M:%S %z") > datenow.strftime("%Y-%m-%d %H:%M:%S %z"):
        t_endupdatedate = sCurrent
    else:
        t_endupdatedate = sAdd

    if (df_utilization.count() > 0):
        df_date = df_updatedate.first()
        t_endupdatedate = df_date['max_update_date']
        t_startupdatedate = df_date['min_update_date']

        aID = []
        aIDDEl = []
        for row in df_utilization.rdd.collect():
            if(row['status'] == '700' or row['status'] == '9'):
                aID.append(str(row['id']))
            else:
                aIDDEl.append(str(row['id']))

        aaID = [aID[i:i + n_size] for i in range(0, len(aID), n_size)]
        aaIDDel = [aIDDEl[i:i + n_size] for i in range(0, len(aIDDEl), n_size)]

        df_utilizationd = None
        for axID in (aaID+aaIDDel):
            strWhereID = ",".join(axID)
            sql_query = f"""
            SELECT
            	rs.id idd,
                rs.reservation_id id,
            	rs.product_refno,
            	rs.reservation_service_date date,
            	rs.reservation_service_start_time start_time,
            	rs.reservation_service_end_time end_time,
                CASE
                    WHEN ((TIME_TO_SEC(rs.reservation_service_end_time)/60 - TIME_TO_SEC(rs.reservation_service_start_time)/60)) < 0 THEN
                        CAST((((TIME_TO_SEC(rs.reservation_service_end_time)/60 - TIME_TO_SEC(rs.reservation_service_start_time)/60)) * -1) AS UNSIGNED)
                    ELSE
                        CAST(((TIME_TO_SEC(rs.reservation_service_end_time)/60 - TIME_TO_SEC(rs.reservation_service_start_time)/60)) AS UNSIGNED)
                END AS duration,
            	rs.employee_refno,
            	rs.reservation_service_total_price price
            FROM
            	reservation_service rs
            WHERE
                rs.reservation_service_is_parent = 1 and
                rs.reservation_id IN ({strWhereID})
            """
            # print(sql_query)

            df_temp = spark.read.format("jdbc").option("url", con_mysql_source) \
                .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "(" + sql_query + ") sdtable") \
                .option("user", conf_mysql_source['USER']).option("password", conf_mysql_source['PASS']).load()
            if(df_utilizationd==None):
                df_utilizationd = df_temp
            else:
                df_utilizationd = df_utilizationd.union(df_temp)

        df_jurnald_name = "df_utilizationd"
        if(df_utilizationd!=None):
            df_utilizationd.createOrReplaceTempView("df_utilizationd")
            spark.catalog.cacheTable(df_jurnald_name)
            cachedTable.append(df_jurnald_name)
            df_utilizationd_count = df_utilizationd.count()

        if (df_utilizationd!=None and df_utilizationd_count > 0):
            # CLEANSING DATA RAW DATA MART DENGAN PARAMETER ID_TRANSACTIONS SEBELUM DIISI DENGAN DATA UPDATE TERBARU
            # INSERT DATA RAW DATA MART DENGAN DATA TERBARU
            # JOIN ON SPARK untuk transaction & transaction detail
            df_rawMartReserve_name = "df_raw_mart_reservation"
            spark_query = "SELECT " \
                          "  df_utilizationd.idd as raw_mart_utilization_id_reservation_detail " \
                          ", df_utilization.id as raw_mart_utilization_id_reservation " \
                          ", df_utilization.m_user_id_user " \
                          ", df_utilization.m_cabang_id_cabang " \
                          ", df_utilization.customer_id as raw_mart_utilization_customer_id " \
                          ", df_utilizationd.employee_refno as raw_mart_utilization_employee_id " \
                          ", df_utilizationd.product_refno as raw_mart_utilization_product_id " \
                          ", df_utilizationd.date as raw_mart_utilization_date " \
                          ", df_utilizationd.start_time as raw_mart_utilization_start_time " \
                          ", df_utilizationd.end_time as raw_mart_utilization_end_time " \
                          ", df_utilizationd.duration as raw_mart_utilization_duration " \
                          ", df_utilizationd.price as raw_mart_utilization_price " \
                          " FROM df_utilization JOIN df_utilizationd ON df_utilization.id=df_utilizationd.id WHERE df_utilization.status IN ('700','9') "
            df_raw_mart_reservation = spark.sql(spark_query).fillna(value=0)
            df_raw_mart_reservation.createOrReplaceTempView(df_rawMartReserve_name)
            spark.catalog.cacheTable(df_rawMartReserve_name)
            cachedTable.append(df_rawMartReserve_name)

             # fetch to be deleted raw -> this is used to clean mart
            df_name_raw_mart_deleted = "df_rawmart_del"
            df_raw_mart_deleted = None
            for chunked_deleted_ids in aaIDDel:
                sql_query = """
                    SELECT DISTINCT
                        m_user_id_user,
                        m_cabang_id_cabang,
                        raw_mart_utilization_employee_id,
                        raw_mart_utilization_date
                    FROM mart_prd.raw_mart_utilization
                    WHERE raw_mart_utilization_id_reservation in ({deleted_transactions})
                """.format(deleted_transactions=",".join([id_trx for id_trx in chunked_deleted_ids ]))

                df_temp = spark.read.format("jdbc").option("url", con_postgres_target) \
                                .option("driver", "org.postgresql.Driver").option("dbtable", "(" + sql_query + ") sdtable") \
                                .option("user", conf_postgres_target['USER']).option("password", conf_postgres_target['PASS']).load()
                df_raw_mart_deleted = df_temp if df_raw_mart_deleted == None else df_raw_mart_deleted.union(df_temp)

            tempViews = []
            if(df_raw_mart_deleted != None):
                df_raw_mart_deleted.createOrReplaceTempView(df_name_raw_mart_deleted)
                spark.catalog.cacheTable(df_name_raw_mart_deleted)
                tempViews.append(df_name_raw_mart_deleted)

            # CLEANSING DATA RAW DATA MART DENGAN PARAMETER ID_TRANSACTIONS SEBELUM DIISI DENGAN DATA UPDATE TERBARU
            executor    = ThreadPoolExecutor()
            cleanJobs   = []

            for axID in aaID:
                strWhereID = ",".join(axID)
                sql_query = "DELETE FROM mart_prd.raw_mart_utilization WHERE raw_mart_utilization_id_reservation in (" + strWhereID + ")"
                cleanJobs.append(executor.submit(executePostgres, sql_query, conf_postgres_target))

            for job in cleanJobs:
                job.result()

            dfraw_rawmart_name = "df_rawmart"
            spark_query_with_del = "SELECT m_user_id_user,m_cabang_id_cabang " \
                                  ", df_utilizationd.employee_refno as raw_mart_utilization_employee_id " \
                                  ", df_utilizationd.date as raw_mart_utilization_date " \
                                  " FROM df_utilization JOIN df_utilizationd ON df_utilization.id=df_utilizationd.id"
            df_join_with_del = spark.sql(spark_query_with_del).fillna(value=0)
            df_join_with_del.createOrReplaceTempView(dfraw_rawmart_name)
            spark.catalog.cacheTable(dfraw_rawmart_name)
            cachedTable.append(dfraw_rawmart_name)

            # CLEANSING DATA RAW DATA MART DENGAN PARAMETER ID_TRANSACTIONS SEBELUM DIISI DENGAN DATA UPDATE TERBARU
            # DELETE UNTUK STATUS 9 DAN 15 IDDEL


            # executor    = ThreadPoolExecutor()
            cleanJobs   = []

            for axID in aaIDDel:
                strWhereID = ",".join(axID)
                sql_query = "DELETE FROM mart_prd.raw_mart_utilization WHERE raw_mart_utilization_id_reservation in (" + strWhereID + ")"
                cleanJobs.append(executor.submit(executePostgres, sql_query, conf_postgres_target))

            for job in cleanJobs:
                job.result()

            # write ke mysql untuk raw datamart di mysql
            df_raw_mart_reservation.write.jdbc(url=con_postgres_target, table="mart_prd.raw_mart_utilization", mode=mode,properties=properties_target)
            # hasil join kita simpen sebagai df_rawmart / df_Join

              # UNION DF RAWMART
            df_rawMart_join_name = "df_rawMart_union"
            df_rawMart_join = df_raw_mart_deleted.union(df_join_with_del)
            df_rawMart_join = df_rawMart_join.distinct()
            df_rawMart_join.createOrReplaceTempView(df_rawMart_join_name)
            spark.catalog.cacheTable(df_rawMart_join_name)
            cachedTable.append(df_rawMart_join_name)

            # DAILY SERVICE
            spark_query = "SELECT m_user_id_user,m_cabang_id_cabang, " \
                          "raw_mart_utilization_employee_id,raw_mart_utilization_date " \
                          "FROM df_rawMart_union " \
                          "GROUP BY m_user_id_user,m_cabang_id_cabang, " \
                          "raw_mart_utilization_employee_id,raw_mart_utilization_date "
            df_daily = spark.sql(spark_query)

            aDaily = []
            aDailyDel = []
            for rowdd in df_daily.rdd.collect():
                aDailyDel.append("(m_user_id_user=" + str(rowdd['m_user_id_user'])
                                 + " AND m_cabang_id_cabang=" + str(rowdd['m_cabang_id_cabang'])
                                 + " AND mart_utilization_daily_date='" + convertToAsiaJakarta(rowdd['raw_mart_utilization_date']) + "'"
                                 + " AND mart_utilization_daily_employee_id="+str(rowdd['raw_mart_utilization_employee_id'])+")")
                aDaily.append("(m_user_id_user=" + str(rowdd['m_user_id_user'])
                              + " AND m_cabang_id_cabang=" + str(rowdd['m_cabang_id_cabang'])
                              + " AND raw_mart_utilization_date='" + convertToAsiaJakarta(rowdd['raw_mart_utilization_date']) + "'"
                              + " AND raw_mart_utilization_employee_id="+str(rowdd['raw_mart_utilization_employee_id'])+")")
            aaDaily = [aDaily[i:i + n_size_insert] for i in range(0, len(aDaily), n_size_insert)]
            aaDailyDel = [aDailyDel[i:i + n_size_insert] for i in range(0, len(aDailyDel), n_size_insert)]

            # executor    = ThreadPoolExecutor()
            cleanJobs   = []

            for axDaily in aaDailyDel:
                sWhereOrDaily = " OR ".join(axDaily)
                sql_query = "DELETE FROM mart_prd.mart_utilization_daily WHERE " + sWhereOrDaily
                cleanJobs.append(executor.submit(executePostgres, sql_query, conf_postgres_target))

            for job in cleanJobs:
                job.result()

            for axDaily in aaDaily:
                sWhereOrDaily = " OR ".join(axDaily)
                sql_query = F"""
                    insert into mart_prd.mart_utilization_daily(
                    	mart_utilization_daily_no,
                    	m_user_id_user,
                    	m_cabang_id_cabang,
                    	mart_utilization_daily_employee_id,
                    	mart_utilization_daily_duration,
                    	mart_utilization_daily_date,
                    	mart_utilization_daily_price)
                    select
                    	public.uuid_generate_v1mc() as mart_utilization_daily_no,
                    	m_user_id_user,
                    	m_cabang_id_cabang,
                    	raw_mart_utilization_employee_id as mart_utilization_daily_employee_id,
                    	SUM(raw_mart_utilization_duration) as mart_utilization_daily_duration,
                    	raw_mart_utilization_date as mart_utilization_daily_date,
                    	SUM(raw_mart_utilization_price) as mart_utilization_daily_price
                    from
                    	mart_prd.raw_mart_utilization
                    WHERE {sWhereOrDaily}
                    group by
                    	m_user_id_user,
                    	m_cabang_id_cabang,
                    	raw_mart_utilization_employee_id,
                    	raw_mart_utilization_date
                """
                executePostgres(sql_query,conf_postgres_target)

            executor.shutdown()

    for tobeclear in cachedTable:
        spark.catalog.dropTempView(tobeclear)


    # EXTRACT LOAD RAW MART SUMMARY RESERVATION STATUS
    sql_query_summary = f"""
        SELECT
            uuid() as raw_mart_summary_reservation_no,
            r.id as raw_mart_summary_reservation_id_reservation,
            r.merchant_refno as m_user_id_user,
            r.outlet_refno as m_cabang_id_cabang,
            r.reservation_date as raw_mart_summary_reservation_date,
            CASE
            	WHEN r.status = 700 THEN 1
            	WHEN r.status = 15 THEN 3
            	ELSE 2
            END as raw_mart_summary_reservation_status,
            r.reservation_type as raw_mart_summary_reservation_type
            FROM reservation r
            WHERE
            	r.updated_at BETWEEN '{sCurrent}' AND '{sAdd}'
                AND r.status NOT IN (9)
    """

    cachedTable = []
    timelog = {}

    df_rService_sum_name = "df_rService_sum"
    df_rService_sum = spark.read.format("jdbc").option("url",con_mysql_source) \
        .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "(" + sql_query_summary + ") sdtable") \
        .option("user", conf_mysql_source['USER']).option("password", conf_mysql_source['PASS']).load()
    df_rService_sum.createOrReplaceTempView(df_rService_sum_name)
    spark.catalog.cacheTable(df_rService_sum_name)
    cachedTable.append(df_rService_sum_name)


    if (df_rService_sum.count() > 0):
        aID = []
        for row in df_rService_sum.rdd.collect():
            aID.append(str(row['raw_mart_summary_reservation_id_reservation']))

        aaID = [aID[i:i + n_size] for i in range(0, len(aID), n_size)]

        df_rawMartSumReserve_name = "df_raw_mart_summary_reservation"
        spark_query_sum = "SELECT raw_mart_summary_reservation_no" \
                        ", raw_mart_summary_reservation_id_reservation" \
                        ", m_user_id_user" \
                        ", m_cabang_id_cabang" \
                        ", raw_mart_summary_reservation_date" \
                        ", raw_mart_summary_reservation_status" \
                        ", raw_mart_summary_reservation_type" \
                        " FROM df_rService_sum"
        df_raw_sum_reserve = spark.sql(spark_query_sum).fillna(value=0)
        df_raw_sum_reserve.createOrReplaceTempView(df_rawMartSumReserve_name)
        spark.catalog.cacheTable(df_rawMartSumReserve_name)
        cachedTable.append(df_rawMartSumReserve_name)

        # CLEANSING DATA RAW DATA MART DENGAN PARAMETER ID_TRANSACTIONS SEBELUM DIISI DENGAN DATA UPDATE TERBARU

        executor    = ThreadPoolExecutor()
        cleanJobs   = []

        for axID in aaID:
            strWhereID = ",".join(axID)
            sql_query = "DELETE FROM mart_prd.raw_mart_summary_reservation WHERE raw_mart_summary_reservation_id_reservation in (" + strWhereID + ")"
            cleanJobs.append(executor.submit(executePostgres, sql_query, conf_postgres_target))

        for job in cleanJobs:
            job.result()

        # write ke mysql untuk raw datamart di mysql
        df_raw_sum_reserve.write.jdbc(url=con_postgres_target, table="mart_prd.raw_mart_summary_reservation", mode=mode,properties=properties_target)

        # DAILY
        spark_query = "SELECT m_user_id_user,m_cabang_id_cabang," \
                      "raw_mart_summary_reservation_date,raw_mart_summary_reservation_status," \
                      "raw_mart_summary_reservation_type " \
                      "FROM df_rService_sum GROUP BY m_user_id_user,m_cabang_id_cabang," \
                      "raw_mart_summary_reservation_status,raw_mart_summary_reservation_date," \
                      "raw_mart_summary_reservation_type"
        df_daily = spark.sql(spark_query)

        aDaily = []
        aDailyDel = []
        for rowdd in df_daily.rdd.collect():
            aDailyDel.append("(m_user_id_user=" + str(rowdd['m_user_id_user'])
                             + " AND m_cabang_id_cabang=" + str(rowdd['m_cabang_id_cabang'])
                             + " AND mart_summary_reservation_daily_type=" + str(rowdd['raw_mart_summary_reservation_type'])
                             + " AND mart_summary_reservation_daily_date='" + convertToAsiaJakarta(rowdd['raw_mart_summary_reservation_date']) + "'"
                             + " AND mart_summary_reservation_daily_status="+str(rowdd['raw_mart_summary_reservation_status'])+")")
            aDaily.append("(m_user_id_user=" + str(rowdd['m_user_id_user'])
                          + " AND m_cabang_id_cabang=" + str(rowdd['m_cabang_id_cabang'])
                          + " AND raw_mart_summary_reservation_type=" + str(rowdd['raw_mart_summary_reservation_type'])
                          + " AND raw_mart_summary_reservation_date='" + convertToAsiaJakarta(rowdd['raw_mart_summary_reservation_date']) + "'"
                          + " AND raw_mart_summary_reservation_status="+str(rowdd['raw_mart_summary_reservation_status'])+")")
        aaDaily = [aDaily[i:i + n_size_insert] for i in range(0, len(aDaily), n_size_insert)]
        aaDailyDel = [aDailyDel[i:i + n_size_insert] for i in range(0, len(aDailyDel), n_size_insert)]

        cleanJobs   = []

        for axDaily in aaDailyDel:
            sWhereOrDaily = " OR ".join(axDaily)
            sql_query = "DELETE FROM mart_prd.mart_summary_reservation_daily WHERE " + sWhereOrDaily
            cleanJobs.append(executor.submit(executePostgres, sql_query, conf_postgres_target))

        for job in cleanJobs:
            job.result()

        for axDaily in aaDaily:
            sWhereOrDaily = " OR ".join(axDaily)
            sql_query = "INSERT INTO mart_prd.mart_summary_reservation_daily(mart_summary_reservation_daily_no," \
                        "m_user_id_user,m_cabang_id_cabang,mart_summary_reservation_daily_date," \
                        "mart_summary_reservation_daily_status, mart_summary_reservation_daily_total,mart_summary_reservation_daily_type) " \
                        "SELECT public.uuid_generate_v1mc() as mart_summary_reservation_daily_no " \
                        ",m_user_id_user,m_cabang_id_cabang" \
                        ",raw_mart_summary_reservation_date as mart_summary_reservation_daily_date" \
                        ",raw_mart_summary_reservation_status as mart_summary_reservation_daily_status" \
                        ",COUNT(raw_mart_summary_reservation_status) as mart_summary_reservation_daily_total" \
                        ",raw_mart_summary_reservation_type as mart_summary_reservation_daily_type " \
                        "FROM mart_prd.raw_mart_summary_reservation WHERE " + sWhereOrDaily + " GROUP BY " \
                        "m_user_id_user,m_cabang_id_cabang,raw_mart_summary_reservation_status," \
                        "raw_mart_summary_reservation_date,raw_mart_summary_reservation_type"
            # print(sql_query)
            executePostgres(sql_query,conf_postgres_target)

        executor.shutdown()

    for tobeclear in cachedTable:
        spark.catalog.dropTempView(tobeclear)

    dateEnd = (time.time() * 1000)
    dfHistory = createDataFrameHistoryLog(spark, t_startupdatedate, t_endupdatedate, df_utilizationd_count, dateEnd - dateStart,json.dumps(param))
    dfHistory.write.jdbc(url=con_postgres_target + '&stringtype=unspecified', table="mart_prd.raw_datamart_utilization_history", mode=mode,properties=properties_target)
    return {'sCurrent': str(t_startupdatedate), 'sEnd': str(t_endupdatedate), 'tProcess': str(dateEnd - dateStart),
            'nrow': str(df_utilizationd_count),'param':param}


#with param idStore, idOutlet, start, end
def processServiceUtilizationParam(appName,sEmail,iIDOutlet,sStart,sEnd):
    conf = helper.get_spark_config(appName)

    spark = SparkSession.builder.config(conf=conf).getOrCreate() #init spark
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)

    #for jumlah loop data per 100 data
    n_size = 100
    n_size_insert = 100

    mode = "append"
    properties_target = {"user": conf_postgres_target['USER'], "password": conf_postgres_target['PASS'],"driver": "org.postgresql.Driver"}
    # print(sCurrent)
    dfrec_utilizationd_count = 0
    dateStart = (time.time() * 1000)

    if sEmail is None:
        iIDStore = '0'
    else:
        sql_get_idStore = "SELECT CASE WHEN user_has_parent = 0 THEN id_user ELSE user_has_parent END as id " \
                        "FROM master_klopos.m_user " \
                        "WHERE user_email = '"+sEmail+"' "
        row_idStore = fetchrow(sql_get_idStore,conf_postgres_target)

        if row_idStore is None:
            return {'status': 'failed', 'content': 'Email not found', 'enum': '1'}

        iIDStore = str(row_idStore[0])

    sWhereStore=""
    if iIDStore != '0':
        sWhereStore = "AND r.merchant_refno = "+iIDStore

    sWhereOutlet=""
    if iIDOutlet:
        sWhereOutlet = "AND r.outlet_refno = "+str(iIDOutlet)

    cachedTable = []

    # EXTRACT LOAD RAW MART SUMMARY RESERVATION STATUS
    sql_query_summary = f"""
        SELECT
            uuid() as raw_mart_summary_reservation_no,
            r.id as raw_mart_summary_reservation_id_reservation,
            r.merchant_refno as m_user_id_user,
            r.outlet_refno as m_cabang_id_cabang,
            r.reservation_date as raw_mart_summary_reservation_date,
            CASE
            	WHEN r.status = 700 THEN 1
            	WHEN r.status = 15 THEN 3
            	ELSE 2
            END as raw_mart_summary_reservation_status,
            r.reservation_type as raw_mart_summary_reservation_type
            FROM reservation r
            WHERE
            	r.updated_at BETWEEN '{sStart}' AND '{sEnd}'
                {sWhereStore}
                {sWhereOutlet}
                AND r.status NOT IN (9)
    """
    dfrec_rService_sum_name = f"dfrec_rService_sum_{iIDStore}"
    dfrec_rService_sum = spark.read.format("jdbc").option("url",con_mysql_source) \
        .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "(" + sql_query_summary + ") sdtable") \
        .option("user", conf_mysql_source['USER']).option("password", conf_mysql_source['PASS']).load()
    dfrec_rService_sum.createOrReplaceTempView(dfrec_rService_sum_name)
    spark.catalog.cacheTable(dfrec_rService_sum_name)
    cachedTable.append(dfrec_rService_sum_name)


    if (dfrec_rService_sum.count() > 0):
        aID = []
        for row in dfrec_rService_sum.rdd.collect():
            aID.append(str(row['raw_mart_summary_reservation_id_reservation']))

        aaID = [aID[i:i + n_size] for i in range(0, len(aID), n_size)]

        dfrec_raw_reserve_sum_name = f"raw_mart_summmary_reservation_{iIDStore}"
        spark_query_sum = "SELECT raw_mart_summary_reservation_no" \
                      ", raw_mart_summary_reservation_id_reservation" \
                      ", m_user_id_user" \
                      ", m_cabang_id_cabang" \
                      ", raw_mart_summary_reservation_date" \
                      ", raw_mart_summary_reservation_status" \
                      ", raw_mart_summary_reservation_type" \
                      f" FROM dfrec_rService_sum_{iIDStore}"
        dfrec_raw_sum = spark.sql(spark_query_sum).fillna(value=0)
        dfrec_raw_sum.createOrReplaceTempView(dfrec_raw_reserve_sum_name)
        spark.catalog.cacheTable(dfrec_raw_reserve_sum_name)
        cachedTable.append(dfrec_raw_reserve_sum_name)
        # CLEANSING DATA RAW DATA MART DENGAN PARAMETER ID_TRANSACTIONS SEBELUM DIISI DENGAN DATA UPDATE TERBARU

        executor    = ThreadPoolExecutor()
        cleanJobs   = []

        for axID in aaID:
            strWhereID = ",".join(axID)
            sql_query = "DELETE FROM mart_prd.raw_mart_summary_reservation WHERE raw_mart_summary_reservation_id_reservation in (" + strWhereID + ")"
            cleanJobs.append(executor.submit(executePostgres, sql_query, conf_postgres_target))

        for job in cleanJobs:
            job.result()

        # write ke mysql untuk raw datamart di mysql
        dfrec_raw_sum.write.jdbc(url=con_postgres_target, table="mart_prd.raw_mart_summary_reservation", mode=mode,properties=properties_target)

        # DAILY
        spark_query = "SELECT m_user_id_user,m_cabang_id_cabang," \
                      "raw_mart_summary_reservation_date,raw_mart_summary_reservation_status," \
                      "raw_mart_summary_reservation_type " \
                      f"FROM dfrec_rService_sum_{iIDStore} GROUP BY m_user_id_user,m_cabang_id_cabang," \
                      "raw_mart_summary_reservation_status,raw_mart_summary_reservation_date," \
                      "raw_mart_summary_reservation_type"
        df_daily = spark.sql(spark_query)

        aDaily = []
        aDailyDel = []
        for rowdd in df_daily.rdd.collect():
            aDailyDel.append("(m_user_id_user=" + str(rowdd['m_user_id_user'])
                             + " AND m_cabang_id_cabang=" + str(rowdd['m_cabang_id_cabang'])
                             + " AND mart_summary_reservation_daily_type=" + str(rowdd['raw_mart_summary_reservation_type'])
                             + " AND mart_summary_reservation_daily_date='" + convertToAsiaJakarta(rowdd['raw_mart_summary_reservation_date']) + "'"
                             + " AND mart_summary_reservation_daily_status="+str(rowdd['raw_mart_summary_reservation_status'])+")")
            aDaily.append("(m_user_id_user=" + str(rowdd['m_user_id_user'])
                          + " AND m_cabang_id_cabang=" + str(rowdd['m_cabang_id_cabang'])
                          + " AND raw_mart_summary_reservation_type=" + str(rowdd['raw_mart_summary_reservation_type'])
                          + " AND raw_mart_summary_reservation_date='" + convertToAsiaJakarta(rowdd['raw_mart_summary_reservation_date']) + "'"
                          + " AND raw_mart_summary_reservation_status="+str(rowdd['raw_mart_summary_reservation_status'])+")")

        aaDaily = [aDaily[i:i + n_size_insert] for i in range(0, len(aDaily), n_size_insert)]
        aaDailyDel = [aDailyDel[i:i + n_size_insert] for i in range(0, len(aDailyDel), n_size_insert)]

        # executor    = ThreadPoolExecutor()
        cleanJobs   = []

        for axDaily in aaDailyDel:
            sWhereOrDaily = " OR ".join(axDaily)
            sql_query = "DELETE FROM mart_prd.mart_summary_reservation_daily WHERE " + sWhereOrDaily
            cleanJobs.append(executor.submit(executePostgres, sql_query, conf_postgres_target))
        for job in cleanJobs:
            job.result()

        for axDaily in aaDaily:
            sWhereOrDaily = " OR ".join(axDaily)
            sql_query = "INSERT INTO mart_prd.mart_summary_reservation_daily(mart_summary_reservation_daily_no," \
                        "m_user_id_user,m_cabang_id_cabang,mart_summary_reservation_daily_date," \
                        "mart_summary_reservation_daily_status, mart_summary_reservation_daily_total," \
                        "mart_summary_reservation_daily_type) " \
                        "SELECT public.uuid_generate_v1mc() as mart_summary_reservation_daily_no " \
                        ",m_user_id_user,m_cabang_id_cabang" \
                        ",raw_mart_summary_reservation_date as mart_summary_reservation_daily_date" \
                        ",raw_mart_summary_reservation_status as mart_summary_reservation_daily_status" \
                        ",COUNT(raw_mart_summary_reservation_status) as mart_summary_reservation_daily_total" \
                        ",raw_mart_summary_reservation_type as mart_summary_reservation_daily_type" \
                        " FROM mart_prd.raw_mart_summary_reservation WHERE " + sWhereOrDaily + " GROUP BY " \
                        "m_user_id_user,m_cabang_id_cabang,raw_mart_summary_reservation_status," \
                        "raw_mart_summary_reservation_date, raw_mart_summary_reservation_type"
            # print(sql_query)
            executePostgres(sql_query,conf_postgres_target)
        executor.shutdown()

    for tobeclear in cachedTable:
        spark.catalog.dropTempView(tobeclear)


    sql_query = f"""
        SELECT
        	r.id,
        	r.reservation_no,
        	r.merchant_refno m_user_id_user,
        	r.outlet_refno m_cabang_id_cabang,
        	r.customer_id,
        	r.status,
        	r.updated_at
        FROM
        	reservation r
        WHERE
            r.reservation_date BETWEEN '{sStart}' and '{sEnd}'
            {sWhereStore}
            {sWhereOutlet}
    """
    # print(sql_query)

    cachedTable = []
    timelog = {}

    dfrec_utilization_name = "dfrec_utilization"+iIDStore
    dfrec_utilization = spark.read.format("jdbc").option("url",con_mysql_source) \
        .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "(" + sql_query + ") sdtable") \
        .option("user", conf_mysql_source['USER']).option("password", conf_mysql_source['PASS']).load()
    dfrec_utilization.createOrReplaceTempView(dfrec_utilization_name)
    spark.catalog.cacheTable(dfrec_utilization_name)
    cachedTable.append(dfrec_utilization_name)

    sWhereOrDaily = ""
    sts_recovery = ""

    if (dfrec_utilization.count() > 0):
        aID = []
        aIDDEl = []
        for row in dfrec_utilization.rdd.collect():
            if(row['status'] == '700' or row['status'] == '9'):
                aID.append(str(row['id']))
            else:
                aIDDEl.append(str(row['id']))

        aaID = [aID[i:i + n_size] for i in range(0, len(aID), n_size)]
        aaIDDel = [aIDDEl[i:i + n_size] for i in range(0, len(aIDDEl), n_size)]

        dfrec_utilizationd = None
        dfrec_utilizationd_name = "dfrec_utilizationd"+iIDStore
        for axID in (aaID+aaIDDel):
            strWhereID = ",".join(axID)
            sql_query = f"""
                SELECT
                	rs.id idd,
                    rs.reservation_id id,
                	rs.product_refno,
                	rs.reservation_service_date date,
                	rs.reservation_service_start_time start_time,
                	rs.reservation_service_end_time end_time,
                    CASE
                        WHEN ((TIME_TO_SEC(rs.reservation_service_end_time)/60 - TIME_TO_SEC(rs.reservation_service_start_time)/60)) < 0 THEN
                            CAST((((TIME_TO_SEC(rs.reservation_service_end_time)/60 - TIME_TO_SEC(rs.reservation_service_start_time)/60)) * -1) AS UNSIGNED)
                        ELSE
                            CAST(((TIME_TO_SEC(rs.reservation_service_end_time)/60 - TIME_TO_SEC(rs.reservation_service_start_time)/60)) AS UNSIGNED)
                    END AS duration,
                	rs.employee_refno,
                	rs.reservation_service_total_price price
                FROM
                	reservation_service rs
                WHERE
                    rs.reservation_service_date is not null and
                    rs.reservation_service_is_parent = 1 and
                    rs.reservation_id IN ({strWhereID})
                """
            # print(sql_query)

            df_temp = spark.read.format("jdbc").option("url", con_mysql_source) \
                .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "(" + sql_query + ") sdtable") \
                .option("user", conf_mysql_source['USER']).option("password", conf_mysql_source['PASS']).load()
            if(dfrec_utilizationd==None):
                dfrec_utilizationd = df_temp
            else:
                dfrec_utilizationd = dfrec_utilizationd.union(df_temp)

        if(dfrec_utilizationd!=None):
            dfrec_utilizationd.createOrReplaceTempView(dfrec_utilizationd_name)
            spark.catalog.cacheTable(dfrec_utilizationd_name)
            cachedTable.append(dfrec_utilizationd_name)
            dfrec_utilizationd_count = dfrec_utilizationd.count()

        if (dfrec_utilizationd!=None and dfrec_utilizationd_count > 0):
            # CLEANSING DATA RAW DATA MART DENGAN PARAMETER ID_TRANSACTIONS SEBELUM DIISI DENGAN DATA UPDATE TERBARU
            # INSERT DATA RAW DATA MART DENGAN DATA TERBARU
            # JOIN ON SPARK untuk transaction & transaction detail
            dfrec_rawMartReserve_name = f"dfrec_raw_mart_reservation_{iIDStore}"
            spark_query = "SELECT " \
                        f"  {dfrec_utilizationd_name}.idd as raw_mart_utilization_id_reservation_detail " \
                        f", {dfrec_utilization_name}.id as raw_mart_utilization_id_reservation " \
                        f", {dfrec_utilization_name}.m_user_id_user " \
                        f", {dfrec_utilization_name}.m_cabang_id_cabang " \
                        f", {dfrec_utilization_name}.customer_id as raw_mart_utilization_customer_id " \
                        f", {dfrec_utilizationd_name}.employee_refno as raw_mart_utilization_employee_id " \
                        f", {dfrec_utilizationd_name}.product_refno as raw_mart_utilization_product_id " \
                        f", {dfrec_utilizationd_name}.date as raw_mart_utilization_date " \
                        f", {dfrec_utilizationd_name}.start_time as raw_mart_utilization_start_time " \
                        f", {dfrec_utilizationd_name}.end_time as raw_mart_utilization_end_time " \
                        f", {dfrec_utilizationd_name}.duration as raw_mart_utilization_duration " \
                        f", {dfrec_utilizationd_name}.price as raw_mart_utilization_price " \
                        f" FROM {dfrec_utilization_name} JOIN {dfrec_utilizationd_name}" \
                        f" ON {dfrec_utilization_name}.id={dfrec_utilizationd_name}.id" \
                        f" WHERE {dfrec_utilization_name}.status IN ('700','9') "

            dfrec_raw_mart_reservation = spark.sql(spark_query).fillna(value=0)
            dfrec_raw_mart_reservation.createOrReplaceTempView(dfrec_rawMartReserve_name)
            spark.catalog.cacheTable(dfrec_rawMartReserve_name)
            cachedTable.append(dfrec_rawMartReserve_name)
            # CLEANSING DATA RAW DATA MART DENGAN PARAMETER idStore, range date and idOutlet

            sStart = datetime.strptime(sStart[0:10], "%Y-%m-%d")
            sEnd = datetime.strptime(sEnd[0:10], "%Y-%m-%d")

            # fetch to be deleted raw -> this is used to clean mart
            df_name_raw_mart_deleted = "df_rawmart_del"
            df_raw_mart_deleted = None
            for chunked_deleted_ids in aaIDDel:
                sql_query = """
                    SELECT DISTINCT
                        m_user_id_user,
                        m_cabang_id_cabang,
                        raw_mart_utilization_employee_id,
                        raw_mart_utilization_date
                    FROM mart_prd.raw_mart_utilization
                    WHERE raw_mart_utilization_id_reservation in ({deleted_transactions})
                """.format(deleted_transactions=",".join([id_trx for id_trx in chunked_deleted_ids ]))

                df_temp = spark.read.format("jdbc").option("url", con_postgres_target) \
                                .option("driver", "org.postgresql.Driver").option("dbtable", "(" + sql_query + ") sdtable") \
                                .option("user", conf_postgres_target['USER']).option("password", conf_postgres_target['PASS']).load()
                df_raw_mart_deleted = df_temp if df_raw_mart_deleted == None else df_raw_mart_deleted.union(df_temp)

            tempViews = []
            if(df_raw_mart_deleted != None):
                df_raw_mart_deleted.createOrReplaceTempView(df_name_raw_mart_deleted)
                spark.catalog.cacheTable(df_name_raw_mart_deleted)
                tempViews.append(df_name_raw_mart_deleted)

            executor    = ThreadPoolExecutor()
            cleanJobs   = []

            for axID in aaID:
                strWhereID = ",".join(axID)
                delOutlet = f' AND m_cabang_id_cabang = {iIDOutlet}' if iIDOutlet else ''
                sql_query = f"""
                    DELETE FROM mart_prd.raw_mart_utilization
                    WHERE raw_mart_utilization_id_reservation in ({strWhereID})
                    AND raw_mart_utilization_date BETWEEN '{sStart}' AND '{sEnd}'
                    AND m_user_id_user = {iIDStore} {delOutlet}
                """
                cleanJobs.append(executor.submit(executePostgres, sql_query, conf_postgres_target))

            for job in cleanJobs:
                job.result()

            dfrec_rawmart_name = "dfrec_rawmart"+iIDStore
            spark_query_with_del = "SELECT m_user_id_user,m_cabang_id_cabang " \
                          ", employee_refno as raw_mart_utilization_employee_id " \
                          ", date as raw_mart_utilization_date " \
                          f" FROM {dfrec_utilization_name} JOIN {dfrec_utilizationd_name}" \
                          f" ON {dfrec_utilization_name}.id={dfrec_utilizationd_name}.id" \
                          f" WHERE {dfrec_utilization_name}.status='700'"

            dfrec_join_with_del = spark.sql(spark_query_with_del).fillna(value=0)
            dfrec_join_with_del.createOrReplaceTempView(dfrec_rawmart_name)
            spark.catalog.cacheTable(dfrec_rawmart_name)
            cachedTable.append(dfrec_rawmart_name)

            # CLEANSING DATA RAW DATA MART DENGAN PARAMETER ID_TRANSACTIONS SEBELUM DIISI DENGAN DATA UPDATE TERBARU
            # DELETE UNTUK STATUS 9 DAN 15 IDDEL

            # executor    = ThreadPoolExecutor()
            cleanJobs   = []

            for axID in aaIDDel:
                strWhereID = ",".join(axID)
                delOutlet = f' AND m_cabang_id_cabang = {iIDOutlet}' if iIDOutlet else ''
                sql_query = f"""
                    DELETE FROM mart_prd.raw_mart_utilization
                    WHERE raw_mart_utilization_id_reservation in ({strWhereID})
                    AND raw_mart_utilization_date BETWEEN '{sStart}' AND '{sEnd}'
                    AND m_user_id_user = {iIDStore} {delOutlet}
                """
                cleanJobs.append(executor.submit(executePostgres, sql_query, conf_postgres_target))

            for job in cleanJobs:
                job.result()

            # write ke mysql untuk raw datamart di mysql
            dfrec_raw_mart_reservation.write.jdbc(url=con_postgres_target, table="mart_prd.raw_mart_utilization", mode=mode,properties=properties_target)
            # hasil join kita simpen sebagai df_rawmart / df_Join

            # DAILY SERVICE
            spark_query = "SELECT m_user_id_user,m_cabang_id_cabang, " \
                          "raw_mart_utilization_employee_id,raw_mart_utilization_date " \
                          "FROM "+dfrec_rawmart_name+" " \
                          "GROUP BY m_user_id_user,m_cabang_id_cabang, " \
                          "raw_mart_utilization_employee_id,raw_mart_utilization_date "

            # print(spark_query)
            df_daily = spark.sql(spark_query)

            aDaily = []
            aDailyDel = []
            for rowdd in df_daily.rdd.collect():
                aDailyDel.append("(m_user_id_user=" + str(rowdd['m_user_id_user'])
                                 + " AND m_cabang_id_cabang=" + str(rowdd['m_cabang_id_cabang'])
                                 + " AND mart_utilization_daily_date='" + convertToAsiaJakarta(rowdd['raw_mart_utilization_date']) + "'"
                                 + " AND mart_utilization_daily_employee_id="+str(rowdd['raw_mart_utilization_employee_id'])+")")
                aDaily.append("(m_user_id_user=" + str(rowdd['m_user_id_user'])
                              + " AND m_cabang_id_cabang=" + str(rowdd['m_cabang_id_cabang'])
                              + " AND raw_mart_utilization_date='" + convertToAsiaJakarta(rowdd['raw_mart_utilization_date']) + "'"
                              + " AND raw_mart_utilization_employee_id="+str(rowdd['raw_mart_utilization_employee_id'])+")")
            aaDaily = [aDaily[i:i + n_size_insert] for i in range(0, len(aDaily), n_size_insert)]
            aaDailyDel = [aDailyDel[i:i + n_size_insert] for i in range(0, len(aDailyDel), n_size_insert)]

            # executor    = ThreadPoolExecutor()
            cleanJobs   = []

            for axDaily in aaDailyDel:
                sWhereOrDaily = " OR ".join(axDaily)
                sql_query = "DELETE FROM mart_prd.mart_utilization_daily WHERE " + sWhereOrDaily
                cleanJobs.append(executor.submit(executePostgres, sql_query, conf_postgres_target))

            for job in cleanJobs:
                job.result()

            for axDaily in aaDaily:
                sWhereOrDaily = " OR ".join(axDaily)
                sql_query = f"""
                    insert into mart_prd.mart_utilization_daily(
                    	mart_utilization_daily_no,
                    	m_user_id_user,
                    	m_cabang_id_cabang,
                    	mart_utilization_daily_employee_id,
                    	mart_utilization_daily_duration,
                    	mart_utilization_daily_date,
                    	mart_utilization_daily_price)
                    select
                    	public.uuid_generate_v1mc() as mart_utilization_daily_no,
                    	m_user_id_user,
                    	m_cabang_id_cabang,
                    	raw_mart_utilization_employee_id as mart_utilization_daily_employee_id,
                    	SUM(raw_mart_utilization_duration) as mart_utilization_daily_duration,
                    	raw_mart_utilization_date as mart_utilization_daily_date,
                    	SUM(raw_mart_utilization_price) as mart_utilization_daily_price
                    from
                    	mart_prd.raw_mart_utilization
                    WHERE {sWhereOrDaily}
                    group by
                    	m_user_id_user,
                    	m_cabang_id_cabang,
                    	raw_mart_utilization_employee_id,
                    	raw_mart_utilization_date
                """
                # print(sql_query)
                executePostgres(sql_query,conf_postgres_target)


            executor.shutdown()

        for tobeclear in cachedTable:
            spark.catalog.dropTempView(tobeclear)

        sts_recovery = 'running etl'
    else:
        executor = ThreadPoolExecutor()
        cleanJobs = []

        delOutlet = f' AND m_cabang_id_cabang = {iIDOutlet}' if iIDOutlet else ''

        # clean data raw mart
        del_rawmart = "DELETE FROM mart_prd.raw_mart_utilization " \
            "WHERE (raw_mart_utilization_date BETWEEN '" + str(sStart) + "' AND '" + str(sEnd) + "')  " \
            "AND m_user_id_user = "+iIDStore+" "\
            ""+delOutlet
        cleanJobs.append(( executePostgres, (del_rawmart,conf_postgres_target)))

        # clean data mart daily
        del_martDaily = "DELETE FROM mart_prd.mart_utilization_daily " \
            "WHERE (mart_utilization_daily_date BETWEEN '" + str(sStart) + "' AND '" + str(sEnd) + "')  " \
            "AND m_user_id_user = "+iIDStore+" "\
            ""+delOutlet
        cleanJobs.append(( executePostgres, (del_martDaily,conf_postgres_target)))

        # clean data raw mart summary
        del_rawmart = "DELETE FROM mart_prd.raw_mart_summary_reservation " \
            "WHERE (raw_mart_summary_reservation_date BETWEEN '" + str(sStart) + "' AND '" + str(sEnd) + "')  " \
            "AND m_user_id_user = "+iIDStore+" "\
            ""+delOutlet
        cleanJobs.append(( executePostgres, (del_rawmart,conf_postgres_target)))

        # clean data mart summary daily
        del_martDaily = "DELETE FROM mart_prd.mart_summary_reservation_daily " \
            "WHERE (mart_summary_reservation_daily_date BETWEEN '" + str(sStart) + "' AND '" + str(sEnd) + "')  " \
            "AND m_user_id_user = "+iIDStore+" "\
            ""+delOutlet
        cleanJobs.append(( executePostgres, (del_martDaily,conf_postgres_target)))

        start_time = time.time()
        for cleanJob in [executor.submit(partial(func, *args)) for func, args in cleanJobs]:
            timelogJob = cleanJob.result()
        timelog['cleaning_datas'] = time.time() - start_time
        executor.shutdown()

        sts_recovery = 'cleaning data'

    dateEnd = (time.time() * 1000)
    content = {'sCurrent': str(sStart), 'sEnd': str(sEnd), 'tProcess': str(dateEnd - dateStart), 'nrow': str(dfrec_utilizationd_count), 'sts': sts_recovery}
    return {'status': 'success', 'content': content, 'enum': '1'}
