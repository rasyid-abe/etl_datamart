from datetime import datetime
from datetime import timedelta
import json
import time
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.functions import *
from Utility.sql import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial, reduce
import helper
import pytz

conf_mysql_source = helper.getConfMySQLMayang()
con_mysql_source = helper.generate_mysql_connection_url(conf_mysql_source)

conf_postgres_target = helper.getConfPostgresMart()
con_postgres_target = helper.generate_postgres_connection_url(conf_postgres_target)

def processComplimentSequencePerMin(seq):
    sql_max_ts_current = "SELECT MAX(ts_end) at time zone 'Asia/Jakarta' FROM mart_prd.raw_datamart_compliment_history"
    row_max_ts_current = fetchrow(sql_max_ts_current,conf_postgres_target)
    sTimeStart = row_max_ts_current[0]
    sql_count_ts_current = "SELECT count(ts_end) FROM mart_prd.raw_datamart_compliment_history WHERE ts_end='"+convertToAsiaJakarta(sTimeStart)+"'"
    row_count_ts_current = fetchrow(sql_count_ts_current,conf_postgres_target)
    nTimeStart = row_count_ts_current[0]
    sTimeEnd=sTimeStart+timedelta(minutes=(seq*nTimeStart))
    sequence = "sequence"+str(seq)+"min"
    return processComplimentParamBegEndTime('pyspark-postgresql-'+str(seq)+'minute-sequence-api-addon', sTimeStart, sTimeEnd,{'fn': sequence,'start_time':str(sTimeStart),'end_time':str(sTimeEnd)})

def processComplimentSequence5Min():
    sql_max_ts_current = "SELECT MAX(ts_end) at time zone 'Asia/Jakarta' FROM mart_prd.raw_datamart_compliment_history"
    row_max_ts_current = fetchrow(sql_max_ts_current,conf_postgres_target)
    sTimeStart = row_max_ts_current[0]
    sql_count_ts_current = "SELECT count(ts_end) FROM mart_prd.raw_datamart_compliment_history WHERE ts_end='"+convertToAsiaJakarta(sTimeStart)+"'"
    row_count_ts_current = fetchrow(sql_count_ts_current,conf_postgres_target)
    nTimeStart = row_count_ts_current[0]
    sTimeEnd=sTimeStart+timedelta(minutes=(5*nTimeStart))
    return processComplimentParamBegEndTime('pyspark-postgresql-5minute-sequence-api-addon', sTimeStart, sTimeEnd,{'fn': 'sequence5min','start_time':str(sTimeStart),'end_time':str(sTimeEnd)})

def processComplimentSequence45Min():
    sql_max_ts_current = "SELECT MAX(ts_end) at time zone 'Asia/Jakarta' FROM mart_prd.raw_datamart_compliment_history"
    row_max_ts_current = fetchrow(sql_max_ts_current,conf_postgres_target)
    sTimeStart = row_max_ts_current[0]
    sql_count_ts_current = "SELECT count(ts_end) FROM mart_prd.raw_datamart_compliment_history WHERE ts_end='"+convertToAsiaJakarta(sTimeStart)+"'"
    row_count_ts_current = fetchrow(sql_count_ts_current,conf_postgres_target)
    nTimeStart = row_count_ts_current[0]
    sTimeEnd=sTimeStart+timedelta(minutes=(45*nTimeStart))
    return processComplimentParamBegEndTime('pyspark-postgresql-45minute-sequence-api-addon', sTimeStart, sTimeEnd,{'fn': 'sequence45min','start_time':str(sTimeStart),'end_time':str(sTimeEnd)})

def processComplimentSequence120Min():
    sql_max_ts_current = "SELECT MAX(ts_end) at time zone 'Asia/Jakarta' FROM mart_prd.raw_datamart_compliment_history"
    row_max_ts_current = fetchrow(sql_max_ts_current,conf_postgres_target)
    sTimeStart = row_max_ts_current[0]
    sql_count_ts_current = "SELECT count(ts_end) FROM mart_prd.raw_datamart_compliment_history WHERE ts_end='"+convertToAsiaJakarta(sTimeStart)+"'"
    row_count_ts_current = fetchrow(sql_count_ts_current,conf_postgres_target)
    nTimeStart = row_count_ts_current[0]
    sTimeEnd=sTimeStart+timedelta(minutes=(120*nTimeStart))
    return processComplimentParamBegEndTime('pyspark-postgresql-120minute-sequence-api-addon', sTimeStart, sTimeEnd,{'fn': 'sequence120min','start_time':str(sTimeStart),'end_time':str(sTimeEnd)})

def processComplimentParamEndTimePeriod(end_time,period):
    sql_max_ts_current = "SELECT MAX(ts_end) at time zone 'Asia/Jakarta' FROM mart_prd.raw_datamart_compliment_history"
    row_max_ts_current = fetchrow(sql_max_ts_current,conf_postgres_target)
    sTimeStart = row_max_ts_current[0]
    sTimeEnd=sTimeStart+timedelta(minutes=period)
    if(sTimeEnd < end_time):
        return processComplimentParamBegEndTime('pyspark-postgresql-period-endtimeperiod-api',sTimeStart,sTimeEnd,{'fn':'endtimeperiod','end_time':str(end_time),'period':str(period)})
    else:
        return processComplimentParamBegEndTime('pyspark-postgresql-period-endtimeperiod-api', sTimeStart, end_time,
                                                  {'fn': 'endtimeperiod', 'end_time': str(end_time),
                                                   'period': str(period)})

# literally start & end time.
def processComplimentParamBegEndTime(appName, sCurrent, sAdd, param):
    conf = helper.get_spark_config(appName)

    spark = SparkSession.builder.config(conf=conf).getOrCreate() #init spark
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)

    datenow = datetime.today().astimezone(tz=pytz.timezone('Asia/Jakarta'))

    # ============================================================
    # sCurrent = datetime.strptime(sCurrent,"%Y-%m-%d %H:%M:%S")
    # sAdd = datetime.strptime(sAdd,"%Y-%m-%d %H:%M:%S")
    # print(sCurrent)
    # print(sAdd)

    n_size = 100
    n_size_insert = 100

    mode = "append"
    properties_target = {"user": conf_postgres_target['USER'], "password": conf_postgres_target['PASS'],"driver": "org.postgresql.Driver"}
    # print(sCurrent)
    df_transComplimentDet_count = 0
    dateStart = (time.time() * 1000)
    t_start = (time.time() * 1000)

    # prevent data be left behind over microseconds
    sCurrent = sCurrent-timedelta(seconds=5)

    sql_query = "SELECT ts.updatedate, ts.M_User_id_user, ts.M_Cabang_id_cabang, ts.transaction_no_nota, " \
	            "ts.transaction_tgl as transaction_tgl, " \
	            "ts.transaction_id_kasir_bayar kasir, ts.transaction_catatan, " \
	            "ts.transaction_jumlah_pesanan,	ts.transaction_otoritas auth, ts.id_transaction, ts.status " \
                "FROM Transactions ts " \
                "WHERE ts.status IN ('1','4','9') AND Transaction_purpose = '5' " \
                "AND ts.transaction_otoritas IS NOT NULL " \
                "AND ts.transaction_tgl > '1970-01-01 00:00:00' " \
                "AND ts.transaction_id_kasir_bayar IS NOT NULL " \
	            "AND ts.updatedate BETWEEN '" + str(sCurrent) + "' AND '" + str(sAdd) + "' "
    # print(sql_query)

    df_transCompliment = spark.read.format("jdbc").option("url", con_mysql_source) \
        .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "(" + sql_query + ") sdtable") \
        .option("user", conf_mysql_source['USER']).option("password", conf_mysql_source['PASS']).load()
    df_transCompliment.createOrReplaceTempView("df_transCompliment")

    df_updatedate = spark.sql(
        "SELECT MAX(updateDate) max_update_date, "\
        "MIN(updateDate) min_update_date "\
        "FROM df_transCompliment"
    )

    sWhereOrHourly = ""
    sWhereOrDaily = ""
    #handle if record not found updatedate in range between sCurrent n sAdd
    t_startupdatedate = sCurrent

    #handle jika tgl terakhir updatedate + 5 minute di atas tgl skr
    if sAdd.strftime("%Y-%m-%d %H:%M:%S %z") > datenow.strftime("%Y-%m-%d %H:%M:%S %z"):
        t_endupdatedate = sCurrent
    else:
        t_endupdatedate = sAdd
    if (df_transCompliment.count() > 0):

        t_endupdatedate = df_updatedate.first()['max_update_date']
        t_startupdatedate = df_updatedate.first()['min_update_date']
        # pprint(t_startupdatedate)

        # list id trx yg aktif
        trxId = []
        # list id trx yg di delete
        trxIdDel = []
        # list id trx detail
        trxDetailId = []

        for row in df_transCompliment.rdd.collect():
            if(row['status'] == '1'):
                trxId.append(str(row['id_transaction']))
            #elif(row['status'] == '9'):
            else:
                trxIdDel.append(str(row['id_transaction']))

        # list id trx aktif yg di segment per 100 id
        nTrxId = [trxId[i:i + n_size] for i in range(0, len(trxId), n_size)]
        # list id trx deleted yg di segment per 100 id
        nTrxIdDel = [trxIdDel[i:i + n_size] for i in range(0, len(trxIdDel), n_size)]

        df_transComplimentDet = None
        for rowTrx in (nTrxId+nTrxIdDel):
            strWhereID = ",".join(rowTrx)
            sql_query = "SELECT uuid() as id_comp, " \
                        "transaction_promo_calculated_value, Transactions_id_transaction " \
                        "FROM Transaction_has_Promo thp " \
                        "WHERE thp.M_Category_Promo_id_category_promo = 4 " \
                        "AND Transactions_id_transaction in(" + strWhereID + ") " \
                        "GROUP BY Transactions_id_transaction"
            # print(sql_query)

            df_temp = spark.read.format("jdbc").option("url", con_mysql_source) \
                .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "(" + sql_query + ") sdtable") \
                .option("user", conf_mysql_source['USER']).option("password", conf_mysql_source['PASS']).load()
            if(df_transComplimentDet == None):
                df_transComplimentDet = df_temp
            else:
                df_transComplimentDet = df_transComplimentDet.union(df_temp)
        if(df_transComplimentDet != None):
            df_transComplimentDet.createOrReplaceTempView("df_transComplimentDet")
            df_transComplimentDet_count = df_transComplimentDet.count()

        if (df_transComplimentDet != None and df_transComplimentDet_count > 0):
            # CLEANSING DATA RAW DATA MART DENGAN PARAMETER ID_TRANSACTIONS SEBELUM DIISI DENGAN DATA UPDATE TERBARU
            # INSERT DATA RAW DATA MART DENGAN DATA TERBARU
            # JOIN ON SPARK untuk transaction & transaction detail
            spark_query = "SELECT id_comp as raw_mart_compliment_no " \
                          ", id_transaction as raw_mart_compliment_transaction_id " \
                          ", M_User_id_user,M_Cabang_id_cabang " \
                          ", transaction_no_nota as raw_mart_compliment_no_nota " \
                          ", transaction_tgl as raw_mart_compliment_datetime " \
                          ", DATE(transaction_tgl) as raw_mart_compliment_date " \
                          ", kasir as raw_mart_compliment_cashier_id " \
                          ", transaction_jumlah_pesanan as raw_mart_compliment_product_qty " \
                          ", auth as raw_mart_compliment_auth_id " \
                          ", transaction_promo_calculated_value as raw_mart_compliment_value " \
                          ", transaction_catatan as raw_mart_compliment_note " \
                          " FROM df_transCompliment JOIN df_transComplimentDet " \
                          "ON df_transCompliment.id_transaction = df_transComplimentDet.Transactions_id_transaction " \
                          "WHERE status='1'"
            df_join = spark.sql(spark_query).fillna(value=0)

            # CLEANSING DATA RAW DATA MART DENGAN PARAMETER ID_TRANSACTIONS SEBELUM DIISI DENGAN DATA UPDATE TERBARU
            executor    = ThreadPoolExecutor()
            cleanJobs   = []

            for rowTrx in (nTrxId+nTrxIdDel):
                strWhereID = ",".join(rowTrx)
                sql_query = "DELETE FROM mart_prd.raw_mart_compliment WHERE raw_mart_compliment_transaction_id in (" + strWhereID + ")"
                cleanJobs.append(executor.submit(executePostgres, sql_query, conf_postgres_target))

            for job in cleanJobs:
                job.result()

            df_join.write.jdbc(url=con_postgres_target, table="mart_prd.raw_mart_compliment", mode=mode,properties=properties_target)
            # hasil join kita simpen sebagai df_rawMartAddon / df_Join

    dateEnd = (time.time() * 1000)
    dfHistory = createDataFrameHistoryLog(spark, t_startupdatedate, t_endupdatedate, df_transComplimentDet_count, dateEnd - dateStart,json.dumps(param))
    dfHistory.write.jdbc(url=con_postgres_target + '&stringtype=unspecified',
        table="mart_prd.raw_datamart_compliment_history", mode=mode,properties=properties_target)
    return {'sCurrent': str(t_startupdatedate), 'sEnd': str(t_endupdatedate), 'tProcess': str(dateEnd - dateStart),
            'nrow': str(df_transComplimentDet_count),'param':param}

#with param idStore, idOutlet, start, end
def processComplimentParam(appName,sEmail,iIDOutlet,sStart,sEnd, core_spec = None, driver_memory_spec = None, executor_memory_spec = None):
    conf = helper.get_spark_config(appName) if core_spec == None or driver_memory_spec == None or executor_memory_spec == None \
        else helper.get_spark_config(
            appName, \
            driver_memory=driver_memory_spec, \
            executor_memory=executor_memory_spec, \
            executor_core=core_spec)

    spark = SparkSession.builder.config(conf=conf).getOrCreate() #init spark
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)

    n_size = 100
    n_size_insert = 100

    mode = "append"
    properties_target = {"user": conf_postgres_target['USER'], "password": conf_postgres_target['PASS'],"driver": "org.postgresql.Driver"}
    # print(sCurrent)
    df_transComplimentDet_count = 0
    dateStart = (time.time() * 1000)

    if sEmail is None:
        iIDStore = '0'
    else:
        sql_get_idStore = "SELECT CASE WHEN user_has_parent = 0 THEN id_user ELSE user_has_parent END as id " \
                        "FROM master_klopos.M_User " \
                        "WHERE user_email = '"+sEmail+"' "
        row_idStore = fetchrow(sql_get_idStore,conf_postgres_target)

        if row_idStore is None:
            return {'status': 'failed', 'content': 'Email not found', 'enum': '1'}

        iIDStore = str(row_idStore[0])

    sWhereStore=""
    if iIDStore != '0':
        sWhereStore = "AND M_User_id_user = "+iIDStore

    sWhereOutlet=""
    if iIDOutlet:
        sWhereOutlet = "AND M_Cabang_id_cabang = "+str(iIDOutlet)

    sql_query = "SELECT ts.updatedate, ts.M_User_id_user, ts.M_Cabang_id_cabang, ts.transaction_no_nota, " \
	            "ts.transaction_tgl as transaction_tgl, " \
	            "ts.transaction_id_kasir_bayar kasir, ts.transaction_catatan, " \
	            "ts.transaction_jumlah_pesanan,	ts.transaction_otoritas auth, ts.id_transaction, ts.status " \
                "FROM Transactions ts " \
                "WHERE ts.status IN ('1') AND Transaction_purpose = '5' " \
                "AND ts.transaction_otoritas IS NOT NULL " \
                "AND ts.transaction_tgl > '1970-01-01 00:00:00' " \
                "AND ts.transaction_id_kasir_bayar IS NOT NULL " \
	            "AND ts.updatedate BETWEEN '" + str(sStart) + "' AND '" + str(sEnd) + "' " \
                ""+sWhereStore+" "\
                ""+sWhereOutlet
    # print(sql_query)

    df_transCompliment_name = "df_compliment_p"+iIDStore
    df_transCompliment = spark.read.format("jdbc").option("url", con_mysql_source) \
        .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "(" + sql_query + ") sdtable") \
        .option("user", conf_mysql_source['USER']).option("password", conf_mysql_source['PASS']).load()
    df_transCompliment.createOrReplaceTempView(df_transCompliment_name)

    sWhereOrHourly = ""
    sWhereOrDaily = ""
    sts_recovery = ""

    if (df_transCompliment.count() > 0):
        trxId = []
        trxDetailId = []

        for row in df_transCompliment.rdd.collect():
            trxId.append(str(row['id_transaction']))

        # list id trx aktif yg di segment per 100 id
        nTrxId = [trxId[i:i + n_size] for i in range(0, len(trxId), n_size)]

        df_transComplimentDet = None
        df_transComplimentDet_name = "df_compliment_p_det"+iIDStore
        for rowTrx in nTrxId:
            strWhereID = ",".join(rowTrx)
            sql_query = "SELECT UUID() as id_comp, " \
                        "transaction_promo_calculated_value, Transactions_id_transaction " \
                        "FROM Transaction_has_Promo thp " \
                        "WHERE thp.M_Category_Promo_id_category_promo = 4 " \
                        "AND Transactions_id_transaction in(" + strWhereID + ") " \
                        "GROUP BY Transactions_id_transaction"
            # print(sql_query)

            df_temp = spark.read.format("jdbc").option("url", con_mysql_source) \
                .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "(" + sql_query + ") sdtable") \
                .option("user", conf_mysql_source['USER']).option("password", conf_mysql_source['PASS']).load()
            if(df_transComplimentDet == None):
                df_transComplimentDet = df_temp
            else:
                df_transComplimentDet = df_transComplimentDet.union(df_temp)

        if(df_transComplimentDet != None):
            df_transComplimentDet.createOrReplaceTempView(df_transComplimentDet_name)
            df_transComplimentDet_count = df_transComplimentDet.count()

        if (df_transComplimentDet != None and df_transComplimentDet_count > 0):
            # CLEANSING DATA RAW DATA MART DENGAN PARAMETER ID_TRANSACTIONS SEBELUM DIISI DENGAN DATA UPDATE TERBARU
            # INSERT DATA RAW DATA MART DENGAN DATA TERBARU
            # JOIN ON SPARK untuk transaction & transaction detail
            spark_query = "SELECT id_comp as raw_mart_compliment_no " \
                          ", id_transaction as raw_mart_compliment_transaction_id " \
                          ", m_user_id_user,m_cabang_id_cabang " \
                          ", transaction_no_nota as raw_mart_compliment_no_nota " \
                          ", transaction_tgl as raw_mart_compliment_datetime " \
                          ", DATE(transaction_tgl) as raw_mart_compliment_date " \
                          ", kasir as raw_mart_compliment_cashier_id " \
                          ", transaction_jumlah_pesanan as raw_mart_compliment_product_qty " \
                          ", auth as raw_mart_compliment_auth_id " \
                          ", transaction_promo_calculated_value as raw_mart_compliment_value " \
                          ", transaction_catatan as raw_mart_compliment_note " \
                          "FROM "+df_transCompliment_name +" JOIN "+df_transComplimentDet_name+" ON " \
                          "id_transaction = Transactions_id_transaction "
            df_join = spark.sql(spark_query).fillna(value=0)

            # CLEANSING DATA RAW DATA MART DENGAN PARAMETER ID_TRANSACTIONS SEBELUM DIISI DENGAN DATA UPDATE TERBARU
            executor    = ThreadPoolExecutor()
            cleanJobs   = []

            for rowTrx in nTrxId:
                strWhereID = ",".join(rowTrx)
                sql_query = "DELETE FROM mart_prd.raw_mart_compliment WHERE raw_mart_compliment_transaction_id in (" + strWhereID + ")"
                cleanJobs.append(executor.submit(executePostgres, sql_query, conf_postgres_target))

            for job in cleanJobs:
                job.result()

            df_join.write.jdbc(url=con_postgres_target, table="mart_prd.raw_mart_compliment", mode=mode,properties=properties_target)
            # hasil join kita simpen sebagai df_rawMartAddon / df_Join



        sts_recovery = 'running etl'
    else:
        d_sdate = str(sStart)[:10]
        d_edate = str(sEnd)[:10]

        # clean data raw mart
        del_rawmart = "DELETE FROM mart_prd.raw_mart_compliment " \
            "WHERE (raw_mart_compliment_date BETWEEN '" + d_sdate + "' AND '" + d_edate + "')  " \
            "AND M_User_id_user = "+iIDStore+" "\
            ""+sWhereOutlet
        executePostgres(del_rawmart, conf_postgres_target)

        sts_recovery = 'cleaning data'


    dateEnd = (time.time() * 1000)
    content = {'sCurrent': str(sStart), 'sEnd': str(sEnd), 'tProcess': str(dateEnd - dateStart),
            'nrow': str(df_transComplimentDet_count), 'sts': sts_recovery}
    return {'status': 'success', 'content': content, 'enum': '1'}
