from datetime import date, datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import json,time, pytz, helper
from Utility.sql import *
from Utility.kafka import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from logger import Logger
from functools import partial
from .Dashboard.Spark.services import RawServiceKafka, RawService, HourlyService, DailyService, MonthlyService, YearlyService
from .Dashboard.Pandas.services import RawService as RawPandas, HourlyService as HourlyPandas, DailyService as DailyPandas, MonthlyService as MonthlyPandas, YearlyService as YearlyPandas
from pyspark.sql.types import StructField, StructType, StringType, FloatType, IntegerType, BooleanType


LOGGER = Logger()

conf_mysql_source = helper.getConfMySQLMayang()
con_mysql_source = helper.generate_mysql_connection_url(conf_mysql_source)

conf_postgres_target = helper.getConfPostgresMart()
con_postgres_target = helper.generate_postgres_connection_url(conf_postgres_target)

def processSequencePerMin(seq=1, sIDStore=None, n_size_insert = 1000, n_size_delete = 1000, core_spec = None, driver_memory_spec = None, executor_memory_spec = None, by = "spark", delayInSecs=300):
    sql_max_ts_current = "SELECT ts_end at time zone 'Asia/Jakarta', done_monthly, done_yearly FROM mart_prd.raw_datamart_sales_dashboard_history order by id desc limit 1"
    row_max_ts_current = fetchrow(sql_max_ts_current,conf_postgres_target)
    sTimeStart = row_max_ts_current[0].astimezone(tz=pytz.timezone('Asia/Jakarta'))
    done_monthly, done_yearly = row_max_ts_current[1], row_max_ts_current[2]
    sql_count_ts_current = "SELECT count(ts_end) FROM mart_prd.raw_datamart_sales_dashboard_history WHERE ts_end='"+convertToAsiaJakarta(sTimeStart)+"'"
    row_count_ts_current = fetchrow(sql_count_ts_current,conf_postgres_target)
    nTimeStart = row_count_ts_current[0]
    sTimeEnd=sTimeStart+timedelta(minutes=(seq*nTimeStart))
    return processParamBegEndTime('pyspark-postgresql-'+str(seq)+'minute-sequence-api-dashboard',\
                                        sTimeStart,\
                                        sTimeEnd,\
                                         {'fn': seq, \
                                            'start_time':str(sTimeStart),\
                                            'end_time':str(sTimeEnd),\
                                            'idStore': sIDStore,\
                                            'n_size_insert':n_size_insert ,\
                                            'n_size_delete':n_size_delete},\
                                        id_user = sIDStore ,\
                                        n_size_insert = n_size_insert,\
                                        n_size_delete = n_size_delete, \
                                        core_spec = core_spec,\
                                        driver_memory_spec = driver_memory_spec,\
                                        executor_memory_spec = executor_memory_spec,\
                                        delayInSecs= delayInSecs,\
                                        already_month=done_monthly, already_year=done_yearly) if by == "spark" \
                                                    else processWithPandas(sTimeStart, sTimeEnd,{'fn': seq, \
                                                                                    'start_time':str(sTimeStart),\
                                                                                    'end_time':str(sTimeEnd),\
                                                                                    'idStore': sIDStore,\
                                                                                    'n_size_insert':n_size_insert ,\
                                                                                    'n_size_delete':n_size_delete},\
                                                                                id_user = sIDStore ,\
                                                                                n_size_insert = n_size_insert,\
                                                                                n_size_delete = n_size_delete,\
                                                                                delayInSecs= delayInSecs,\
                                                                                already_month=done_monthly, already_year=done_yearly)

def processSalesDashboardSequenceKafka(topic_name, max_message, timeout):
    sql_max_ts_current = "SELECT ts_end at time zone 'Asia/Jakarta', done_monthly, done_yearly FROM mart_prd.raw_datamart_sales_dashboard_history order by id desc limit 1"
    row_max_ts_current = fetchrow(sql_max_ts_current, conf_postgres_target)
    done_monthly, done_yearly = row_max_ts_current[1], row_max_ts_current[2]
    sTimeStart = row_max_ts_current[0]
    sTimeEnd = sTimeStart
    return processSalesDashboardKafka('pyspark-postgresql-kafka-api-sales-dashboard', topic_name, max_message, timeout,
        sTimeStart, sTimeEnd, done_monthly, done_yearly, {'method': 'kafka', 'fn': '0'})

def processSalesDashboardKafka(appName, topic_name, max_message, timeout, start, end, already_month, already_year, param, n_size_insert = 1000, n_size_delete = 1000):
    """
        This function is used to run micro-batching ETL
    """

    conf = helper.get_spark_config(appName)

    spark = SparkSession.builder.config(conf=conf).getOrCreate() #init spark
    startRunTime = (time.time() * 1000)

    mode = "append"
    properties_target = {"user": conf_postgres_target['USER'], "password": conf_postgres_target['PASS'],"driver": "org.postgresql.Driver"}

    numrows = 0

    transaction_ids, deleted_transaction_ids, consumer = \
        get_message(topic=topic_name, consumer='etl-sales-dashboard', max_message=max_message, timeout=timeout)

    if len(transaction_ids) > 0:
        # dataframe name
        try:
            # now = datetime.now().astimezone(tz=pytz.timezone('Asia/Jakarta'))
            # sCurrent = sCurrent-timedelta(seconds=5 if sAdd < now else delayInSecs )

            #Init logic services
            rawServices = RawServiceKafka(spark, con_mysql_source, conf_mysql_source, con_postgres_target, conf_postgres_target, mode, properties_target, n_size_insert = n_size_insert, n_size_delete = n_size_delete)
            hourlyService = HourlyService(spark, con_mysql_source, conf_mysql_source, con_postgres_target, conf_postgres_target, mode, properties_target, n_size_insert = n_size_insert, n_size_delete = n_size_delete)
            dailyService = DailyService(spark, con_mysql_source, conf_mysql_source, con_postgres_target, conf_postgres_target, mode, properties_target, n_size_insert = n_size_insert, n_size_delete = n_size_delete)
            monthlyService = MonthlyService(spark, con_mysql_source, conf_mysql_source, con_postgres_target, conf_postgres_target, mode, properties_target, n_size_insert = n_size_insert, n_size_delete = n_size_delete)
            yearlyService = YearlyService(spark, con_mysql_source, conf_mysql_source, con_postgres_target, conf_postgres_target, mode, properties_target, n_size_insert = n_size_insert, n_size_delete = n_size_delete)

            # Extract, will return needed filters for marting & cleaning
            (postfix,
                tobeDeleted,
                martHourly,
                martDaily,
                martMonthly,
                martYearly) = rawServices.proceed( transaction_ids, deleted_transaction_ids, user = None)

            if rawServices.numrow > 0:
                # if row exist
                executor    = ThreadPoolExecutor()
                cleanJobs   = []
                cleanMartJobs   = []

                numrows = rawServices.numrow

                # get previous raw data daily
                previousRawDaily = rawServices.fetchPreviousRawDaily(tobeDeleted)

                # merge previous dan present raw data daily
                dailyTobeDelete = []
                for i in martDaily[1]:
                    for j in i:
                        dailyTobeDelete.append(j)

                for k in previousRawDaily:
                    for l in k:
                        dailyTobeDelete.append(l)

                dailyDelete = list(dict.fromkeys(dailyTobeDelete))
                dailyDeleteBatch = [dailyDelete[i:i + n_size_delete] for i in range(0, len(dailyDelete), n_size_delete)]


                # get previous raw data houlry
                previousRawHourly = rawServices.fetchPreviousRawHourly(tobeDeleted)

                # merge previous dan present raw data hourly
                hourlyTobeDelete = []
                for i in martHourly[1]:
                    for j in i:
                        hourlyTobeDelete.append(j)

                for k in previousRawHourly:
                    for l in k:
                        hourlyTobeDelete.append(l)

                hourlyDelete = list(dict.fromkeys(hourlyTobeDelete))
                hourlyDeleteBatch = [hourlyDelete[i:i + n_size_delete] for i in range(0, len(hourlyDelete), n_size_delete)]


                # Cleaning Raw
                for chunk in tobeDeleted: # remove deleted transactions
                    cleanJobs.append(executor.submit(rawServices.delete,chunk))

                # Cleaning Marts
                for chunk in hourlyDeleteBatch:
                    cleanMartJobs.append(executor.submit(hourlyService.delete, chunk))

                for chunk in dailyDeleteBatch:
                    cleanMartJobs.append(executor.submit(dailyService.delete, chunk))

                for chunk in martMonthly[1]:
                    cleanMartJobs.append(executor.submit(monthlyService.delete, chunk))

                for chunk in martYearly[1]:
                    cleanMartJobs.append(executor.submit(yearlyService.delete, chunk))

                # transform source to raw while waiting cleaning raw
                df_raw = rawServices.transform(postfix)

                # ensure raw is cleaned thoroughly
                for job in cleanJobs:
                    job.result()

                # write raw to database after raw-cleaning is done, also we can write to raw while mart is being cleaned
                rawServices.write(df_raw)

                # ensure datamart is cleaned
                for job in cleanMartJobs:
                    job.result()

                # extract, transform , load
                hourlyService.proceed(martHourly[0], postfix=postfix)
                dailyService.proceed(martDaily[0], postfix=postfix)
                monthlyService.proceed(martMonthly[0], postfix=postfix)
                yearlyService.proceed(martYearly[0], postfix=postfix)

            # end of if numrow > 0

            start = rawServices.minUpdate
            end  = rawServices.maxUpdate

            if not already_month:
                # generate previous month here
                prev_month = start.month - 1
                at_year = start.year

                if prev_month == 0 :
                    prev_month  = 12
                    at_year = at_year - 1

                # compute previous month
                monthlyService.deleteTargeted(at_year,prev_month) #clean
                df = monthlyService.transformTargeted(at_year,prev_month) #transform
                monthlyService.write(df, name = f"write_monthly_targeted_{prev_month}_{at_year}")
                already_month = True

            if not already_year:
                # generate previous year here
                prev_year = start.year - 1
                yearlyService.deleteTargeted(prev_year) #clean
                df = yearlyService.transformTargeted(prev_year) #transform
                yearlyService.write(df, name = f"write_yearly_targeted_{prev_month}_{prev_year}")
                already_year = True

            if already_year and end.year > start.year: # if we already had compute year, and the next iteration is 'new year' then change it to false
                already_year = False

            if already_month and ((end.month > start.month and end.year == start.year) or (end.year > start.year)): # if we already had compute month, and the next iteration is 'next month' then change it to false
                already_month = False

            # clear all temp view
            rawServices.dropTempView()
            dailyService.dropTempView()
            hourlyService.dropTempView()
            monthlyService.dropTempView()
            yearlyService.dropTempView()

            hourlyService.timelog.update(hourlyService.timelog)
            rawServices.timelog.update(dailyService.timelog)
            rawServices.timelog.update(monthlyService.timelog)
            rawServices.timelog.update(yearlyService.timelog)
        except:
            consumer.close()
            consumer = None
            raise


    # update time logs

        # param['delay'] = 5 if sAdd < now else delayInSecs

    endRunTime = (time.time() * 1000)

    dataActLog = [(str(convertToAsiaJakarta(datetime.now())) , convertToAsiaJakarta(start), convertToAsiaJakarta(end), numrows, endRunTime - startRunTime, json.dumps(param), already_month, already_year)]
    schemaLog = StructType([
        StructField('actdate', StringType(), True),
        StructField('ts_current', StringType(), True),
        StructField('ts_end', StringType(), True),
        StructField('num_row', IntegerType(), True),
        StructField('time_process', FloatType(), True),
        StructField('params', StringType(), True),
        StructField('done_monthly', BooleanType(), True),
        StructField('done_yearly', BooleanType(), True),
    ])
    rdd = spark.sparkContext.parallelize(dataActLog)
    dfHistory = spark.createDataFrame(rdd, schemaLog)
    dfHistory.write.jdbc(url=f"{con_postgres_target}&stringtype=unspecified", table="mart_prd.raw_datamart_sales_dashboard_history", mode=mode,properties=properties_target)

    try:
        consumer.commit(asynchronous=False)
    except:
        pass
    consumer.close()

    return {'sCurrent': str(start), 'sEnd': str(end), 'tProcess': str(endRunTime - startRunTime),
            'nrow': str(numrows),'param':param}


def processParamBegEndTime(appName,sCurrent,sAdd,param, id_user = None, n_size_insert = 1000 ,n_size_delete = 1000,
        core_spec = None, driver_memory_spec = None, executor_memory_spec = None,
        already_month = False, already_year = False , delayInSecs = 300):
    """
        This function is used to run micro-batching ETL
    """
    conf = helper.get_spark_config(appName) if  core_spec == None or driver_memory_spec == None or executor_memory_spec == None \
                                            else helper.get_spark_config(appName,\
                                                                        driver_memory=driver_memory_spec,\
                                                                        executor_memory=executor_memory_spec, \
                                                                        executor_core=core_spec )

    spark = SparkSession.builder.config(conf=conf).getOrCreate() #init spark
    startRunTime = (time.time() * 1000)

    mode = "append"
    properties_target = {"user": conf_postgres_target['USER'], "password": conf_postgres_target['PASS'],"driver": "org.postgresql.Driver"}

    now = datetime.now().astimezone(tz=pytz.timezone('Asia/Jakarta'))
    sCurrent    = sCurrent-timedelta(seconds=5 if sAdd < now else delayInSecs )

    #Init logic services
    rawServices = RawService(spark, con_mysql_source, conf_mysql_source, con_postgres_target, conf_postgres_target, mode, properties_target, n_size_insert = n_size_insert, n_size_delete = n_size_delete)
    hourlyService = HourlyService(spark, con_mysql_source, conf_mysql_source, con_postgres_target, conf_postgres_target, mode, properties_target, n_size_insert = n_size_insert, n_size_delete = n_size_delete)
    dailyService = DailyService(spark, con_mysql_source, conf_mysql_source, con_postgres_target, conf_postgres_target, mode, properties_target, n_size_insert = n_size_insert, n_size_delete = n_size_delete)
    monthlyService = MonthlyService(spark, con_mysql_source, conf_mysql_source, con_postgres_target, conf_postgres_target, mode, properties_target, n_size_insert = n_size_insert, n_size_delete = n_size_delete)
    yearlyService = YearlyService(spark, con_mysql_source, conf_mysql_source, con_postgres_target, conf_postgres_target, mode, properties_target, n_size_insert = n_size_insert, n_size_delete = n_size_delete)

    # Extract, will return needed filters for marting & cleaning
    (postfix,
        tobeDeleted,
        martHourly,
        martDaily,
        martMonthly,
        martYearly) = rawServices.proceed( sCurrent, sAdd, user = id_user)

    if rawServices.numrow > 0:
        # if row exist
        executor    = ThreadPoolExecutor()
        cleanJobs   = []
        cleanMartJobs   = []


        # get previous raw data daily
        previousRawDaily = rawServices.fetchPreviousRawDaily(tobeDeleted)

        # merge previous dan present raw data daily
        dailyTobeDelete = []
        for i in martDaily[1]:
            for j in i:
                dailyTobeDelete.append(j)

        for k in previousRawDaily:
            for l in k:
                dailyTobeDelete.append(l)

        dailyDelete = list(dict.fromkeys(dailyTobeDelete))
        dailyDeleteBatch = [dailyDelete[i:i + n_size_delete] for i in range(0, len(dailyDelete), n_size_delete)]


        # get previous raw data houlry
        previousRawHourly = rawServices.fetchPreviousRawHourly(tobeDeleted)

        # merge previous dan present raw data hourly
        hourlyTobeDelete = []
        for i in martHourly[1]:
            for j in i:
                hourlyTobeDelete.append(j)

        for k in previousRawHourly:
            for l in k:
                hourlyTobeDelete.append(l)

        hourlyDelete = list(dict.fromkeys(hourlyTobeDelete))
        hourlyDeleteBatch = [hourlyDelete[i:i + n_size_delete] for i in range(0, len(hourlyDelete), n_size_delete)]


        # Cleaning Raw
        for chunk in tobeDeleted: # remove deleted transactions
            cleanJobs.append(executor.submit(rawServices.delete,chunk))

        # Cleaning Marts
        for chunk in hourlyDeleteBatch:
            cleanMartJobs.append(executor.submit(hourlyService.delete, chunk))

        for chunk in dailyDeleteBatch:
            cleanMartJobs.append(executor.submit(dailyService.delete, chunk))

        for chunk in martMonthly[1]:
            cleanMartJobs.append(executor.submit(monthlyService.delete, chunk))

        for chunk in martYearly[1]:
            cleanMartJobs.append(executor.submit(yearlyService.delete, chunk))

        # transform source to raw while waiting cleaning raw
        df_raw = rawServices.transform(postfix)

        # ensure raw is cleaned thoroughly
        for job in cleanJobs:
            job.result()

        # write raw to database after raw-cleaning is done, also we can write to raw while mart is being cleaned
        rawServices.write(df_raw)

        # ensure datamart is cleaned
        for job in cleanMartJobs:
            job.result()

        # extract, transform , load
        hourlyService.proceed(martHourly[0], postfix=postfix)
        dailyService.proceed(martDaily[0], postfix=postfix)
        monthlyService.proceed(martMonthly[0], postfix=postfix)
        yearlyService.proceed(martYearly[0], postfix=postfix)

    # end of if numrow > 0

    start = rawServices.minUpdate
    end  = rawServices.maxUpdate

    if not already_month:
        # generate previous month here
        prev_month = start.month - 1
        at_year = start.year

        if prev_month == 0 :
            prev_month  = 12
            at_year = at_year - 1

        # compute previous month
        monthlyService.deleteTargeted(at_year,prev_month) #clean
        df = monthlyService.transformTargeted(at_year,prev_month) #transform
        monthlyService.write(df, name = f"write_monthly_targeted_{prev_month}_{at_year}")
        already_month = True

    if not already_year:
        # generate previous year here
        prev_year = start.year - 1
        yearlyService.deleteTargeted(prev_year) #clean
        df = yearlyService.transformTargeted(prev_year) #transform
        yearlyService.write(df, name = f"write_yearly_targeted_{prev_month}_{prev_year}")
        already_year = True

    if already_year and end.year > start.year: # if we already had compute year, and the next iteration is 'new year' then change it to false
        already_year = False

    if already_month and ((end.month > start.month and end.year == start.year) or (end.year > start.year)): # if we already had compute month, and the next iteration is 'next month' then change it to false
        already_month = False

    # clear all temp view
    rawServices.dropTempView()
    dailyService.dropTempView()
    hourlyService.dropTempView()
    monthlyService.dropTempView()
    yearlyService.dropTempView()

    # update time logs
    hourlyService.timelog.update(hourlyService.timelog)
    rawServices.timelog.update(dailyService.timelog)
    rawServices.timelog.update(monthlyService.timelog)
    rawServices.timelog.update(yearlyService.timelog)

    param['delay'] = 5 if sAdd < now else delayInSecs

    endRunTime = (time.time() * 1000)

    dataActLog = [(str(convertToAsiaJakarta(datetime.now())) , convertToAsiaJakarta(start), convertToAsiaJakarta(end), rawServices.numrow, endRunTime - startRunTime, json.dumps(param), already_month, already_year)]
    schemaLog = StructType([
        StructField('actdate', StringType(), True),
        StructField('ts_current', StringType(), True),
        StructField('ts_end', StringType(), True),
        StructField('num_row', IntegerType(), True),
        StructField('time_process', FloatType(), True),
        StructField('params', StringType(), True),
        StructField('done_monthly', BooleanType(), True),
        StructField('done_yearly', BooleanType(), True),
    ])
    rdd = spark.sparkContext.parallelize(dataActLog)
    dfHistory = spark.createDataFrame(rdd, schemaLog)
    dfHistory.write.jdbc(url=f"{con_postgres_target}&stringtype=unspecified", table="mart_prd.raw_datamart_sales_dashboard_history", mode=mode,properties=properties_target)

    return {'sCurrent': str(rawServices.minUpdate), 'sEnd': str( rawServices.maxUpdate), 'tProcess': str(endRunTime - startRunTime),
            'nrow': str(rawServices.numrow),'param':param, 'log':rawServices.timelog}

def processWithPandas(sCurrent,sAdd,param, id_user = None, n_size_insert = 1000 ,n_size_delete = 1000,
        already_month = False, already_year = False, delayInSecs = None):
#     # frequent problem found:
#     # - df is empty ==> if empty then skip
#     # - typo, sensitive case, differs with spark x postgres which case unsensitive
#     # - pandas datatype, bigint casted to float, etc ==> map value when converting back to pandas
#     # - waitress eror but still loading ==> try except

    startRunTime = time.time()
    pandasService = RawPandas(conf_mysql_source, conf_postgres_target, n_size_insert, n_size_delete )

    now = datetime.now().astimezone(tz=pytz.timezone('Asia/Jakarta'))
    _delay = 5 if sAdd < now else delayInSecs
    sCurrent    = sCurrent-timedelta(seconds=_delay)

    (
        df_trx,
        df_product,
        df_mdr,
        df_addon,
        df_commission,
        df_child,
        martHourly,
        martDaily,
        martMonthly,
        martYearly,
        tobeDeletedHourly,
        tobeDeletedDaily,
        tobeDeletedMonthly,
        tobeDeletedYearly,
        tobeDeletedRaw
    )  = pandasService.proceed(sCurrent, sAdd, user= id_user)
    hourlyService = HourlyPandas(conf_mysql_source, conf_postgres_target, n_size_insert, n_size_delete)
    dailyService = DailyPandas(conf_mysql_source, conf_postgres_target, n_size_insert, n_size_delete)
    monthlyService = MonthlyPandas(conf_mysql_source, conf_postgres_target, n_size_insert, n_size_delete)
    yearlyService = YearlyPandas(conf_mysql_source, conf_postgres_target, n_size_insert, n_size_delete)

    if pandasService.numrow > 0:
        # if row exist

        executor    = ThreadPoolExecutor()
        cleanJobs   = []
        cleanJobsMarts   = []


        # get previous raw data daily
        previousRawDaily = pandasService.fetchPreviousRawDaily(tobeDeletedRaw)

        # merge previous dan present raw data daily
        dailyTobeDeletePandas = []
        for i in tobeDeletedDaily:
            for j in i:
                dailyTobeDeletePandas.append("".join(j.split()))

        for k in previousRawDaily:
            for l in k:
                dailyTobeDeletePandas.append("".join(l.split()))

        dailyPandasDelete = list(dict.fromkeys(dailyTobeDeletePandas))
        dailyPandasDeleteBatch = [dailyPandasDelete[i:i + n_size_delete] for i in range(0, len(dailyPandasDelete), n_size_delete)]


        # get previous raw data hourly
        previousRawHourly = pandasService.fetchPreviousRawHourly(tobeDeletedRaw)

        # merge previous dan present raw data hourly
        hourlyTobeDeletePandas = []
        for i in tobeDeletedHourly:
            for j in i:
                hourlyTobeDeletePandas.append("".join(j.split()))

        for k in previousRawHourly:
            for l in k:
                hourlyTobeDeletePandas.append("".join(l.split()))

        hourlyPandasDelete = list(dict.fromkeys(hourlyTobeDeletePandas))
        hourlyPandasDeleteBatch = [hourlyPandasDelete[i:i + n_size_delete] for i in range(0, len(hourlyPandasDelete), n_size_delete)]


        # Cleaning Raw
        for chunk in tobeDeletedRaw:
            cleanJobs.append(executor.submit(pandasService.delete, chunk))

        for chunk in hourlyPandasDeleteBatch:
        # Cleaning Marts
            cleanJobsMarts.append(executor.submit(hourlyService.delete, chunk))

        for chunk in dailyPandasDeleteBatch:
        # Cleaning Marts
            cleanJobsMarts.append(executor.submit(dailyService.delete, chunk))

        for chunk in tobeDeletedMonthly:
            cleanJobsMarts.append(executor.submit(monthlyService.delete, chunk))

        for chunk in tobeDeletedYearly:
            cleanJobsMarts.append(executor.submit(yearlyService.delete, chunk))

        # transform source to raw
        df_raw = pandasService.transform(df_trx, df_product, df_mdr, df_addon, df_commission, df_child)
        del df_trx, df_product, df_mdr, df_addon, df_commission, df_child


        for job in cleanJobs: #wait for each delete job is done
            job.result()

        pandasService.write(df_raw)
        del df_raw

        for job in cleanJobsMarts:
            job.result()

        hourlyService.proceed(martHourly)
        dailyService.proceed(martDaily)
        monthlyService.proceed(martMonthly)
        yearlyService.proceed(martYearly)
    # end of raw.numRow > 0

    sCurrent = pandasService.minUpdate
    sAdd = pandasService.maxUpdate

    if not already_month:
        # generate previous month here
        prev_month = sCurrent.month - 1
        at_year = sCurrent.year

        if prev_month == 0 :
            prev_month  = 12
            at_year = at_year - 1
        monthlyService.deleteTargeted(at_year, prev_month)
        df = monthlyService.transformTargeted(at_year, prev_month)
        monthlyService.write(df, name = f"write_targeted_monthly_{prev_month}{at_year}")
        already_month = True

    if not already_year:
        # generate previous year here
        prev_year = sCurrent.year - 1
        yearlyService.deleteTargeted(prev_year)
        df = yearlyService.transformTargeted(prev_year)
        yearlyService.write(df, name = f"write_targeted_monthly_{prev_year}")
        already_year = True

    pandasService.timelog.update(hourlyService.timelog)
    pandasService.timelog.update(dailyService.timelog)
    pandasService.timelog.update(monthlyService.timelog)
    pandasService.timelog.update(yearlyService.timelog)

    if already_year and sAdd.year > sCurrent.year: # if we already had compute year, and the next iteration is 'new year' then change it to false
        already_year = False

    if already_month and ((sAdd.month > sCurrent.month and sAdd.year == sCurrent.year) or (sAdd.year > sCurrent.year)): # if we already had compute month, and the next iteration is 'next month' then change it to false
        already_month = False

    endRunTime = time.time()
    param['via'] = 'pandas'
    param['delay'] = _delay

    createETLLog('mart_prd.raw_datamart_sales_dashboard_history',conf_postgres_target,
                convertToAsiaJakarta(pandasService.minUpdate),
                convertToAsiaJakarta(pandasService.maxUpdate),
                pandasService.numrow,
                endRunTime - startRunTime,
                json.dumps(param),
                done_monthly= 1 if already_month else 0,
                done_yearly = 1 if already_year else 0 )


    return {'sCurrent': str(pandasService.minUpdate),'nrow': pandasService.numrow, 'sEnd': str( pandasService.maxUpdate), 'tProcess': str(endRunTime - startRunTime), 'timelog':pandasService.timelog}

def processParam(appName,\
                       sEmail,\
                       sStart,\
                       sEnd,\
                       idCabang = None,\
                       n_size_insert = 1000,\
                       n_size_delete = 1000, core_spec = None, driver_memory_spec = None, executor_memory_spec = None):
#     """
#         This function is used to run recovery
#     """
    conf = helper.get_spark_config(appName) if  core_spec == None or driver_memory_spec == None or executor_memory_spec == None \
                                            else helper.get_spark_config(appName,\
                                                                        driver_memory=driver_memory_spec,\
                                                                        executor_memory=executor_memory_spec, \
                                                                        executor_core=core_spec )

    spark = SparkSession.builder.config(conf=conf).getOrCreate() #init spark
    startRunTime = time.time()

    mode = "append"
    properties_target = {"user": conf_postgres_target['USER'], "password": conf_postgres_target['PASS'],"driver": "org.postgresql.Driver"}

    sql_get_idStore = f"""SELECT CASE WHEN user_has_parent = 0 THEN id_user ELSE user_has_parent END as id
                        FROM master_klopos.M_User
                        WHERE user_email = '{sEmail}' """
    row_idStore = fetchrow(sql_get_idStore,conf_postgres_target)
    if row_idStore is None:
        return (False, "Email {} not found!".format(sEmail))

    id_user = str(row_idStore[0])


    #Init logic services
    rawServices = RawService(spark, con_mysql_source, conf_mysql_source, con_postgres_target, conf_postgres_target, mode, properties_target, n_size_insert = n_size_insert, n_size_delete = n_size_delete)
    hourlyService = HourlyService(spark, con_mysql_source, conf_mysql_source, con_postgres_target, conf_postgres_target, mode, properties_target, n_size_insert = n_size_insert, n_size_delete = n_size_delete)
    dailyService = DailyService(spark, con_mysql_source, conf_mysql_source, con_postgres_target, conf_postgres_target, mode, properties_target, n_size_insert = n_size_insert, n_size_delete = n_size_delete)
    monthlyService = MonthlyService(spark, con_mysql_source, conf_mysql_source, con_postgres_target, conf_postgres_target, mode, properties_target, n_size_insert = n_size_insert, n_size_delete = n_size_delete)
    yearlyService = YearlyService(spark, con_mysql_source, conf_mysql_source, con_postgres_target, conf_postgres_target, mode, properties_target, n_size_insert = n_size_insert, n_size_delete = n_size_delete)
    executor    = ThreadPoolExecutor()

    # Extract, will return needed filters for marting & cleaning
    (postfix,
        tobeDeleted,
        martHourly,
        martDaily,
        martMonthly,
        martYearly) = rawServices.proceed( sStart, sEnd, user = id_user, action="RECOVERY", cabang = idCabang)

#     subExtraService = SubextraMarting(spark, con_mysql_source, conf_mysql_source, con_postgres_target, conf_postgres_target, mode, properties_target, n_size_insert = n_size_insert, n_size_delete = n_size_delete)
#     executor    = ThreadPoolExecutor()

    recoveryAt = f'and m_user_id_user = {id_user} {f"and m_cabang_id_cabang = {idCabang}" if idCabang != None else ""}'

    if rawServices.numrow > 0:
        # if row exist
        cleanJobs   = []
        cleanMartJobs   = []

        # get previous raw data daily
        previousRawDaily = rawServices.fetchPreviousRawDaily(tobeDeleted)

        # merge previous dan present raw data daily
        dailyTobeDelete = []
        for i in martDaily[1]:
            for j in i:
                dailyTobeDelete.append(j)

        for k in previousRawDaily:
            for l in k:
                dailyTobeDelete.append(l)

        dailyDelete = list(dict.fromkeys(dailyTobeDelete))
        dailyDeleteBatch = [dailyDelete[i:i + n_size_delete] for i in range(0, len(dailyDelete), n_size_delete)]


        # get previous raw data houlry
        previousRawHourly = rawServices.fetchPreviousRawHourly(tobeDeleted)

        # merge previous dan present raw data hourly
        hourlyTobeDelete = []
        for i in martHourly[1]:
            for j in i:
                hourlyTobeDelete.append(j)

        for k in previousRawHourly:
            for l in k:
                hourlyTobeDelete.append(l)

        hourlyDelete = list(dict.fromkeys(hourlyTobeDelete))
        hourlyDeleteBatch = [hourlyDelete[i:i + n_size_delete] for i in range(0, len(hourlyDelete), n_size_delete)]

        # Cleaning Raw
        for chunk in tobeDeleted: # remove deleted transactions
            cleanJobs.append(executor.submit(rawServices.delete,chunk))


        # Cleaning Marts
        for chunk in hourlyDeleteBatch:
            cleanMartJobs.append(executor.submit(hourlyService.delete, chunk))

        for chunk in dailyDeleteBatch:
            cleanMartJobs.append(executor.submit(dailyService.delete, chunk))

        for chunk in martMonthly[1]:
            cleanMartJobs.append(executor.submit(monthlyService.delete, chunk))

        for chunk in martYearly[1]:
            cleanMartJobs.append(executor.submit(yearlyService.delete, chunk))

        # transform source to raw while waiting cleaning raw
        df_raw = rawServices.transform(postfix)

        # ensure raw is cleaned thoroughly
        for job in cleanJobs:
            job.result()

        # write raw to database after raw-cleaning is done, also we can write to raw while mart is being cleaned
        rawServices.write(df_raw)

        # ensure datamart is cleaned
        for job in cleanMartJobs:
            job.result()

        # extract, transform , load
        hourlyService.proceed(martHourly[0], postfix=postfix)
        dailyService.proceed(martDaily[0], postfix=postfix)
        monthlyService.proceed(martMonthly[0], postfix=postfix)
        yearlyService.proceed(martYearly[0], postfix=postfix)
    else:
        # if no row exists, do cleaning and recalculate monthly yearly
        cleanJobs = []

        sStartStr = sStart.strftime('%Y-%m-%d')
        sEndStr = sEnd.strftime('%Y-%m-%d')

        cleanRaw = f"DELETE FROM mart_prd.raw_mart_sales_dashboard WHERE raw_mart_sales_dashboard_date BETWEEN '{sStartStr}' AND '{sEndStr}' {recoveryAt}"
        cleanJobs.append(executor.submit(executePostgres,cleanRaw, conf_postgres_target))

        cleanHourly = f"DELETE FROM mart_prd.mart_sales_dashboard_hourly WHERE mart_sales_dashboard_hourly_date BETWEEN '{sStartStr}' AND '{sEndStr}' {recoveryAt}"
        cleanJobs.append(executor.submit(executePostgres,cleanHourly, conf_postgres_target))

        cleanDaily = f"DELETE FROM mart_prd.mart_sales_dashboard_daily WHERE mart_sales_dashboard_daily_date BETWEEN '{sStartStr}' AND '{sEndStr}' {recoveryAt}"
        cleanJobs.append(executor.submit(executePostgres,cleanDaily, conf_postgres_target))

        months = []
        years = []
        d = sStart
        while d <= sEnd:
            months.append((d.month, d.year))
            years.append(d.year)
            if d.month == 12:
                d = datetime(d.year + 1, 1, 1).astimezone(tz=pytz.timezone('Asia/Jakarta'))
            else:
                d = datetime(d.year, d.month + 1, 1).astimezone(tz=pytz.timezone('Asia/Jakarta'))
        years = list(set(years))

        for (month,year) in months:
            cleanJobs.append(executor.submit(monthlyService.deleteTargeted,year, month, recoveryAt))

        for year in years:
            cleanJobs.append(executor.submit(yearlyService.deleteTargeted, year,recoveryAt ))

        # wait for all cleaning job to be done
        for cleanJob in cleanJobs:
            cleanJob.result()

        for (month,year) in months:
            df = monthlyService.transformTargeted(year, month, recoveryAt = recoveryAt , postfix = f"{id_user}{idCabang}_{month}{year}") #transform
            monthlyService.write(df, name = f"write_monthly_targeted_{month}_{year}")

        for year in years:
            df = yearlyService.transformTargeted(year, recoveryAt = recoveryAt , postfix = f"{id_user}{idCabang}_{year}") #transform
            yearlyService.write(df, name = f"write_yearly_targeted_{year}")


    # clear all temp view
    rawServices.dropTempView()
    hourlyService.dropTempView()
    dailyService.dropTempView()
    monthlyService.dropTempView()
    yearlyService.dropTempView()

    # update time logs
    rawServices.timelog.update(hourlyService.timelog)
    rawServices.timelog.update(dailyService.timelog)
    rawServices.timelog.update(monthlyService.timelog)
    rawServices.timelog.update(yearlyService.timelog)

    endRunTime = time.time()
    return {'sCurrent': str(sStart), 'sEnd': str(sEnd), 'tProcess': str(endRunTime - startRunTime),
            'nrow': str(rawServices.numrow), 'log':rawServices.timelog}


def processParamWithPandas(sEmail,\
                            sStart,\
                            sEnd,\
                            idCabang = None,\
                            n_size_insert = 1000,\
                            n_size_delete = 1000):

    startRunTime = time.time()
    sql_get_idStore = f"""SELECT CASE WHEN user_has_parent = 0 THEN id_user ELSE user_has_parent END as id
                        FROM master_klopos.M_User
                        WHERE user_email = '{sEmail}' """
    row_idStore = fetchrow(sql_get_idStore,conf_postgres_target)
    if row_idStore is None:
        return (False, "Email {} not found!".format(sEmail))

    id_user =  str(row_idStore[0])
    pandasService = RawPandas(conf_mysql_source, conf_postgres_target, n_size_insert, n_size_delete )
    recoveryAt = f'and m_user_id_user = {id_user} {f"and m_cabang_id_cabang = {idCabang}" if idCabang != None else ""}'

    (
        df_trx,
        df_product,
        df_mdr,
        df_addon,
        df_commission,
        df_child,
        martHourly,
        martDaily,
        martMonthly,
        martYearly,
        tobeDeletedHourly,
        tobeDeletedDaily,
        tobeDeletedMonthly,
        tobeDeletedYearly,
        tobeDeletedRaw
    )  = pandasService.proceed(sStart, sEnd, action= "RECOVERY" , user= id_user,cabang = idCabang)


    hourlyService = HourlyPandas(conf_mysql_source, conf_postgres_target, n_size_insert, n_size_delete)
    dailyService = DailyPandas(conf_mysql_source, conf_postgres_target, n_size_insert, n_size_delete)
    monthlyService = MonthlyPandas(conf_mysql_source, conf_postgres_target, n_size_insert, n_size_delete)
    yearlyService = YearlyPandas(conf_mysql_source, conf_postgres_target, n_size_insert, n_size_delete)
    executor    = ThreadPoolExecutor()

    if pandasService.numrow > 0:
        # if row exist
        executor    = ThreadPoolExecutor()
        cleanJobs   = []
        cleanJobsMarts   = []


        # get previous raw data daily
        previousRawDaily = pandasService.fetchPreviousRawDaily(tobeDeletedRaw)

        # merge previous dan present raw data daily
        dailyTobeDeletePandas = []
        for i in tobeDeletedDaily:
            for j in i:
                dailyTobeDeletePandas.append("".join(j.split()))

        for k in previousRawDaily:
            for l in k:
                dailyTobeDeletePandas.append("".join(l.split()))

        dailyPandasDelete = list(dict.fromkeys(dailyTobeDeletePandas))
        dailyPandasDeleteBatch = [dailyPandasDelete[i:i + n_size_delete] for i in range(0, len(dailyPandasDelete), n_size_delete)]


        # get previous raw data hourly
        previousRawHourly = pandasService.fetchPreviousRawHourly(tobeDeletedRaw)

        # merge previous dan present raw data hourly
        hourlyTobeDeletePandas = []
        for i in tobeDeletedHourly:
            for j in i:
                hourlyTobeDeletePandas.append("".join(j.split()))

        for k in previousRawHourly:
            for l in k:
                hourlyTobeDeletePandas.append("".join(l.split()))

        hourlyPandasDelete = list(dict.fromkeys(hourlyTobeDeletePandas))
        hourlyPandasDeleteBatch = [hourlyPandasDelete[i:i + n_size_delete] for i in range(0, len(hourlyPandasDelete), n_size_delete)]


        # Cleaning Raw
        for chunk in tobeDeletedRaw:
            cleanJobs.append(executor.submit(pandasService.delete, chunk))

        # Cleaning Marts
        for chunk in hourlyPandasDeleteBatch:
            cleanJobsMarts.append(executor.submit(hourlyService.delete, chunk))

        for chunk in dailyPandasDeleteBatch:
            cleanJobsMarts.append(executor.submit(dailyService.delete, chunk))

        for chunk in tobeDeletedMonthly:
            cleanJobsMarts.append(executor.submit(monthlyService.delete, chunk))

        for chunk in tobeDeletedYearly:
            cleanJobsMarts.append(executor.submit(yearlyService.delete, chunk))

        # transform source to raw
        df_raw = pandasService.transform(df_trx, df_product, df_mdr, df_addon, df_commission, df_child)
        del df_trx, df_product, df_mdr, df_addon, df_commission, df_child


        for job in cleanJobs: #wait for each delete job is done
            job.result()

        pandasService.write(df_raw)
        del df_raw

        for job in cleanJobsMarts:
            job.result()

        hourlyService.proceed(martHourly)
        dailyService.proceed(martDaily)
        monthlyService.proceed(martMonthly)
        yearlyService.proceed(martYearly)
        # end of raw.numRow > 0
    else:
        # if no row exists, do cleaning and recalculate monthly yearly
        cleanJobs = []

        sStartStr = sStart.strftime('%Y-%m-%d')
        sEndStr = sEnd.strftime('%Y-%m-%d')

        cleanRaw = f"DELETE FROM mart_prd.raw_mart_sales_dashboard WHERE raw_mart_sales_dashboard_date BETWEEN '{sStartStr}' AND '{sEndStr}' {recoveryAt}"
        cleanJobs.append(executor.submit(executePostgres,cleanRaw, conf_postgres_target))

        cleanHourly = f"DELETE FROM mart_prd.mart_sales_dashboard_hourly WHERE mart_sales_dashboard_hourly_date BETWEEN '{sStartStr}' AND '{sEndStr}' {recoveryAt}"
        cleanJobs.append(executor.submit(executePostgres,cleanHourly, conf_postgres_target))

        cleanDaily = f"DELETE FROM mart_prd.mart_sales_dashboard_daily WHERE mart_sales_dashboard_daily_date BETWEEN '{sStartStr}' AND '{sEndStr}' {recoveryAt}"
        cleanJobs.append(executor.submit(executePostgres,cleanDaily, conf_postgres_target))

        months = []
        years = []
        d = sStart
        while d <= sEnd:
            months.append((d.month, d.year))
            years.append(d.year)
            if d.month == 12:
                d = datetime(d.year + 1, 1, 1).astimezone(tz=pytz.timezone('Asia/Jakarta'))
            else:
                d = datetime(d.year, d.month + 1, 1).astimezone(tz=pytz.timezone('Asia/Jakarta'))
        years = list(set(years))

        for month,year in months:
            cleanJobs.append(executor.submit(monthlyService.deleteTargeted, year, month, recoveryAt))

        for year in years:
            cleanJobs.append(executor.submit(yearlyService.deleteTargeted, year, recoveryAt))

        # wait for all cleaning job to be done
        for cleanJob in cleanJobs:
            cleanJob.result()

        for month,year in months:
            df = monthlyService.transformTargeted(year, month, recoveryAt=recoveryAt)
            monthlyService.write(df, name = f"write_targeted_monthly_{month}{year}")

        for year in years:
            df = yearlyService.transformTargeted(year,  recoveryAt=recoveryAt)
            yearlyService.write(df, name = f"write_targeted_yearly_{year}")

    pandasService.timelog.update(dailyService.timelog)
    pandasService.timelog.update(monthlyService.timelog)
    pandasService.timelog.update(yearlyService.timelog)

    endRunTime = time.time()

    return {'sCurrent': str(sStart), 'sEnd': str(sEnd), 'tProcess': str(endRunTime - startRunTime),
        'nrow': str(pandasService.numrow), 'log':pandasService.timelog}
