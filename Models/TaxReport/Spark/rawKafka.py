from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from Utility.sql import *
import time, pytz
from datetime import datetime


class ETL:
    def __init__(self, spark:SparkSession,  url_con_source , conf_con_source, url_con_target, conf_con_target, mode, properties_target, n_size_insert = 1000, n_size_delete = 1000):
        self.credentials = {
            'target' : {
                'url' : url_con_target,
                'conf' : conf_con_target,
                'mode' : mode,
                'properties' : properties_target
            },
            'source': {
                'url' : url_con_source,
                'conf' : conf_con_source,
            }
        }
        self.spark = spark
        self.cachedTable = []
        self.n_size_insert = n_size_insert
        self.n_size_delete = n_size_delete
        self.timelog = {}

    def pushCachedTable(self, df:DataFrame, name):
        df.createOrReplaceTempView(name)
        self.spark.catalog.cacheTable(name)
        self.cachedTable.append(name)

    def dropTempView(self):
        for table in self.cachedTable:
            self.spark.catalog.dropTempView(table)

class RawKafka(ETL):
    def proceed(self, transaction_ids:list, deleted_transaction_ids:list, action:str = 'ETL', user:str = None, cabang: int = None):
        start = ''
        end = ''

        recoveryAt = None
        postfix = ''
        if user != None:
            recoveryAt = f' AND t.M_User_id_user IN ({user})' if  type(user) is str and len(user.split(',')) > 1 else f' AND t.M_User_id_user = {user}'
            postfix = f'{str(user).replace(" ","").replace(",","_")}'

            if cabang != None and( type(cabang) == int or (type(cabang) == str and len(cabang.split(',')) == 1)):
                recoveryAt = f' {recoveryAt} AND t.M_Cabang_id_cabang = {cabang}'
                postfix = f'{postfix}_{cabang}'


        rdds, df_name = self._extract(transaction_ids, postfix = postfix)

        tobeDeleted     = []

        martDaily           = []
        martDeletedDaily    = []
        martMonthly         = []
        martDeletedMonthly  = []
        martYearly          = []
        martDeletedYearly   = []


        _timestamp = time.time()
        self.numrow = len(rdds)
        self.timelog['get_numrow'] = time.time() - _timestamp

        if self.numrow > 0:
            if action == "ETL":
                _timestamp = time.time()
                df_updatedate = self.spark.sql(f"SELECT MAX(updatedate) max_update_date, MIN(updatedate) min_update_date FROM {df_name}").first()
                self.minUpdate = df_updatedate['min_update_date']
                self.maxUpdate = df_updatedate['max_update_date']
                start = df_updatedate['min_update_date']
                end = df_updatedate['max_update_date']
                self.timelog['get_updatedate'] = time.time() - _timestamp

            # GET FILTERS

            for row in deleted_transaction_ids:
                tobeDeleted.append(str(row))

            for rdd in rdds:
                tobeDeleted.append(str(rdd['id_transaction'])) # for cleaning

                if rdd['status'] == '1':
                    martDaily.append((rdd['M_User_id_user'], str(rdd['tgl']), rdd['M_Cabang_id_cabang'], int(rdd['tax_base_type'])))


                    if action != 'ETL':
                        # only push non etl for monthly yearly (recovery)
                        martMonthly.append((rdd['M_User_id_user'], rdd['tahun'], rdd['bulan'], rdd['M_Cabang_id_cabang'], int(rdd['tax_base_type'])))
                        martYearly.append((rdd['M_User_id_user'], rdd['tahun'], rdd['M_Cabang_id_cabang'], int(rdd['tax_base_type'])))
                else:
                    # else delete
                    martDeletedDaily.append((rdd['M_User_id_user'], str(rdd['tgl']), rdd['M_Cabang_id_cabang'], int(rdd['tax_base_type'])))
                    martDeletedMonthly.append((rdd['M_User_id_user'], rdd['tahun'], rdd['bulan'], rdd['M_Cabang_id_cabang'], int(rdd['tax_base_type'])))
                    martDeletedYearly.append((rdd['M_User_id_user'], rdd['tahun'], rdd['M_Cabang_id_cabang'], int(rdd['tax_base_type'])))

            if action == "ETL":
                # if action is not recovery, we only want to compute monthly and yearly which transactions is on previous month or previous year.
                for rdd in self.spark.sql(f"SELECT M_User_id_user, M_Cabang_id_cabang, tax_base_type, tahun , bulan  FROM {df_name} WHERE status = '1' AND ((tahun < {start.year}) OR (bulan < {start.month} and tahun = {start.year}))").rdd.collect():
                    martMonthly.append((rdd['M_User_id_user'], rdd['tahun'], rdd['bulan'], rdd['M_Cabang_id_cabang'], int(rdd['tax_base_type'])))

                for rdd in self.spark.sql(f"SELECT M_User_id_user, M_Cabang_id_cabang, tax_base_type, tahun  FROM {df_name} WHERE status = '1' AND tahun < {start.year} ").rdd.collect():
                    martYearly.append((rdd['M_User_id_user'], rdd['tahun'], rdd['M_Cabang_id_cabang'], int(rdd['tax_base_type'])))

            # chunks filters and remove duplicates
            tobeDeleted = [tobeDeleted[i:i + self.n_size_delete] for i in range(0, len(tobeDeleted), self.n_size_delete)]

            martDaily = list(set(martDaily))
            martDeletedDaily = list(set(martDeletedDaily))
            martDaily = ([martDaily[i:i + self.n_size_insert] for i in range(0, len(martDaily), self.n_size_insert)], [(martDaily + martDeletedDaily)[i:i + self.n_size_delete] for i in range(0, len((martDaily + martDeletedDaily)), self.n_size_delete)])

            martMonthly = list(set(martMonthly))
            martDeletedMonthly = list(set(martDeletedMonthly))
            martMonthly = ([martMonthly[i:i + self.n_size_insert] for i in range(0, len(martMonthly), self.n_size_insert)], [(martMonthly+ martDeletedMonthly)[i:i + self.n_size_delete] for i in range(0, len((martMonthly+ martDeletedMonthly)), self.n_size_delete)])

            martYearly = list(set(martYearly))
            martDeletedYearly = list(set(martDeletedYearly))
            martYearly = ([martYearly[i:i + self.n_size_insert] for i in range(0, len(martYearly), self.n_size_insert)], [(martYearly +martDeletedYearly)[i:i + self.n_size_delete] for i in range(0, len((martYearly+martDeletedYearly)), self.n_size_delete)])

        return (postfix,
                tobeDeleted,
                martDaily,
                martMonthly,
                martYearly)

    def _extract(self, transaction_ids:list, postfix:str = ''):

        query = f"""SELECT
                        t.id_transaction,
                        t.M_User_id_user,
                        t.M_Cabang_id_cabang,
                        t.transaction_tgl,
                        DATE(t.transaction_tgl) tgl,
                        MONTH(t.transaction_tgl) bulan,
                        YEAR(t.transaction_tgl) tahun,
                        t.transaction_no_nota,
                        t.transaction_total,
                        IFNULL(
                        CASE
                        	WHEN t.transaction_type_detail != ''
                        	THEN IF(
                        		NOT JSON_VALID(transaction_type_detail) OR CAST(t.transaction_type_detail->'$.tax_base_type' AS UNSIGNED) = 0,
                        		CAST(1 AS UNSIGNED), CAST(t.transaction_type_detail->'$.tax_base_type' AS UNSIGNED)
                        	)
                        	ELSE CAST(1 AS UNSIGNED)
                        END, CAST(1 AS UNSIGNED)) as tax_base_type,
                        CASE
                        	WHEN tde.id_transaction_detail_extend IS NULL
                            THEN t.transaction_tax_nominal
                        	ELSE SUM(td.transaction_detail_total_pajak_produk)
                        END AS pajak_transaksi,
                        IFNULL(SUM(td.transaction_detail_pajak_produk), 0) as total_pajak_produk,
                        t.updatedate,
                        t.status
                    FROM Transactions t
                    LEFT JOIN Transaction_Detail td ON t.id_transaction = td.Transactions_id_transaction
                    LEFT JOIN transaction_detail_extend tde ON tde.Transaction_Detail_id_transaction_detail = td.id_transaction_detail
                    WHERE
                        t.Transaction_purpose IN ('5', '9')
                        AND t.status IN ('1', '4', '9')
                        AND t.id_transaction IN ({','.join(transaction_ids)})
                        AND t.transaction_tgl > '1970-01-01 00:00:00'
                        AND t.transaction_refund = 0
                        AND t.transaction_no_nota IS NOT NULL
                    GROUP BY t.id_transaction
            """
        _timestamp = time.time()

        df_trx = self.spark.read.format("jdbc").option("url",self.credentials['source']['url']) \
                .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "(" + query + ") sdtable") \
                .option("user", self.credentials['source']['conf']['USER']).option("password", self.credentials['source']['conf']['PASS']).load()
        self.pushCachedTable(df_trx, f'df_tax_trx_{postfix}')
        self.timelog['extract_trx'] = time.time() - _timestamp

        _timestamp = time.time()
        rdds = self.spark.sql(f'SELECT id_transaction, M_User_id_user, tgl, bulan, tahun, tax_base_type, M_Cabang_id_cabang, status  FROM df_tax_trx_{postfix}').rdd.collect()
        self.timelog['collect_tax_rdd'] = time.time() - _timestamp

        return rdds, f'df_tax_trx_{postfix}'

    def transform(self, postfix:str):
        _timestamp = time.time()
        spark_query = f"""
            SELECT
                trx.id_transaction as raw_mart_tax_report_id_transaction,
                trx.M_User_id_user as m_user_id_user,
                trx.M_Cabang_id_cabang as m_cabang_id_cabang,
                trx.transaction_tgl as raw_mart_tax_report_datetime,
                DATE(trx.transaction_tgl) as raw_mart_tax_report_date,
                trx.transaction_no_nota as raw_mart_tax_report_no_nota,
                trx.transaction_total raw_mart_tax_report_transaction_value,
                IFNULL(trx.tax_base_type, 1) as raw_mart_tax_report_base_type,
                trx.pajak_transaksi as raw_mart_tax_report_tax_value,
                trx.total_pajak_produk as raw_mart_tax_report_tax_product
            FROM df_tax_trx_{postfix} trx
            WHERE trx.status = '1'"""
        df = self.spark.sql(spark_query)
        self.timelog['transform_raw'] = time.time() - _timestamp
        return df

    def write(self, df:DataFrame):
        _timestamp = time.time()
        df.write.jdbc(url=f"{self.credentials['target']['url']}&stringtype=unspecified",
                             table="mart_prd.raw_mart_tax_report",
                             mode=self.credentials['target']['mode'],
                             properties=self.credentials['target']['properties'])
        self.timelog['write_raw'] = time.time() - _timestamp

    def delete(self, filters:list):
        _timestamp = time.time()
        sql = f"DELETE FROM mart_prd.raw_mart_tax_report WHERE raw_mart_tax_report_id_transaction IN ({','.join(filters)})"
        executePostgres(sql, self.credentials['target']['conf'])
        _runtime = time.time() - _timestamp

        if "delete_raw" in self.timelog:
            self.timelog['delete_raw'] += _runtime
        else:
            self.timelog['delete_raw'] = _runtime

    def fetchPreviousRawDaily(self, filters:list):
        dfPrevRaw = None
        for chunk in filters:
            query = f"""
                SELECT
                    m_user_id_user,
                    raw_mart_tax_report_date,
                    m_cabang_id_cabang,
                    raw_mart_tax_report_base_type
                FROM mart_prd.raw_mart_tax_report
                WHERE raw_mart_tax_report_id_transaction
                    IN ({','.join(repr(filter) for filter in chunk)})
            """
            tempdf = self.spark.read.format("jdbc").option("url",self.credentials['target']['url']) \
                    .option("driver", "org.postgresql.Driver").option("dbtable", "(" + query + ") sdtable") \
                    .option("user", self.credentials['target']['conf']['USER']).option("password", self.credentials['target']['conf']['PASS']).load()

            dfPrevRaw = tempdf if dfPrevRaw == None else dfPrevRaw.union(tempdf)

        lTemp = []
        for i in dfPrevRaw.rdd.collect():
            lTemp.append((i['m_user_id_user'], i['raw_mart_tax_report_date'].strftime('%Y-%m-%d'), i['m_cabang_id_cabang'], i['raw_mart_tax_report_base_type']))

        return [lTemp]
