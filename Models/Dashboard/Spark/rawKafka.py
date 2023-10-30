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

        martHourly          = []
        martDeletedHourly   = []
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
                    martHourly.append((rdd['M_User_id_user'], str(rdd['tgl']), rdd['jam'], rdd['order_type'], rdd['M_Cabang_id_cabang']))
                    martDaily.append((rdd['M_User_id_user'], str(rdd['tgl']), rdd['order_type'], rdd['M_Cabang_id_cabang']))


                    if action != 'ETL':
                        # only push non etl for monthly yearly (recovery)
                        martMonthly.append((rdd['M_User_id_user'], rdd['tahun'], rdd['bulan'], rdd['order_type'], rdd['M_Cabang_id_cabang']))
                        martYearly.append((rdd['M_User_id_user'], rdd['tahun'], rdd['order_type'], rdd['M_Cabang_id_cabang']))
                else:
                    # else delete
                    martDeletedHourly.append((rdd['M_User_id_user'], str(rdd['tgl']), rdd['jam'], rdd['order_type'], rdd['M_Cabang_id_cabang']))
                    martDeletedDaily.append((rdd['M_User_id_user'], str(rdd['tgl']), rdd['order_type'], rdd['M_Cabang_id_cabang']))
                    martDeletedMonthly.append((rdd['M_User_id_user'], rdd['tahun'], rdd['bulan'], rdd['order_type'], rdd['M_Cabang_id_cabang']))
                    martDeletedYearly.append((rdd['M_User_id_user'], rdd['tahun'], rdd['order_type'], rdd['M_Cabang_id_cabang']))

            if action == "ETL":
                # if action is not recovery, we only want to compute monthly and yearly which transactions is on previous month or previous year.
                for rdd in self.spark.sql(f"SELECT M_User_id_user, M_Cabang_id_cabang, order_type, tahun , bulan  FROM {df_name} WHERE status = '1' AND ((tahun < {start.year}) OR (bulan < {start.month} and tahun = {start.year}))").rdd.collect():
                    martMonthly.append((rdd['M_User_id_user'], rdd['tahun'], rdd['bulan'], rdd['order_type'], rdd['M_Cabang_id_cabang']))

                for rdd in self.spark.sql(f"SELECT M_User_id_user, M_Cabang_id_cabang, order_type, tahun  FROM {df_name} WHERE status = '1' AND tahun < {start.year} ").rdd.collect():
                    martYearly.append((rdd['M_User_id_user'], rdd['tahun'], rdd['order_type'], rdd['M_Cabang_id_cabang']))

            # chunks filters and remove duplicates
            tobeDeleted = [tobeDeleted[i:i + self.n_size_delete] for i in range(0, len(tobeDeleted), self.n_size_delete)]

            martHourly = list(set(martHourly))
            martDeletedHourly = list(set(martDeletedHourly))
            martHourly = ([martHourly[i:i + self.n_size_insert] for i in range(0, len(martHourly), self.n_size_insert)], [(martHourly + martDeletedHourly)[i:i + self.n_size_delete] for i in range(0, len((martHourly + martDeletedHourly)), self.n_size_delete)])

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
                martHourly,
                martDaily,
                martMonthly,
                martYearly)

    def _extract(self, transaction_ids:list, postfix:str = ''):
        # tl:dr
        # using Join is more optimized and cost efficient, although the data might delayed slightly to valid. But eventually it will be valid if once stop moving.


        # Using Join 718366 row(s) fetched - 7.679s (481ms fetch), on 2023-06-17 at 20:58:11
        # Using IN 29622 row(s) fetched - 856ms (19ms fetch), on 2023-06-17 at 20:57:06 -> 718K / 30K * ~ 1s is more than join runtime

        # Therefore, using Join is better using IN id_transactions.
        # What is the other consideration, the data is moving?
        # Ex: We need to fetch : Transaction, Transaction_Detail, Transaction_Addon_detail , Transaction_has_Payment_Method, Transaction_Commission
        # We fetch separately using Join -> Transaction , Transaction Join Transaction_Detail, Transaction Join Transaction_Addon_Detail
        # Transaction -> id(s) are 1,2,3
        # Transaction x Transaction_Detail -> still 1,2,3
        # ...
        # ...
        # Transaction x Transaction_Commission -> only 1 -> 2 3 are updated in current interval in database while we fetch other datas.
        # ^ Is it safe?
        # Yes no, for current iteration data will be invalid
        # But eventually, the data will be valid because we will meet id 2 3 again. e.g.,
        # Transaction -> id (s) are 4, 2, 3

        # Other consideration
        # If we find this case, using IN will also costly -> we fetch 2 & 3 twice using IN.


        # fetch t.status IN 1,4,9 to clean raw which is canceled (4 or 9)
        query = f"""SELECT
                        t.id_transaction,
                        IF(t.transaction_refund > 0 OR COALESCE(t.transaction_refund_reason, '') != '', 1, 0) is_refund,
                        t.M_User_id_user,
                        t.M_Cabang_id_cabang,
                        t.transaction_tgl,
                        HOUR(t.transaction_tgl) jam,
                        DATE(t.transaction_tgl) tgl,
                        MONTH(t.transaction_tgl) bulan,
                        YEAR(t.transaction_tgl) tahun,
                        t.transaction_total,
                        t.transaction_refund,
                        t.transaction_refund_reason,
                        COALESCE(t.transaction_pisah, 0) transaction_pisah,
                        t.transaction_no_nota_parent,
                        IFNULL(t.transaction_type, 99) as order_type,
                        t.updatedate,
                        t.status
                    FROM Transactions t
                    WHERE
                        t.Transaction_purpose IN ('5', '9')
                        AND t.status IN ('1', '4', '9')
                        AND t.id_transaction IN ({','.join(transaction_ids)})
                        AND t.transaction_tgl > '1970-01-01 00:00:00'

                        """
        _timestamp = time.time()

        df_trx = self.spark.read.format("jdbc").option("url",self.credentials['source']['url']) \
                .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "(" + query + ") sdtable") \
                .option("user", self.credentials['source']['conf']['USER']).option("password", self.credentials['source']['conf']['PASS']).load()
        self.pushCachedTable(df_trx, f'df_salesdashboard_trx_{postfix}')
        self.timelog['extract_trx'] = time.time() - _timestamp

        _timestamp = time.time()
        rdds = self.spark.sql(f'SELECT id_transaction, M_User_id_user, jam, tgl, bulan, tahun, M_Cabang_id_cabang, order_type, status  FROM df_salesdashboard_trx_{postfix}').rdd.collect()
        self.timelog['collect_trx_rdd'] = time.time() - _timestamp


        # fetch only status 1
        query = f"""SELECT
                        t.id_transaction,
                        COALESCE(SUM(td.transaction_detail_jumlah), 0) as product_trx,
                        COALESCE(SUM(td.transaction_detail_jumlah * td.transaction_detail_price_modal), 0) AS hpp_produk,
                        COALESCE(SUM(td.transaction_detail_total_price), 0) as penjualan_kotor
                    FROM Transactions t
                    LEFT JOIN Transaction_Detail td ON t.id_transaction = td.Transactions_id_transaction
                    WHERE
                        t.Transaction_purpose IN ('5', '9')
                        AND t.status IN ('1')
                        AND t.id_transaction IN ({','.join(transaction_ids)})
                        AND t.transaction_tgl > '1970-01-01 00:00:00'
                    GROUP BY 1"""
        _timestamp = time.time()
        df_product = self.spark.read.format("jdbc").option("url",self.credentials['source']['url']) \
                .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "(" + query + ") sdtable") \
                .option("user", self.credentials['source']['conf']['USER']).option("password", self.credentials['source']['conf']['PASS']).load()
        self.pushCachedTable(df_product, f'df_salesdashboard_product_{postfix}')
        self.timelog['extract_product'] = time.time() - _timestamp



        query = f"""SELECT
                        t.id_transaction,
                        COALESCE(SUM(tad.transaction_addon_detail_quantity *  transaction_addon_detail_quantity_bahan * tad.transaction_addon_detail_harga_modal), 0) AS hpp_addon
                    FROM Transactions t
                    LEFT JOIN Transaction_Detail td ON t.id_transaction = td.Transactions_id_transaction
                    LEFT JOIN Transaction_Addon_Detail tad on tad.Transaction_Detail_id_transaction_detail = td.id_transaction_detail
                    WHERE
                        t.Transaction_purpose IN ('5', '9')
                        AND t.status IN ('1')
                        AND t.id_transaction IN ({','.join(transaction_ids)})
                        AND t.transaction_tgl > '1970-01-01 00:00:00'
                    GROUP BY 1"""
        _timestamp = time.time()
        df_addondetail = self.spark.read.format("jdbc").option("url",self.credentials['source']['url']) \
                .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "(" + query + ") sdtable") \
                .option("user", self.credentials['source']['conf']['USER']).option("password", self.credentials['source']['conf']['PASS']).load()
        self.pushCachedTable(df_addondetail, f'df_salesdashboard_addon_{postfix}')
        self.timelog['extract_addon'] = time.time() - _timestamp


        query = f"""SELECT
                        t.id_transaction,
                        COALESCE(SUM(tc.commission_total_nominal), 0) as komisi
                    FROM Transactions t
                    JOIN Transaction_Commission tc ON t.id_transaction = tc.Transactions_id_transaction
                    WHERE
                        t.Transaction_purpose IN ('5', '9')
                        AND t.status IN ('1')
                        AND t.id_transaction IN ({','.join(transaction_ids)})
                        AND t.transaction_tgl > '1970-01-01 00:00:00'
                        AND (
                            (t.transaction_pisah = 1 AND t.transaction_no_payment <= 1)
                            OR t.transaction_pisah != 1  OR t.transaction_pisah IS NULL
                        )
                        AND t.transaction_tgl_payment IS NOT NULL

                    GROUP BY 1"""

        _timestamp = time.time()
        df_commission = self.spark.read.format("jdbc").option("url",self.credentials['source']['url']) \
                .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "(" + query + ") sdtable") \
                .option("user", self.credentials['source']['conf']['USER']).option("password", self.credentials['source']['conf']['PASS']).load()
        self.pushCachedTable(df_commission, f'df_salesdashboard_commission_{postfix}')
        self.timelog['extract_commission'] = time.time() - _timestamp


        query = f"""SELECT
                        id_transaction,
                        COALESCE(SUM(thpm.transaction_has_payment_method_MDR), 0) AS mdr,
                        CASE
                             WHEN thpm.Payment_method_id_payment_method = 1 THEN
                                 COALESCE(SUM(thpm.transaction_has_payment_method_value - t.transaction_kembalian), 0)
                             ELSE
                                 COALESCE(SUM(thpm.transaction_has_payment_method_value), 0)
                        END AS payment_value
                    FROM Transactions t
                    LEFT JOIN Transaction_has_Payment_Method thpm ON t.id_transaction = thpm.Transaction_id_transaction
                    WHERE
                        t.Transaction_purpose IN ('5', '9')
                        AND t.status IN ('1')
                        AND t.id_transaction IN ({','.join(transaction_ids)})
                        AND t.transaction_tgl > '1970-01-01 00:00:00'
                    GROUP BY 1"""

        _timestamp = time.time()
        df_mdr = self.spark.read.format("jdbc").option("url",self.credentials['source']['url']) \
                .option("driver", "com.mysql.jdbc.Driver").option("dbtable", "(" + query + ") sdtable") \
                .option("user", self.credentials['source']['conf']['USER']).option("password", self.credentials['source']['conf']['PASS']).load()
        self.pushCachedTable(df_mdr, f'df_salesdashboard_mdr_{postfix}')
        self.timelog['extract_mdr'] = time.time() - _timestamp

        return rdds, f'df_salesdashboard_trx_{postfix}'

    def transform(self, postfix:str):
        _timestamp = time.time()
        spark_query = f"""
            SELECT
                trx.id_transaction as raw_mart_sales_dashboard_id_transaction,
                trx.M_User_id_user as m_user_id_user,
                trx.M_Cabang_id_cabang as m_cabang_id_cabang,
                CASE WHEN trx.is_refund = 1 THEN true ELSE false END as raw_mart_sales_dashboard_is_refund,
                DATE_FORMAT(transaction_tgl,'yyyy-MM-dd H:m:s +07:00') as raw_mart_sales_dashboard_datetime,
                DATE(trx.transaction_tgl) as raw_mart_sales_dashboard_date,
                trx.transaction_total as raw_mart_sales_dashboard_sales_value,
                IFNULL(trx_mdr.payment_value, 0) as raw_mart_sales_dashboard_payment_value,
                IFNULL(trx_detail.penjualan_kotor,0) as raw_mart_sales_dashboard_gross_sales,
                trx.transaction_refund as raw_mart_sales_dashboard_refund,
                trx.order_type as raw_mart_sales_dashboard_order_type,
                IFNULL(trx_detail.product_trx,0) as raw_mart_sales_dashboard_product,
                IFNULL(trx_detail.hpp_produk,0) as raw_mart_sales_dashboard_hpp_product,
                IFNULL(trx_addon.hpp_addon, 0) as raw_mart_sales_dashboard_hpp_addon,
                IFNULL(trx_mdr.mdr , 0) as raw_mart_sales_dashboard_mdr,
                IFNULL(trx_commission.komisi, 0) as raw_mart_sales_dashboard_commission,
                CASE WHEN
                    (COALESCE(trx.transaction_pisah, 0) = 1 AND
                    (trx.transaction_no_nota_parent IS NOT NULL AND trx.transaction_no_nota_parent != ''))
    	               OR
                    (COALESCE(trx.transaction_refund, 0) > 0 OR coalesce(trx.transaction_refund_reason, '') != '')
    	        THEN 0 ELSE 1 END as raw_mart_sales_dashboard_count_transaction
            FROM df_salesdashboard_trx_{postfix} trx
                LEFT JOIN df_salesdashboard_mdr_{postfix} trx_mdr ON trx.id_transaction = trx_mdr.id_transaction
                LEFT JOIN df_salesdashboard_product_{postfix} trx_detail ON trx.id_transaction = trx_detail.id_transaction
                LEFT JOIN df_salesdashboard_addon_{postfix} trx_addon ON trx.id_transaction = trx_addon.id_transaction
                LEFT JOIN df_salesdashboard_commission_{postfix} trx_commission on trx.id_transaction = trx_commission.id_transaction
            WHERE trx.status = '1'"""
        df = self.spark.sql(spark_query)
        self.timelog['transform_raw'] = time.time() - _timestamp
        return df

    def write(self, df:DataFrame):
        _timestamp = time.time()
        df.write.jdbc(url=f"{self.credentials['target']['url']}&stringtype=unspecified",
                             table="mart_prd.raw_mart_sales_dashboard",
                             mode=self.credentials['target']['mode'],
                             properties=self.credentials['target']['properties'])
        self.timelog['write_raw'] = time.time() - _timestamp

    def delete(self, filters:list):
        _timestamp = time.time()
        sql = f"DELETE FROM mart_prd.raw_mart_sales_dashboard WHERE raw_mart_sales_dashboard_id_transaction IN ({','.join(filters)})"
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
                    SELECT m_user_id_user,
                            raw_mart_sales_dashboard_date,
                            raw_mart_sales_dashboard_order_type,
                            m_cabang_id_cabang
                    FROM mart_prd.raw_mart_sales_dashboard
                    WHERE raw_mart_sales_dashboard_id_transaction
                        IN ({','.join(repr(filter) for filter in chunk)})
                """
                tempdf = self.spark.read.format("jdbc").option("url",self.credentials['target']['url']) \
                        .option("driver", "org.postgresql.Driver").option("dbtable", "(" + query + ") sdtable") \
                        .option("user", self.credentials['target']['conf']['USER']).option("password", self.credentials['target']['conf']['PASS']).load()

                dfPrevRaw = tempdf if dfPrevRaw == None else dfPrevRaw.union(tempdf)

            lTemp = []
            for i in dfPrevRaw.rdd.collect():
                lTemp.append((i['m_user_id_user'], i['raw_mart_sales_dashboard_date'].strftime('%Y-%m-%d'), i['raw_mart_sales_dashboard_order_type'], i['m_cabang_id_cabang']))

            return [lTemp]

    def fetchPreviousRawHourly(self, filters:list):
            dfPrevRaw = None
            for chunk in filters:
                query = f"""
                    SELECT m_user_id_user,
                            raw_mart_sales_dashboard_date,
                            date_part('hour',raw_mart_sales_dashboard_datetime) as hourly,
                            raw_mart_sales_dashboard_order_type,
                            m_cabang_id_cabang
                    FROM mart_prd.raw_mart_sales_dashboard
                    WHERE raw_mart_sales_dashboard_id_transaction
                        IN ({','.join(repr(filter) for filter in chunk)})
                """
                tempdf = self.spark.read.format("jdbc").option("url",self.credentials['target']['url']) \
                        .option("driver", "org.postgresql.Driver").option("dbtable", "(" + query + ") sdtable") \
                        .option("user", self.credentials['target']['conf']['USER']).option("password", self.credentials['target']['conf']['PASS']).load()

                dfPrevRaw = tempdf if dfPrevRaw == None else dfPrevRaw.union(tempdf)

            lTemp = []
            for i in dfPrevRaw.rdd.collect():
                lTemp.append((i['m_user_id_user'], i['raw_mart_sales_dashboard_date'].strftime('%Y-%m-%d'), int(i['hourly']), i['raw_mart_sales_dashboard_order_type'], i['m_cabang_id_cabang']))

            return [lTemp]
