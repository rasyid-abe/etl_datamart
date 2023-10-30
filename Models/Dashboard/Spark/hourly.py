from .raw import ETL
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from Utility.sql import *
import time

class Hourly(ETL):

    def proceed(self, filters:list, postfix:str = ''):
        if len(filters) == 0:
            return
        df_name = self._extract(filters, postfix)
        self.write(self._transform(df_name))

    def _extract(self, filters:list, postfix:str):
        _timestamp = time.time()
        df = None
        for chunk in filters:
            query = f"""
                SELECT m_user_id_user,
                        m_cabang_id_cabang,
                        raw_mart_sales_dashboard_is_refund,
                        date_part('hour',raw_mart_sales_dashboard_datetime) as mart_sales_dashboard_hourly_hour,
                        raw_mart_sales_dashboard_date,
                        raw_mart_sales_dashboard_order_type,
                        raw_mart_sales_dashboard_refund,
                        raw_mart_sales_dashboard_commission,
                        raw_mart_sales_dashboard_mdr,
                        raw_mart_sales_dashboard_product,
                        raw_mart_sales_dashboard_sales_value,
                        raw_mart_sales_dashboard_payment_value,
                        raw_mart_sales_dashboard_gross_sales,
                        raw_mart_sales_dashboard_hpp_product,
                        raw_mart_sales_dashboard_hpp_addon,
                        raw_mart_sales_dashboard_count_transaction,
                        raw_mart_sales_dashboard_id_transaction
                FROM mart_prd.raw_mart_sales_dashboard
                WHERE (m_user_id_user, raw_mart_sales_dashboard_date, date_part('hour',raw_mart_sales_dashboard_datetime), raw_mart_sales_dashboard_order_type, m_cabang_id_cabang)
                    IN ({','.join(repr(filter) for filter in chunk)})
            """
            tempdf = self.spark.read.format("jdbc").option("url",self.credentials['target']['url']) \
                    .option("driver", "org.postgresql.Driver").option("dbtable", "(" + query + ") sdtable") \
                    .option("user", self.credentials['target']['conf']['USER']).option("password", self.credentials['target']['conf']['PASS']) \
                    .option("customSchema", """raw_mart_sales_dashboard_refund DOUBLE,
                                                raw_mart_sales_dashboard_commission DOUBLE,
                                                raw_mart_sales_dashboard_mdr DOUBLE,
                                                raw_mart_sales_dashboard_sales_value DOUBLE,
                                                raw_mart_sales_dashboard_payment_value DOUBLE,
                                                raw_mart_sales_dashboard_gross_sales DOUBLE,
                                                raw_mart_sales_dashboard_hpp_product DOUBLE,
                                                raw_mart_sales_dashboard_hpp_addon DOUBLE """).load()

            df = tempdf if df == None else df.union(tempdf)
        self.pushCachedTable(df, f'df_salesdashboard_mart_hourly_{postfix}')
        self.timelog['extract_mart_hourly'] = time.time() - _timestamp
        return f'df_salesdashboard_mart_hourly_{postfix}'

    def _transform(self, name:str) -> DataFrame :
        _timestamp = time.time()
        spark_query = f"""
                SELECT
                    m_user_id_user,
                    m_cabang_id_cabang,
                    raw_mart_sales_dashboard_order_type as mart_sales_dashboard_hourly_order_type,
                    mart_sales_dashboard_hourly_hour,
                    raw_mart_sales_dashboard_date AS mart_sales_dashboard_hourly_date,
                    SUM(IF (raw_mart_sales_dashboard_is_refund , 0 , raw_mart_sales_dashboard_sales_value )) mart_sales_dashboard_hourly_sales_value,
                    SUM(IF (raw_mart_sales_dashboard_is_refund , 0 , raw_mart_sales_dashboard_payment_value )) mart_sales_dashboard_hourly_payment_value,
                    SUM (
                    	case when raw_mart_sales_dashboard_is_refund is false then
                    		raw_mart_sales_dashboard_sales_value
                    		- raw_mart_sales_dashboard_mdr
                    		- raw_mart_sales_dashboard_hpp_product
                    		- raw_mart_sales_dashboard_hpp_addon
                    		- raw_mart_sales_dashboard_commission
                    	else
                    		raw_mart_sales_dashboard_sales_value
                    		+ raw_mart_sales_dashboard_mdr
                    		+ raw_mart_sales_dashboard_hpp_product
                    		+ raw_mart_sales_dashboard_hpp_addon
                    		+ raw_mart_sales_dashboard_commission
                    	end
                    ) AS mart_sales_dashboard_hourly_gross_value,
                    SUM(IF (raw_mart_sales_dashboard_is_refund , 0 , raw_mart_sales_dashboard_gross_sales )) mart_sales_dashboard_hourly_gross_sales,
                    SUM(IF (raw_mart_sales_dashboard_is_refund , 0 , raw_mart_sales_dashboard_product )) mart_sales_dashboard_hourly_product_qty,
                    SUM(raw_mart_sales_dashboard_count_transaction) mart_sales_dashboard_hourly_transaction,
                    SUM(IF (raw_mart_sales_dashboard_is_refund , raw_mart_sales_dashboard_refund , 0 )) mart_sales_dashboard_hourly_refund,
                    (
                        SUM(IF (not raw_mart_sales_dashboard_is_refund , raw_mart_sales_dashboard_commission , 0 )) -
                        SUM(IF (raw_mart_sales_dashboard_is_refund , raw_mart_sales_dashboard_commission , 0 ))
                    ) as  mart_sales_dashboard_hourly_commission
                FROM {name}
                GROUP BY m_user_id_user, raw_mart_sales_dashboard_date, mart_sales_dashboard_hourly_hour, m_cabang_id_cabang, raw_mart_sales_dashboard_order_type"""

        df = self.spark.sql(spark_query)
        self.timelog['transform_mart_hourly'] = time.time() - _timestamp
        return df

    def write(self, df:DataFrame):
        _timestamp = time.time()
        df.write.jdbc(url=f"{self.credentials['target']['url']}&stringtype=unspecified",
                             table="mart_prd.mart_sales_dashboard_hourly",
                             mode=self.credentials['target']['mode'],
                             properties=self.credentials['target']['properties'])
        self.timelog['write_hourly'] = time.time() - _timestamp

    def delete(self, filters:list):
        _timestamp = time.time()
        sql = f"DELETE FROM mart_prd.mart_sales_dashboard_hourly WHERE (m_user_id_user, mart_sales_dashboard_hourly_date,mart_sales_dashboard_hourly_hour, mart_sales_dashboard_hourly_order_type, m_cabang_id_cabang) IN ({','.join([repr(filter) for filter in filters])})"
        executePostgres(sql, self.credentials['target']['conf'])
        _runtime = time.time() - _timestamp

        if "delete_hourly" in self.timelog:
            self.timelog['delete_hourly'] += _runtime
        else:
            self.timelog['delete_hourly'] = _runtime
