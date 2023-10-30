from .raw import ETL
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from Utility.sql import *
import time

class Daily(ETL):

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
                SELECT
                    m_user_id_user,
                    m_cabang_id_cabang,
                    raw_mart_tax_report_date,
                    raw_mart_tax_report_base_type,
                    raw_mart_tax_report_transaction_value,
                    raw_mart_tax_report_tax_value,
                    raw_mart_tax_report_tax_product
                FROM mart_prd.raw_mart_tax_report
                WHERE (m_user_id_user, raw_mart_tax_report_date, m_cabang_id_cabang, raw_mart_tax_report_base_type)
                    IN ({','.join(repr(filter) for filter in chunk)})
            """
            tempdf = self.spark.read.format("jdbc").option("url",self.credentials['target']['url']) \
                    .option("driver", "org.postgresql.Driver").option("dbtable", "(" + query + ") sdtable") \
                    .option("user", self.credentials['target']['conf']['USER']).option("password", self.credentials['target']['conf']['PASS']).load()

            df = tempdf if df == None else df.union(tempdf)
        self.pushCachedTable(df, f'df_taxreport_mart_{postfix}')
        self.timelog['extract_mart_daily'] = time.time() - _timestamp
        return f'df_taxreport_mart_{postfix}'

    def _transform(self, name:str) -> DataFrame :
        _timestamp = time.time()
        spark_query = f"""
                SELECT
                    m_user_id_user,
                    m_cabang_id_cabang,
                    raw_mart_tax_report_date as mart_tax_report_date,
                    raw_mart_tax_report_base_type as mart_tax_report_base_type,
                    SUM(raw_mart_tax_report_transaction_value) as mart_tax_report_transaction_value,
                    SUM(raw_mart_tax_report_tax_value) as mart_tax_report_tax_value,
                    SUM(raw_mart_tax_report_tax_product) as mart_tax_report_tax_product
                FROM {name}
                GROUP BY m_user_id_user, raw_mart_tax_report_date, m_cabang_id_cabang, raw_mart_tax_report_base_type"""

        df = self.spark.sql(spark_query)
        self.timelog['transform_mart_daily'] = time.time() - _timestamp
        return df

    def write(self, df:DataFrame):
        _timestamp = time.time()
        df.write.jdbc(url=f"{self.credentials['target']['url']}&stringtype=unspecified",
                             table="mart_prd.mart_tax_report_daily",
                             mode=self.credentials['target']['mode'],
                             properties=self.credentials['target']['properties'])
        self.timelog['write_daily'] = time.time() - _timestamp

    def delete(self, filters:list):
        _timestamp = time.time()
        sql = f"DELETE FROM mart_prd.mart_tax_report_daily WHERE (m_user_id_user, mart_tax_report_date, m_cabang_id_cabang, mart_tax_report_base_type) IN ({','.join([repr(filter) for filter in filters])})"
        executePostgres(sql, self.credentials['target']['conf'])
        _runtime = time.time() - _timestamp

        if "delete_daily" in self.timelog:
            self.timelog['delete_daily'] += _runtime
        else:
            self.timelog['delete_daily'] = _runtime
