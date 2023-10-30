from .raw import ETL
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from Utility.sql import *
import time
from datetime import date, timedelta

class Yearly(ETL):
    def proceed(self, filters:list, postfix:str = '') :
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
                    mart_tax_report_year,
                    mart_tax_report_base_type,
                    mart_tax_report_transaction_value,
                    mart_tax_report_tax_value,
                    mart_tax_report_tax_product
                FROM mart_prd.mart_tax_report_monthly
                WHERE (m_user_id_user, mart_tax_report_year, m_cabang_id_cabang, mart_tax_report_base_type)
                    IN ({','.join([repr(filter) for filter in chunk])})"""

            tempdf = self.spark.read.format("jdbc").option("url",self.credentials['target']['url']) \
                    .option("driver", "org.postgresql.Driver").option("dbtable", "(" + query + ") sdtable") \
                    .option("user", self.credentials['target']['conf']['USER']).option("password", self.credentials['target']['conf']['PASS']).load()

            df = tempdf if df == None else df.union(tempdf)

        self.pushCachedTable(df, f'df_taxreport_mart_yearly_{postfix}')
        self.timelog['extract_mart_yearly'] = time.time() - _timestamp
        return f'df_taxreport_mart_yearly_{postfix}'

    def _transform(self, name:str) -> DataFrame :
        _timestamp = time.time()
        spark_query = f"""
                SELECT
                    m_user_id_user,
                    m_cabang_id_cabang,
                    mart_tax_report_year AS mart_tax_report_year,
                    mart_tax_report_base_type as mart_tax_report_base_type,
                    SUM(mart_tax_report_transaction_value) AS mart_tax_report_transaction_value,
                    SUM(mart_tax_report_tax_value) AS mart_tax_report_tax_value,
                    SUM(mart_tax_report_tax_product) AS mart_tax_report_tax_product
                FROM {name}
                GROUP BY m_user_id_user, mart_tax_report_year, m_cabang_id_cabang, mart_tax_report_base_type"""

        df = self.spark.sql(spark_query)
        self.timelog['transform_mart_yearly'] = time.time() - _timestamp
        return df

    def write(self, df:DataFrame, name:str = None):
        _timestamp = time.time()
        df.write.jdbc(url=f"{self.credentials['target']['url']}&stringtype=unspecified",
                             table="mart_prd.mart_tax_report_yearly",
                             mode=self.credentials['target']['mode'],
                             properties=self.credentials['target']['properties'])
        self.timelog['write_yearly' if name  == None else name] = time.time() - _timestamp

    def delete(self, filters:list):
        _timestamp = time.time()
        sql = f"""DELETE FROM mart_prd.mart_tax_report_yearly
                    WHERE (m_user_id_user, mart_tax_report_year, m_cabang_id_cabang, mart_tax_report_base_type)
                    IN ({','.join([repr(filter) for filter in filters])})"""
        executePostgres(sql, self.credentials['target']['conf'])
        _runtime = time.time() - _timestamp

        if "delete_yearly" in self.timelog:
            self.timelog['delete_yearly'] += _runtime
        else:
            self.timelog['delete_yearly'] = _runtime


    def deleteTargeted(self, year:int, recoveryAt:str = None):
        _timestamp = time.time()
        sql = f"""DELETE FROM mart_prd.mart_tax_report_yearly WHERE mart_tax_report_year = {year} {recoveryAt if recoveryAt != None else ''}"""
        executePostgres(sql, self.credentials['target']['conf'])
        self.timelog[f'delete_targeted_yearly_{year}'] = time.time() - _timestamp

    def transformTargeted(self, year:int, recoveryAt:str = None, postfix:str = ''):
        query = f"""
                SELECT
                    m_user_id_user,
                    m_cabang_id_cabang,
                    mart_tax_report_year,
                    mart_tax_report_base_type,
                    mart_tax_report_transaction_value,
                    mart_tax_report_tax_value,
                    mart_tax_report_tax_product
                FROM mart_prd.mart_tax_report_monthly
                WHERE  mart_tax_report_year = {year} {recoveryAt if recoveryAt != None else ''}"""
        df = self.spark.read.format("jdbc").option("url",self.credentials['target']['url']) \
                    .option("driver", "org.postgresql.Driver").option("dbtable", "(" + query + ") sdtable") \
                    .option("user", self.credentials['target']['conf']['USER']).option("password", self.credentials['target']['conf']['PASS']).load()

        df_targeted_name = f"df_targeted_taxreport_year_{year}_{postfix}"

        self.pushCachedTable(df, df_targeted_name)

        return self._transform(df_targeted_name)
