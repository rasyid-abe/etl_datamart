from .raw import ETL
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from Utility.sql import *
import time
from datetime import date, timedelta

class Monthly(ETL):
    def _getFirstLastDay(self, month:int, year:int):
        first_date = date(year, month, 1)
        last_date =  date(year +1, 1, 1) - timedelta(days=1)  if month == 12 else date(year, month+1, 1) - timedelta(days=1)
        return (first_date,last_date)

    def proceed(self, filters:list, postfix:str = ''):
        if len(filters) == 0:
            return
        df_name = self._extract(filters, postfix)
        self.write(self._transform(df_name))

    def _extract(self, filters:list, postfix:str):
        _timestamp = time.time()
        df = None
        for chunk in filters:
            conditions = []

            for item in chunk:
                first, end = self._getFirstLastDay(item[2], item[1])
                conditions.append(f"(m_user_id_user = {item[0]} AND ( mart_tax_report_date BETWEEN '{first}' AND '{end}') AND mart_tax_report_base_type = {item[3]} AND m_cabang_id_cabang = {item[4]})")

            query = f"""
                SELECT
                    m_user_id_user,
                    m_cabang_id_cabang,
                    mart_tax_report_date,
                    mart_tax_report_base_type,
                    mart_tax_report_transaction_value,
                    mart_tax_report_tax_value,
                    mart_tax_report_tax_product
                FROM mart_prd.mart_tax_report_daily
                WHERE {' OR '.join(conditions)}"""
            tempdf = self.spark.read.format("jdbc").option("url",self.credentials['target']['url']) \
                    .option("driver", "org.postgresql.Driver").option("dbtable", "(" + query + ") sdtable") \
                    .option("user", self.credentials['target']['conf']['USER']).option("password", self.credentials['target']['conf']['PASS']).load()

            df = tempdf if df == None else df.union(tempdf)

        self.pushCachedTable(df, f'df_taxreport_mart_monthly_{postfix}')
        self.timelog['extract_mart_monthly'] = time.time() - _timestamp
        return f'df_taxreport_mart_monthly_{postfix}'

    def _transform(self, name:str) -> DataFrame :
        _timestamp = time.time()
        spark_query = f"""
                SELECT
                    m_user_id_user,
                    m_cabang_id_cabang,
                    MONTH(mart_tax_report_date) AS mart_tax_report_month,
                    YEAR(mart_tax_report_date) AS mart_tax_report_year,
                    mart_tax_report_base_type AS mart_tax_report_base_type,
                    SUM(mart_tax_report_transaction_value) AS mart_tax_report_transaction_value,
                    SUM(mart_tax_report_tax_value) AS mart_tax_report_tax_value,
                    SUM(mart_tax_report_tax_product) AS mart_tax_report_tax_product
                FROM {name}
                GROUP BY m_user_id_user,
                    YEAR(mart_tax_report_date),
                    MONTH(mart_tax_report_date),
                    m_cabang_id_cabang, mart_tax_report_base_type"""

        df = self.spark.sql(spark_query)
        self.timelog['transform_mart_monthly'] = time.time() - _timestamp
        return df

    def write(self, df:DataFrame, name:str = None):
        _timestamp = time.time()
        df.write.jdbc(url=f"{self.credentials['target']['url']}&stringtype=unspecified",
                             table="mart_prd.mart_tax_report_monthly",
                             mode=self.credentials['target']['mode'],
                             properties=self.credentials['target']['properties'])
        self.timelog['write_monthly' if name  == None else name] = time.time() - _timestamp

    def delete(self, filters:list):
        _timestamp = time.time()
        sql = f"""DELETE FROM mart_prd.mart_tax_report_monthly
                    WHERE (m_user_id_user, mart_tax_report_year, mart_tax_report_month, m_cabang_id_cabang, mart_tax_report_base_type)
                    IN ({','.join([repr(filter) for filter in filters])})"""
        executePostgres(sql, self.credentials['target']['conf'])
        _runtime = time.time() - _timestamp

        if "delete_monthly" in self.timelog:
            self.timelog['delete_monthly'] += _runtime
        else:
            self.timelog['delete_monthly'] = _runtime


    def deleteTargeted(self, year:int, month:int, recoveryAt:str = None):
        _timestamp = time.time()
        sql = f"""DELETE FROM mart_prd.mart_tax_report_monthly WHERE mart_tax_report_year = {year} AND mart_tax_report_month = {month} {recoveryAt if recoveryAt != None else ''}"""
        executePostgres(sql, self.credentials['target']['conf'])
        self.timelog[f'delete_targeted_monthly_{month}_{year}'] = time.time() - _timestamp

    def transformTargeted(self, year:int, month:int, recoveryAt:str = None, postfix:str = ''):
        first, end = self._getFirstLastDay(month, year)

        query = f"""
                SELECT
                    m_user_id_user,
                    m_cabang_id_cabang,
                    mart_tax_report_date,
                    mart_tax_report_base_type,
                    mart_tax_report_transaction_value,
                    mart_tax_report_tax_value,
                    mart_tax_report_tax_product
                FROM mart_prd.mart_tax_report_daily
                WHERE mart_tax_report_date BETWEEN '{first}+07:00' AND '{end}+07:00' {recoveryAt if recoveryAt != None else ''}"""
        df = self.spark.read.format("jdbc").option("url",self.credentials['target']['url']) \
                    .option("driver", "org.postgresql.Driver").option("dbtable", "(" + query + ") sdtable") \
                    .option("user", self.credentials['target']['conf']['USER']).option("password", self.credentials['target']['conf']['PASS']).load()

        df_targeted_name = f"df_targeted_taxreport_{month}{year}_{postfix}"

        self.pushCachedTable(df, df_targeted_name)

        return self._transform(df_targeted_name)
