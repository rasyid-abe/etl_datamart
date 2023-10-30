from .raw import ETL
from Utility.Pandas.PandasService import PandasService  as pds
import pandas as pd, time
from Utility.sql import *
from datetime import date, timedelta


class Monthly(ETL):
    def _getFirstLastDay(self, month:int, year:int):
        first_date = date(year, month, 1)
        last_date =  date(year +1, 1, 1) - timedelta(days=1)  if month == 12 else date(year, month+1, 1) - timedelta(days=1)
        return (first_date,last_date)

    def proceed(self, filters : list):
        if len(filters) == 0:
            return

        self.write(self._transform(self._extract(filters)))


    def _extract(self, filters : list):
        _timestamp = time.time()

        conditions = []
        for chunk in filters:
            temp = []
            for item in chunk:
                item = item.replace("(","").replace(")","").replace(" ","").split(",")
                first, end = self._getFirstLastDay(int(item[2]), int(item[1]))
                temp.append(f"(m_user_id_user = {item[0]} AND ( mart_sales_dashboard_daily_date BETWEEN '{first}' AND '{end}') AND mart_sales_dashboard_daily_order_type = {item[3]} AND m_cabang_id_cabang = {item[4]})")
            conditions.append(" OR ".join(temp))

        df = pds.fetch(self.targetConUri, [
            f"""SELECT m_user_id_user,
                        m_cabang_id_cabang,
                        CAST(DATE_PART('month',mart_sales_dashboard_daily_date) as INT) as bulan,
                        CAST(DATE_PART('year',mart_sales_dashboard_daily_date) as INT) as tahun,
                        mart_sales_dashboard_daily_order_type,
                        mart_sales_dashboard_daily_sales_value,
                        mart_sales_dashboard_daily_payment_value,
                        mart_sales_dashboard_daily_gross_value,
                        mart_sales_dashboard_daily_gross_sales,
                        mart_sales_dashboard_daily_product_qty,
                        mart_sales_dashboard_daily_transaction,
                        mart_sales_dashboard_daily_refund,
                        mart_sales_dashboard_daily_commission
                FROM mart_prd.mart_sales_dashboard_daily
                WHERE {condition}
            """ for condition in conditions
        ])
        self.timelog['extract_mart_monthly'] = time.time() - _timestamp
        return df

    def _transform(self, df:pd.DataFrame, logName:str = None):
        _timestamp = time.time()
        df = pds.aggregate(df,
            {
                'mart_sales_dashboard_daily_sales_value':'sum',
                'mart_sales_dashboard_daily_payment_value':'sum',
                'mart_sales_dashboard_daily_gross_value':'sum',
                'mart_sales_dashboard_daily_gross_sales':'sum',
                'mart_sales_dashboard_daily_product_qty':'sum',
                'mart_sales_dashboard_daily_transaction':'sum',
                'mart_sales_dashboard_daily_refund':'sum',
                'mart_sales_dashboard_daily_commission':'sum',
            },[
                'm_user_id_user',
                'm_cabang_id_cabang',
                'mart_sales_dashboard_daily_order_type',
                'bulan',
                'tahun',
            ],
            renameCols={
                'mart_sales_dashboard_daily_order_type':   'mart_sales_dashboard_monthly_order_type',
                'mart_sales_dashboard_daily_sales_value':  'mart_sales_dashboard_monthly_sales_value',
                'mart_sales_dashboard_daily_payment_value':'mart_sales_dashboard_monthly_payment_value',
                'mart_sales_dashboard_daily_gross_value':  'mart_sales_dashboard_monthly_gross_value',
                'mart_sales_dashboard_daily_gross_sales':  'mart_sales_dashboard_monthly_gross_sales',
                'mart_sales_dashboard_daily_product_qty':  'mart_sales_dashboard_monthly_product_qty',
                'mart_sales_dashboard_daily_transaction':  'mart_sales_dashboard_monthly_transaction',
                'mart_sales_dashboard_daily_refund':       'mart_sales_dashboard_monthly_refund',
                'mart_sales_dashboard_daily_commission':   'mart_sales_dashboard_monthly_commission',
                'bulan':                                'mart_sales_dashboard_monthly_month',
                'tahun':                                'mart_sales_dashboard_monthly_year',
            }
        )
        self.timelog['transform_mart_monthly' if logName == None else logName] = time.time() - _timestamp
        return df

    def write(self, df:pd.DataFrame, name:str = None):
        self.timelog['write_monthly' if name == None else name] = pds.write(df, self.targetConUri, table='mart_sales_dashboard_monthly', cols=[str(col) for col in df.columns])

    def delete(self, filters:list):
        _timestamp = time.time()
        sql = f"DELETE FROM mart_prd.mart_sales_dashboard_monthly WHERE(m_user_id_user, mart_sales_dashboard_monthly_year, mart_sales_dashboard_monthly_month, mart_sales_dashboard_monthly_order_type, m_cabang_id_cabang) IN ({','.join(filters)})"
        executePostgres(sql, self.targetConfig)
        _runtime = time.time() - _timestamp

        if "delete_monthly" in self.timelog:
            self.timelog['delete_monthly'] += _runtime
        else:
            self.timelog['delete_monthly'] = _runtime

    def deleteTargeted(self, year:int, month:int, recoveryAt: str = None):
        _timestamp = time.time()
        sql = f"""DELETE FROM mart_prd.mart_sales_dashboard_monthly WHERE mart_sales_dashboard_monthly_year = {year} AND mart_sales_dashboard_monthly_month = {month} {recoveryAt if recoveryAt != None else ''}"""
        executePostgres(sql, self.targetConfig)
        self.timelog[f'delete_targeted_monthly_{month}_{year}'] = time.time() - _timestamp



    def transformTargeted(self, year:int, month:int, recoveryAt:str = None):
        _timestamp = time.time()
        first, end = self._getFirstLastDay(month, year)

        sql_queries = []
        while first <= end:
            sql_queries.append(f"""SELECT m_user_id_user,
                                        m_cabang_id_cabang,
                                        CAST(DATE_PART('month',mart_sales_dashboard_daily_date) as INT) as bulan,
                                        CAST(DATE_PART('year',mart_sales_dashboard_daily_date) as INT) as tahun,
                                        mart_sales_dashboard_daily_order_type,
                                        mart_sales_dashboard_daily_sales_value,
                                        mart_sales_dashboard_daily_payment_value,
                                        mart_sales_dashboard_daily_gross_value,
                                        mart_sales_dashboard_daily_gross_sales,
                                        mart_sales_dashboard_daily_product_qty,
                                        mart_sales_dashboard_daily_transaction,
                                        mart_sales_dashboard_daily_refund,
                                        mart_sales_dashboard_daily_commission
                                FROM mart_prd.mart_sales_dashboard_daily
                                WHERE  mart_sales_dashboard_daily_date = '{first.strftime('%Y-%m-%d')}' {recoveryAt if recoveryAt != None else ''}""")

            first = first +timedelta(days=1)

        df = pds.fetch(self.targetConUri, sql_queries)
        self.timelog[f'extract_mart_monthly_{month}{year}'] = time.time() - _timestamp

        return self._transform(df, logName = f'transform_monthly_{month}{year}')
