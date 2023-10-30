from .raw import ETL
from Utility.Pandas.PandasService import PandasService  as pds
import pandas as pd, time
from Utility.sql import *
from datetime import date, timedelta


class Yearly(ETL):
    def proceed(self, filters : list):
        if len(filters) == 0:
            return
        self.write(self._transform(self._extract(filters)))


    def _extract(self, filters : list):
        _timestamp = time.time()
        df = pds.fetch(self.targetConUri, [
            f"""SELECT m_user_id_user,
                        m_cabang_id_cabang,
                        mart_sales_dashboard_monthly_year,
                        mart_sales_dashboard_monthly_order_type,
                        mart_sales_dashboard_monthly_sales_value,
                        mart_sales_dashboard_monthly_payment_value,
                        mart_sales_dashboard_monthly_gross_value,
                        mart_sales_dashboard_monthly_gross_sales,
                        mart_sales_dashboard_monthly_product_qty,
                        mart_sales_dashboard_monthly_transaction,
                        mart_sales_dashboard_monthly_refund,
                        mart_sales_dashboard_monthly_commission
                FROM mart_prd.mart_sales_dashboard_monthly
                WHERE (m_user_id_user, mart_sales_dashboard_monthly_year, mart_sales_dashboard_monthly_order_type, m_cabang_id_cabang)
                    IN ({','.join(filter)})
            """ for filter in filters
        ])
        self.timelog['extract_mart_yearly'] = time.time() - _timestamp
        return df

    def _transform(self, df:pd.DataFrame, logName:str = None):
        _timestamp = time.time()
        df = pds.aggregate(df,
            {
                'mart_sales_dashboard_monthly_sales_value':'sum',
                'mart_sales_dashboard_monthly_payment_value':'sum',
                'mart_sales_dashboard_monthly_gross_value':'sum',
                'mart_sales_dashboard_monthly_gross_sales':'sum',
                'mart_sales_dashboard_monthly_product_qty':'sum',
                'mart_sales_dashboard_monthly_transaction':'sum',
                'mart_sales_dashboard_monthly_refund':'sum',
                'mart_sales_dashboard_monthly_commission':'sum',
            },[
                'm_user_id_user',
                'm_cabang_id_cabang',
                'mart_sales_dashboard_monthly_order_type',
                'mart_sales_dashboard_monthly_year',
            ],
            renameCols={
                'mart_sales_dashboard_monthly_order_type':   'mart_sales_dashboard_yearly_order_type',
                'mart_sales_dashboard_monthly_sales_value':  'mart_sales_dashboard_yearly_sales_value',
                'mart_sales_dashboard_monthly_payment_value':'mart_sales_dashboard_yearly_payment_value',
                'mart_sales_dashboard_monthly_gross_value':  'mart_sales_dashboard_yearly_gross_value',
                'mart_sales_dashboard_monthly_gross_sales':  'mart_sales_dashboard_yearly_gross_sales',
                'mart_sales_dashboard_monthly_product_qty':  'mart_sales_dashboard_yearly_product_qty',
                'mart_sales_dashboard_monthly_transaction':  'mart_sales_dashboard_yearly_transaction',
                'mart_sales_dashboard_monthly_refund':       'mart_sales_dashboard_yearly_refund',
                'mart_sales_dashboard_monthly_commission':   'mart_sales_dashboard_yearly_commission',
                'mart_sales_dashboard_monthly_year':         'mart_sales_dashboard_yearly_year',
            }
        )
        self.timelog['transform_mart_yearly' if logName == None else logName] = time.time() - _timestamp
        return df

    def write(self, df:pd.DataFrame, name:str = None):
        self.timelog['write_yearly' if name == None else name] = pds.write(df, self.targetConUri, table='mart_sales_dashboard_yearly', cols=[str(col) for col in df.columns])

    def delete(self, filters:list):
        _timestamp = time.time()
        sql = f"DELETE FROM  mart_prd.mart_sales_dashboard_yearly WHERE (m_user_id_user, mart_sales_dashboard_yearly_year, mart_sales_dashboard_yearly_order_type, m_cabang_id_cabang)  IN ({','.join(filters)})"
        executePostgres(sql, self.targetConfig)
        _runtime = time.time() - _timestamp

        if "delete_yearly" in self.timelog:
            self.timelog['delete_yearly'] += _runtime
        else:
            self.timelog['delete_yearly'] = _runtime

    def deleteTargeted(self, year:int, recoveryAt: str = None):
        _timestamp = time.time()
        sql = f"""DELETE FROM mart_prd.mart_sales_dashboard_yearly WHERE mart_sales_dashboard_yearly_year = {year} {recoveryAt if recoveryAt != None else ''}"""
        executePostgres(sql, self.targetConfig)
        self.timelog[f'delete_targeted_yearly_{year}'] = time.time() - _timestamp



    def transformTargeted(self, year:int, recoveryAt:str = None):
        _timestamp = time.time()

        sql_queries = []
        for month in range(1,13): # for month 1 till 12
            sql_queries.append(f"""SELECT m_user_id_user,
                        m_cabang_id_cabang,
                        mart_sales_dashboard_monthly_year,
                        mart_sales_dashboard_monthly_order_type,
                        mart_sales_dashboard_monthly_sales_value,
                        mart_sales_dashboard_monthly_payment_value,
                        mart_sales_dashboard_monthly_gross_value,
                        mart_sales_dashboard_monthly_gross_sales,
                        mart_sales_dashboard_monthly_product_qty,
                        mart_sales_dashboard_monthly_transaction,
                        mart_sales_dashboard_monthly_refund,
                        mart_sales_dashboard_monthly_commission
                FROM mart_prd.mart_sales_dashboard_monthly
                WHERE  mart_sales_dashboard_monthly_year = {year} AND mart_sales_dashboard_monthly_year = {month}  {recoveryAt if recoveryAt != None else ''}""")

        df = pds.fetch(self.targetConUri, sql_queries)
        self.timelog[f'extract_mart_yearly_{year}'] = time.time() - _timestamp

        return self._transform(df, logName = f'transform_mart_yearly_{year}')
