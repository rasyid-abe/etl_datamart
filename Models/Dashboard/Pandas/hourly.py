from .raw import ETL
from Utility.Pandas.PandasService import PandasService  as pds
import pandas as pd, time
from Utility.sql import *


class Hourly(ETL):
    def proceed(self, filters : list):
        if len(filters) == 0:
            return
        self.write(self._transform(self._extract(filters)))


    def _extract(self, filters : list):
        _timestamp = time.time()

        df = pds.fetch(self.targetConUri, [
            f""" SELECT m_user_id_user,
                        m_cabang_id_cabang,
                        date_part('hour',raw_mart_sales_dashboard_datetime) as hourly,
                        raw_mart_sales_dashboard_date,
                        raw_mart_sales_dashboard_order_type,
                        raw_mart_sales_dashboard_is_refund,
                        raw_mart_sales_dashboard_refund,
                        raw_mart_sales_dashboard_commission,
                        raw_mart_sales_dashboard_mdr,
                        raw_mart_sales_dashboard_sales_value,
                        raw_mart_sales_dashboard_payment_value,
                        raw_mart_sales_dashboard_gross_sales,
                        raw_mart_sales_dashboard_hpp_product,
                        raw_mart_sales_dashboard_hpp_addon,
                        raw_mart_sales_dashboard_product,
                        raw_mart_sales_dashboard_id_transaction
                FROM mart_prd.raw_mart_sales_dashboard
                WHERE (m_user_id_user, raw_mart_sales_dashboard_date,  date_part('hour',raw_mart_sales_dashboard_datetime), raw_mart_sales_dashboard_order_type, m_cabang_id_cabang)
                IN ({','.join(filter)})
            """ for filter in filters
        ])
        self.timelog['extract_mart_hourly'] = time.time() - _timestamp
        return df

    def _transform(self, df:pd.DataFrame):
        _timestamp = time.time()
        df_nonrefund = df.loc[df['raw_mart_sales_dashboard_is_refund'] == False]
        df_refund    = df.loc[df['raw_mart_sales_dashboard_is_refund'] == True]

        df_nonrefund = pds.aggregate(df_nonrefund, {
                'raw_mart_sales_dashboard_sales_value':'sum',
                'raw_mart_sales_dashboard_payment_value':'sum',
                'raw_mart_sales_dashboard_gross_sales':'sum',
                'raw_mart_sales_dashboard_id_transaction': 'count',
                'raw_mart_sales_dashboard_product': 'sum',
                'raw_mart_sales_dashboard_commission': 'sum',
                'raw_mart_sales_dashboard_mdr': 'sum',
                'raw_mart_sales_dashboard_hpp_product': 'sum',
                'raw_mart_sales_dashboard_hpp_addon': 'sum'
            }, [
                'm_user_id_user',
                'm_cabang_id_cabang',
                'raw_mart_sales_dashboard_order_type',
                'raw_mart_sales_dashboard_date',
                'hourly'
            ])

        df_refund = pds.aggregate(df_refund, {
                'raw_mart_sales_dashboard_refund': 'sum',
                'raw_mart_sales_dashboard_commission': 'sum'
            },[
                'm_user_id_user',
                'm_cabang_id_cabang',
                'raw_mart_sales_dashboard_order_type',
                'raw_mart_sales_dashboard_date',
                'hourly'
            ],
            onlyProcess=[
                'm_user_id_user',
                'm_cabang_id_cabang',
                'raw_mart_sales_dashboard_order_type',
                'raw_mart_sales_dashboard_date',
                'raw_mart_sales_dashboard_refund',
                'raw_mart_sales_dashboard_commission',
                'hourly'
            ],
            renameCols={
                'm_user_id_user': 'user',
                'm_cabang_id_cabang': 'cabang',
                'raw_mart_sales_dashboard_order_type': 'order_type',
                'raw_mart_sales_dashboard_date': 'date',
                'raw_mart_sales_dashboard_commission' :'commission',
                'hourly': 'hour'
            })



        df = pd.merge(df_nonrefund, df_refund, how='left',
                    left_on=['m_user_id_user',
                        'm_cabang_id_cabang',
                        'raw_mart_sales_dashboard_order_type',
                        'raw_mart_sales_dashboard_date',
                        'hourly' ],
                    right_on=['user',
                        'cabang',
                        'order_type',
                        'date',
                        'hour']).fillna(0) # left

        # non refund trx
        df['mart_sales_dashboard_hourly_gross_value'] = df['raw_mart_sales_dashboard_sales_value'] \
                                                         - df['raw_mart_sales_dashboard_hpp_product'] \
                                                         - df['raw_mart_sales_dashboard_hpp_addon'] \
                                                         - df['raw_mart_sales_dashboard_refund'] \
                                                         - df['raw_mart_sales_dashboard_mdr'] \
                                                         - (
                                                            df['raw_mart_sales_dashboard_commission'] - df['commission']
                                                         )
        df['mart_sales_dashboard_hourly_commission']  =  df['raw_mart_sales_dashboard_commission'] - df['commission']


        # if refund only in the hour
        df_2 = pd.merge(df_refund, df_nonrefund, how='left',
                    right_on=['m_user_id_user',
                        'm_cabang_id_cabang',
                        'raw_mart_sales_dashboard_order_type',
                        'raw_mart_sales_dashboard_date',
                        'hourly' ],
                    left_on=['user',
                        'cabang',
                        'order_type',
                        'date',
                        'hour']).fillna(0)
        df_2 = df_2.loc[df_2['m_user_id_user'] == 0] # get which hour is only exist a refund only



        df_2['mart_sales_dashboard_hourly_gross_value'] = (-1 * df_2['raw_mart_sales_dashboard_refund']) + df_2['commission']
        df_2['mart_sales_dashboard_hourly_commission'] = -1 * df_2['commission']
        df_2['raw_mart_sales_dashboard_sales_value'] = 0
        df_2['raw_mart_sales_dashboard_payment_value'] = 0
        df_2['raw_mart_sales_dashboard_gross_sales'] = 0
        df_2['raw_mart_sales_dashboard_product'] = 0
        df_2['raw_mart_sales_dashboard_id_transaction'] = 0

        df = pds.renameIt(df[[
            'm_user_id_user',
            'm_cabang_id_cabang',
            'raw_mart_sales_dashboard_order_type',
            'hourly',
            'raw_mart_sales_dashboard_date',
            'raw_mart_sales_dashboard_sales_value',
            'raw_mart_sales_dashboard_payment_value',
            'mart_sales_dashboard_hourly_gross_value',
            'raw_mart_sales_dashboard_gross_sales',
            'raw_mart_sales_dashboard_product',
            'raw_mart_sales_dashboard_id_transaction',
            'raw_mart_sales_dashboard_refund',
            'mart_sales_dashboard_hourly_commission',
        ]],{
            'raw_mart_sales_dashboard_order_type'      : 'mart_sales_dashboard_hourly_order_type',
            'raw_mart_sales_dashboard_date'            : 'mart_sales_dashboard_hourly_date',
            'raw_mart_sales_dashboard_sales_value'     : 'mart_sales_dashboard_hourly_sales_value',
            'raw_mart_sales_dashboard_payment_value'   : 'mart_sales_dashboard_hourly_payment_value',
            'raw_mart_sales_dashboard_gross_sales'     : 'mart_sales_dashboard_hourly_gross_sales',
            'raw_mart_sales_dashboard_product'         : 'mart_sales_dashboard_hourly_product_qty',
            'raw_mart_sales_dashboard_id_transaction'  : 'mart_sales_dashboard_hourly_transaction',
            'raw_mart_sales_dashboard_refund'          : 'mart_sales_dashboard_hourly_refund',
            'hourly'                                   : 'mart_sales_dashboard_hourly_hour',
        })


        df_2 = pds.renameIt(df_2[[
            'user',
            'cabang',
            'order_type',
            'date',
            'hour',
            'raw_mart_sales_dashboard_sales_value',
            'raw_mart_sales_dashboard_payment_value',
            'mart_sales_dashboard_hourly_gross_value',
            'raw_mart_sales_dashboard_gross_sales',
            'raw_mart_sales_dashboard_product',
            'raw_mart_sales_dashboard_id_transaction',
            'raw_mart_sales_dashboard_refund',
            'mart_sales_dashboard_hourly_commission',
        ]],{
            'user'                                      : 'm_user_id_user',
            'cabang'                                    : 'm_cabang_id_cabang',
            'order_type'                                : 'mart_sales_dashboard_hourly_order_type',
            'date'                                      : 'mart_sales_dashboard_hourly_date',
            'raw_mart_sales_dashboard_sales_value'      : 'mart_sales_dashboard_hourly_sales_value',
            'raw_mart_sales_dashboard_payment_value'    : 'mart_sales_dashboard_hourly_payment_value',
            'raw_mart_sales_dashboard_gross_sales'      : 'mart_sales_dashboard_hourly_gross_sales',
            'raw_mart_sales_dashboard_product'          : 'mart_sales_dashboard_hourly_product_qty',
            'raw_mart_sales_dashboard_id_transaction'   : 'mart_sales_dashboard_hourly_transaction',
            'raw_mart_sales_dashboard_refund'           : 'mart_sales_dashboard_hourly_refund',
            'hour'                                      : 'mart_sales_dashboard_hourly_hour',
        })
        df = pd.concat([df, df_2], ignore_index=True)

        df["mart_sales_dashboard_hourly_hour"] = df["mart_sales_dashboard_hourly_hour"].astype(int)
        self.timelog['transform_mart_hourly'] = time.time() - _timestamp
        return df

    def write(self, df:pd.DataFrame):
        self.timelog['write_hourly'] = pds.write(df, self.targetConUri, table='mart_sales_dashboard_hourly', cols=[str(col) for col in df.columns])

    def delete(self, filters:list):
        _timestamp = time.time()
        sql = f"DELETE FROM mart_prd.mart_sales_dashboard_hourly WHERE (m_user_id_user, mart_sales_dashboard_hourly_date, mart_sales_dashboard_hourly_hour, mart_sales_dashboard_hourly_order_type, m_cabang_id_cabang) IN ({','.join(filters)})"
        executePostgres(sql, self.targetConfig)
        _runtime = time.time() - _timestamp

        if "delete_hourly" in self.timelog:
            self.timelog['delete_hourly'] += _runtime
        else:
            self.timelog['delete_hourly'] = _runtime
