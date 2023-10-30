from .raw import ETL
from Utility.Pandas.PandasService import PandasService  as pds
import pandas as pd, time
from Utility.sql import *


class Daily(ETL):
    def proceed(self, filters : list):
        if len(filters) == 0:
            return
        self.write(self._transform(self._extract(filters)))


    def _extract(self, filters : list):
        _timestamp = time.time()

        df = pds.fetch(self.targetConUri, [
            f""" SELECT m_user_id_user,
                        m_cabang_id_cabang,
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
                WHERE (m_user_id_user, raw_mart_sales_dashboard_date, raw_mart_sales_dashboard_order_type, m_cabang_id_cabang)
                IN ({','.join(filter)})
            """ for filter in filters
        ])
        self.timelog['extract_mart_daily'] = time.time() - _timestamp
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
                'raw_mart_sales_dashboard_date'
            ])

        df_refund = pds.aggregate(df_refund, {
                'raw_mart_sales_dashboard_refund': 'sum',
                'raw_mart_sales_dashboard_commission': 'sum'
            },[
                'm_user_id_user',
                'm_cabang_id_cabang',
                'raw_mart_sales_dashboard_order_type',
                'raw_mart_sales_dashboard_date'
            ],
            onlyProcess=[
                'm_user_id_user',
                'm_cabang_id_cabang',
                'raw_mart_sales_dashboard_order_type',
                'raw_mart_sales_dashboard_date',
                'raw_mart_sales_dashboard_refund',
                'raw_mart_sales_dashboard_commission'
            ],
            renameCols={
                'm_user_id_user': 'user',
                'm_cabang_id_cabang': 'cabang',
                'raw_mart_sales_dashboard_order_type': 'order_type',
                'raw_mart_sales_dashboard_date': 'date',
                'raw_mart_sales_dashboard_commission' :'commission'
            })

        df = pd.merge(df_nonrefund, df_refund, how='left',
                    left_on=['m_user_id_user',
                        'm_cabang_id_cabang',
                        'raw_mart_sales_dashboard_order_type',
                        'raw_mart_sales_dashboard_date' ],
                    right_on=['user',
                        'cabang',
                        'order_type',
                        'date']).fillna(0)

        df['mart_sales_dashboard_daily_gross_value'] = df['raw_mart_sales_dashboard_sales_value'] \
                                                         - df['raw_mart_sales_dashboard_hpp_product'] \
                                                         - df['raw_mart_sales_dashboard_hpp_addon'] \
                                                         - df['raw_mart_sales_dashboard_refund'] \
                                                         - df['raw_mart_sales_dashboard_mdr'] \
                                                         - (
                                                            df['raw_mart_sales_dashboard_commission'] - df['commission']
                                                         )
        df['mart_sales_dashboard_daily_commission']  =  df['raw_mart_sales_dashboard_commission'] - df['commission']

        # if refund only in the hour
        df_2 = pd.merge(df_refund, df_nonrefund, how='left',
                    right_on=['m_user_id_user',
                        'm_cabang_id_cabang',
                        'raw_mart_sales_dashboard_order_type',
                        'raw_mart_sales_dashboard_date' ],
                    left_on=['user',
                        'cabang',
                        'order_type',
                        'date']).fillna(0)
        df_2 = df_2.loc[df_2['m_user_id_user'] == 0] # get which day is only exist a refund only


        df_2['mart_sales_dashboard_daily_gross_value'] = (-1 * df_2['raw_mart_sales_dashboard_refund']) + df_2['commission']
        df_2['mart_sales_dashboard_daily_commission'] = -1 * df_2['commission']
        df_2['raw_mart_sales_dashboard_sales_value'] = 0
        df_2['raw_mart_sales_dashboard_payment_value'] = 0
        df_2['raw_mart_sales_dashboard_gross_sales'] = 0
        df_2['raw_mart_sales_dashboard_product'] = 0
        df_2['raw_mart_sales_dashboard_id_transaction'] = 0

        df = pds.renameIt(df[[
            'm_user_id_user',
            'm_cabang_id_cabang',
            'raw_mart_sales_dashboard_order_type',
            'raw_mart_sales_dashboard_date',
            'raw_mart_sales_dashboard_sales_value',
            'raw_mart_sales_dashboard_payment_value',
            'raw_mart_sales_dashboard_gross_sales',
            'mart_sales_dashboard_daily_gross_value',
            'raw_mart_sales_dashboard_product',
            'raw_mart_sales_dashboard_id_transaction',
            'raw_mart_sales_dashboard_refund',
            'mart_sales_dashboard_daily_commission',
        ]],{
            'raw_mart_sales_dashboard_order_type'      : 'mart_sales_dashboard_daily_order_type',
            'raw_mart_sales_dashboard_date'            : 'mart_sales_dashboard_daily_date',
            'raw_mart_sales_dashboard_sales_value'     : 'mart_sales_dashboard_daily_sales_value',
            'raw_mart_sales_dashboard_payment_value'   : 'mart_sales_dashboard_daily_payment_value',
            'raw_mart_sales_dashboard_gross_sales'     : 'mart_sales_dashboard_daily_gross_sales',
            'raw_mart_sales_dashboard_product'         : 'mart_sales_dashboard_daily_product_qty',
            'raw_mart_sales_dashboard_id_transaction'  : 'mart_sales_dashboard_daily_transaction',
            'raw_mart_sales_dashboard_refund'          : 'mart_sales_dashboard_daily_refund',
        })


        df_2 = pds.renameIt(df_2[[
            'user',
            'cabang',
            'order_type',
            'date',
            'raw_mart_sales_dashboard_sales_value',
            'raw_mart_sales_dashboard_payment_value',
            'mart_sales_dashboard_daily_gross_value',
            'raw_mart_sales_dashboard_gross_sales',
            'raw_mart_sales_dashboard_product',
            'raw_mart_sales_dashboard_id_transaction',
            'raw_mart_sales_dashboard_refund',
            'mart_sales_dashboard_daily_commission',
        ]],{
            'user'                                     : 'm_user_id_user',
            'cabang'                                   : 'm_cabang_id_cabang',
            'order_type'                               : 'mart_sales_dashboard_daily_order_type',
            'date'                                     : 'mart_sales_dashboard_daily_date',
            'raw_mart_sales_dashboard_sales_value'     : 'mart_sales_dashboard_daily_sales_value',
            'raw_mart_sales_dashboard_payment_value'   : 'mart_sales_dashboard_daily_payment_value',
            'raw_mart_sales_dashboard_gross_sales'     : 'mart_sales_dashboard_daily_gross_sales',
            'raw_mart_sales_dashboard_product'         : 'mart_sales_dashboard_daily_product_qty',
            'raw_mart_sales_dashboard_id_transaction'  : 'mart_sales_dashboard_daily_transaction',
            'raw_mart_sales_dashboard_refund'          : 'mart_sales_dashboard_daily_refund',
        })
        df = pd.concat([df, df_2], ignore_index=True)

        self.timelog['transform_mart_daily'] = time.time() - _timestamp
        return df

    def write(self, df:pd.DataFrame):
        self.timelog['write_daily'] = pds.write(df, self.targetConUri, table='mart_sales_dashboard_daily', cols=[str(col) for col in df.columns])

    def delete(self, filters:list):
        _timestamp = time.time()
        sql = f"DELETE FROM mart_prd.mart_sales_dashboard_daily WHERE (m_user_id_user, mart_sales_dashboard_daily_date, mart_sales_dashboard_daily_order_type, m_cabang_id_cabang) IN ({','.join(filters)})"
        executePostgres(sql, self.targetConfig)
        _runtime = time.time() - _timestamp

        if "delete_daily" in self.timelog:
            self.timelog['delete_daily'] += _runtime
        else:
            self.timelog['delete_daily'] = _runtime
