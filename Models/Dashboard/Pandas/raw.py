from Utility.Pandas.PandasService import PandasService  as pds
import pandas as pd, time
import pandas as pd, pytz, pyarrow as pa
from datetime import datetime
from Utility.sql import *


class ETL():

    def __init__(self, sourceConfig, targetConfig, size_insert, size_delete):
        self.numrow = 0
        self.timelog = {}
        self.minUpdate = None
        self.maxUpdate = None
        self.sourceConfig = sourceConfig
        self.targetConfig = targetConfig
        self.sizeInsert   = size_insert
        self.sizeDelete   = size_delete

        self.sourceConUri = pds.getConnectionUri(sourceConfig['USER'], sourceConfig['PASS'],sourceConfig['HOST'],sourceConfig['PORT'],sourceConfig['DBNAME'], "mysql")
        self.targetConUri = pds.getConnectionUri(targetConfig['USER'], targetConfig['PASS'],targetConfig['HOST'],targetConfig['PORT'],targetConfig['DBNAME'], "postgresql")



class Raw(ETL):

    def proceed(self, start:datetime, end:datetime, action:str = 'ETL', user:str = None, cabang: int = None):
        self.minUpdate = start
        if(end > datetime.today().astimezone(tz=pytz.timezone('Asia/Jakarta'))):
            self.maxUpdate = start
        else:
            self.maxUpdate = end

        recoveryAt = None
        if user != None:
            recoveryAt = f' AND t.M_User_id_user IN ({user})' if  type(user) is str and len(user.split(',')) > 1 else f' AND t.M_User_id_user = {user}'

            if cabang != None and( type(cabang) == int or (type(cabang) == str and len(cabang.split(',')) == 1)):
                recoveryAt = f' {recoveryAt} AND t.M_Cabang_id_cabang = {cabang}'

        # fetch data
        df_trx, df_product, df_mdr, df_addon, df_commission, df_child = self._extract(start, end, 't.updatedate' if action == 'ETL' else 't.transaction_tgl', recoveryAt=recoveryAt)

        self.numrow = df_trx.shape[0]

        tobeDeleted         = []
        martHourly          = []
        martDeletedHourly   = []
        martDaily           = []
        martDeletedDaily    = []
        martMonthly         = []
        martDeletedMonthly  = []
        martYearly          = []
        martDeletedYearly   = []

        if self.numrow > 0 :

            if action == "ETL":
                _timestamp = time.time()
                self.minUpdate = df_trx['updatedate'].min()
                self.maxUpdate = df_trx['updatedate'].max()
                self.timelog['get_updatedate'] = time.time() - _timestamp

            # get filters
            tobeDeleted = df_trx['id_transaction'].astype(str).values.tolist()
            martHourly   = df_trx.loc[df_trx['status'] == '1', ['M_User_id_user', 'tgl' , 'hourly','order_type','M_Cabang_id_cabang']]\
                                .apply(lambda row: f"""( {row['M_User_id_user']}, '{row['tgl']}', {row['hourly']}, {row['order_type']}, {row['M_Cabang_id_cabang']} )""", axis = 1)\
                                .drop_duplicates().values.tolist()

            martDeletedHourly  = df_trx.loc[df_trx['status'] != '1', ['M_User_id_user', 'tgl' , 'hourly','order_type','M_Cabang_id_cabang']]\
                                .apply(lambda row: f"""( {row['M_User_id_user']}, '{row['tgl']}', {row['hourly']}, {row['order_type']}, {row['M_Cabang_id_cabang']} )""", axis = 1)\
                                .drop_duplicates().values.tolist()

            martDaily   = df_trx.loc[df_trx['status'] == '1', ['M_User_id_user', 'tgl','order_type','M_Cabang_id_cabang']]\
                                .apply(lambda row: f"""( {row['M_User_id_user']}, '{row['tgl']}', {row['order_type']}, {row['M_Cabang_id_cabang']} )""", axis = 1)\
                                .drop_duplicates().values.tolist()

            martDeletedDaily =  df_trx.loc[df_trx['status'] != '1', ['M_User_id_user', 'tgl','order_type','M_Cabang_id_cabang']]\
                                .apply(lambda row: f"""( {row['M_User_id_user']}, '{row['tgl']}', {row['order_type']}, {row['M_Cabang_id_cabang']} )""", axis = 1)\
                                .drop_duplicates().values.tolist()


            martMonthly = (df_trx.loc[ (df_trx['status'] == '1') & ( (df_trx['tahun'] < start.year) | ((df_trx['tahun'] == start.year) & (df_trx['bulan'] < start.month))),
                                        ['M_User_id_user', 'tahun', 'bulan','order_type','M_Cabang_id_cabang']] \
                            if action == "ETL" \
                                else df_trx.loc[df_trx['status'] == '1',  ['M_User_id_user', 'tahun', 'bulan','order_type','M_Cabang_id_cabang']])\
                                    .apply(lambda row: f"""( {row['M_User_id_user']}, {row['tahun']}, {row['bulan']}, {row['order_type']}, {row['M_Cabang_id_cabang']} )""" , axis= 1)\
                                    .drop_duplicates().values.tolist()

            martDeletedMonthly = (df_trx.loc[ (df_trx['status'] != '1') & ( (df_trx['tahun'] < start.year) | ((df_trx['tahun'] == start.year) & (df_trx['bulan'] < start.month))),
                                        ['M_User_id_user', 'tahun', 'bulan','order_type','M_Cabang_id_cabang']] \
                            if action == "ETL" \
                                else df_trx.loc[df_trx['status'] != '1',  ['M_User_id_user', 'tahun', 'bulan','order_type','M_Cabang_id_cabang']])\
                                    .apply(lambda row: f"""( {row['M_User_id_user']}, {row['tahun']}, {row['bulan']}, {row['order_type']}, {row['M_Cabang_id_cabang']} )""" , axis= 1)\
                                    .drop_duplicates().values.tolist()

            martYearly  = (df_trx.loc[ (df_trx['status'] == '1') & (df_trx['tahun'] < start.year) , ['M_User_id_user', 'tahun','order_type','M_Cabang_id_cabang']] \
                            if action == "ETL"\
                                  else df_trx.loc[df_trx['status'] == '1', ['M_User_id_user', 'tahun','order_type','M_Cabang_id_cabang']])\
                            .apply(lambda row: f"""( {row['M_User_id_user']}, {row['tahun']}, {row['order_type']}, {row['M_Cabang_id_cabang']} )""" , axis= 1)\
                            .drop_duplicates().values.tolist()

            martDeletedYearly  = (df_trx.loc[ (df_trx['status'] != '1') & (df_trx['tahun'] < start.year) , ['M_User_id_user', 'tahun','order_type','M_Cabang_id_cabang']] \
                            if action == "ETL"\
                                  else df_trx.loc[df_trx['status'] != '1', ['M_User_id_user', 'tahun','order_type','M_Cabang_id_cabang']])\
                            .apply(lambda row: f"""( {row['M_User_id_user']}, {row['tahun']}, {row['order_type']}, {row['M_Cabang_id_cabang']} )""" , axis= 1)\
                            .drop_duplicates().values.tolist()

        # filter status only with 1
        df_filtered = df_trx.loc[df_trx['status'] == '1']
        del df_trx
        return (
            df_filtered,
            df_product,
            df_mdr,
            df_addon,
            df_commission,
            df_child,
            pds.chunkIt(martHourly, self.sizeInsert),
            pds.chunkIt(martDaily, self.sizeInsert),
            pds.chunkIt(martMonthly, self.sizeInsert),
            pds.chunkIt(martYearly, self.sizeInsert),
            pds.chunkIt(martHourly+martDeletedHourly, self.sizeDelete),
            pds.chunkIt(martDaily+martDeletedDaily, self.sizeDelete),
            pds.chunkIt(martMonthly+martDeletedMonthly, self.sizeDelete),
            pds.chunkIt(martYearly+martDeletedYearly, self.sizeDelete),
            pds.chunkIt(tobeDeleted, self.sizeDelete)
        )

    def _extract(self, start:datetime, end:datetime, incrementalColumn:str, recoveryAt:str = None):
         # fetch t.status IN 1,4,9 to clean raw which is canceled (4 or 9)
        query = f"""SELECT
                        t.id_transaction,
                        t.M_User_id_user,
                        t.M_Cabang_id_cabang,
                        t.transaction_tgl,
                        HOUR(t.transaction_tgl) hourly,
                        (DATE(t.transaction_tgl)) tgl,
                        MONTH(t.transaction_tgl) bulan,
                        YEAR(t.transaction_tgl) tahun,
                        t.transaction_total,
                        t.transaction_refund,
                        IFNULL(t.transaction_type, 99) as order_type,
                        t.updatedate,
                        IF(t.transaction_refund > 0 OR COALESCE(t.transaction_refund_reason, '') != '', 1, 0) is_refund,
                        t.status
                    FROM Transactions t
                    WHERE
                        t.Transaction_purpose IN ('5', '9')
                        AND t.status IN ('1', '4', '9')
                        AND {incrementalColumn} BETWEEN '{start}' AND '{end}'
                        AND t.transaction_tgl > '1970-01-01 00:00:00'
                        AND (
                            (t.transaction_pisah = 1 AND t.transaction_no_payment <= 1)
                            OR t.transaction_pisah != 1  OR t.transaction_pisah IS NULL
                        )
                        {recoveryAt if recoveryAt != None else ''}"""
        _timestamp = time.time()
        df_trx = pds.fetch(self.sourceConUri, query)
        df_trx['tgl'] = pds.customDate(df_trx['tgl'], '%Y-%m-%d')
        self.timelog['extract_trx'] = time.time() - _timestamp


        # fetch only status 1
        query = f"""SELECT
                        id_transaction,
                        SUM(td.transaction_detail_jumlah) as product_trx,
                        SUM(td.transaction_detail_jumlah * td.transaction_detail_price_modal) AS hpp_produk,
                        SUM(td.transaction_detail_total_price) as penjualan_kotor
                    FROM Transactions t JOIN Transaction_Detail td ON t.id_transaction = td.Transactions_id_transaction
                    WHERE t.Transaction_purpose IN ('5', '9')
                        AND t.status = '1'
                        AND {incrementalColumn} BETWEEN '{start}' AND '{end}'
                        AND t.transaction_tgl > '1970-01-01 00:00:00'
                        AND (
                            (t.transaction_pisah = 1 AND t.transaction_no_payment <= 1)
                            OR t.transaction_pisah != 1  OR t.transaction_pisah IS NULL
                        )
                        {recoveryAt if recoveryAt != None else ''}
                    GROUP BY t.id_transaction"""
        _timestamp = time.time()
        df_product = pds.fetch(self.sourceConUri, query)
        self.timelog['extract_product'] = time.time() - _timestamp



        query = f"""SELECT
                        id_transaction,
                        SUM(tad.transaction_addon_detail_quantity *  transaction_addon_detail_quantity_bahan * tad.transaction_addon_detail_harga_modal) AS hpp_addon
                    FROM Transactions t
                        JOIN Transaction_Detail td ON t.id_transaction = td.Transactions_id_transaction
                        JOIN Transaction_Addon_Detail tad ON tad.Transaction_Detail_id_transaction_detail = td.id_transaction_detail
                    WHERE t.Transaction_purpose IN ('5', '9')
                        AND t.status = '1'
                        AND {incrementalColumn} BETWEEN '{start}' AND '{end}'
                        AND t.transaction_tgl > '1970-01-01 00:00:00'
                        AND (
                            (t.transaction_pisah = 1 AND t.transaction_no_payment <= 1)
                            OR t.transaction_pisah != 1 OR t.transaction_pisah IS NULL
                        )
                        {recoveryAt if recoveryAt != None else ''}
                    GROUP BY t.id_transaction"""
        _timestamp = time.time()
        df_addondetail = pds.fetch(self.sourceConUri, query)
        self.timelog['extract_addon'] = time.time() - _timestamp


        query = f"""SELECT
                        id_transaction,
                        SUM(IFNULL(tc.commission_total_nominal,0)) as komisi
                    FROM Transactions t
                        JOIN Transaction_Commission tc ON t.id_transaction = tc.Transactions_id_transaction
                    WHERE t.Transaction_purpose IN ('5', '9')
                        AND t.status = '1'
                        AND {incrementalColumn} BETWEEN '{start}' AND '{end}'
                        AND t.transaction_tgl > '1970-01-01 00:00:00'
                        AND (
                            (t.transaction_pisah = 1 AND t.transaction_no_payment <= 1)
                            OR t.transaction_pisah != 1  OR t.transaction_pisah IS NULL
                        ) AND t.transaction_tgl_payment IS NOT NULL
                        {recoveryAt if recoveryAt != None else ''}
                    GROUP BY t.id_transaction"""
        _timestamp = time.time()
        df_commission = pds.fetch(self.sourceConUri, query)
        self.timelog['extract_commission'] = time.time() - _timestamp


        query = f"""SELECT
                        id_transaction,
                        SUM(IFNULL(thpm.transaction_has_payment_method_MDR,0)) AS mdr,
                        CASE
                             WHEN thpm.Payment_method_id_payment_method = 1 THEN
                                 SUM(thpm.transaction_has_payment_method_value - t.transaction_kembalian)
                             ELSE
                                 SUM(thpm.transaction_has_payment_method_value)
                        END AS payment_value
                    FROM Transactions t
                        JOIN Transaction_has_Payment_Method thpm ON t.id_transaction = thpm.Transaction_id_transaction
                    WHERE t.Transaction_purpose IN ('5', '9')
                        AND t.status = '1'
                        AND {incrementalColumn} BETWEEN '{start}' AND '{end}'
                        AND t.transaction_tgl > '1970-01-01 00:00:00'
                        AND (
                            (t.transaction_pisah = 1 AND t.transaction_no_payment <= 1)
                            OR t.transaction_pisah != 1  OR t.transaction_pisah IS NULL
                        )
                        {recoveryAt if recoveryAt != None else ''}
                    GROUP BY t.id_transaction"""
        _timestamp = time.time()
        df_mdr = pds.fetch(self.sourceConUri, query)
        self.timelog['extract_mdr'] = time.time() - _timestamp


        query = f"""SELECT
                        childtrx.id_transaction,
                        total,
                        IFNULL(mdr, 0) as mdr,
                        IFNULL(payment_value, 0) as payment_value
                    FROM (
                        SELECT
                            t.id_transaction as id_transaction,
                            SUM(child.transaction_total) as total
                        FROM Transactions child
                        JOIN Transactions t ON child.M_Cabang_id_cabang = t.M_Cabang_id_cabang
                                AND child.transaction_no_nota_parent = t.transaction_no_nota
                                AND child.M_User_id_user = t.M_User_id_user
                                AND t.Transaction_purpose IN ('5', '9')
                                AND t.status = '1'
                                AND {incrementalColumn} BETWEEN '{start}' AND '{end}'
                                AND t.transaction_pisah = 1 AND t.transaction_no_payment <= 1
                                 {recoveryAt if recoveryAt != None else ''}
                        GROUP BY t.id_transaction
                    ) childtrx LEFT JOIN (
                        SELECT
                            t.id_transaction as id_transaction,
                            SUM(IFNULL(thpm.transaction_has_payment_method_MDR,0)) AS mdr,
                            CASE
                                 WHEN thpm.Payment_method_id_payment_method = 1 THEN
                                     SUM(thpm.transaction_has_payment_method_value - t.transaction_kembalian)
                                 ELSE
                                     SUM(thpm.transaction_has_payment_method_value)
                            END AS payment_value
                        FROM Transactions child
                        JOIN Transactions t ON child.M_Cabang_id_cabang = t.M_Cabang_id_cabang
                                AND child.transaction_no_nota_parent = t.transaction_no_nota
                                AND child.M_User_id_user = t.M_User_id_user
                                AND t.Transaction_purpose IN ('5', '9')
                                AND t.status = '1'
                                AND {incrementalColumn} BETWEEN '{start}' AND '{end}'
                                AND t.transaction_pisah = 1 AND t.transaction_no_payment <= 1
                                 {recoveryAt if recoveryAt != None else ''}
                        JOIN Transaction_has_Payment_Method thpm ON child.id_transaction = thpm.Transaction_id_transaction
                        GROUP BY t.id_transaction
                    ) childmdr ON childtrx.id_transaction = childmdr.id_transaction"""
        _timestamp = time.time()
        df_child = pds.fetch(self.sourceConUri, query)
        self.timelog['extract_child'] = time.time() - _timestamp

        return (
            df_trx,
            df_product,
            df_mdr,
            df_addondetail,
            df_commission,
            df_child
        )

    def transform(self, trx:pd.DataFrame, product:pd.DataFrame, mdr:pd.DataFrame, addon:pd.DataFrame, commission:pd.DataFrame, child:pd.DataFrame):
        _timestamp = time.time()

        trx['is_refund'] = trx['is_refund'].astype(bool)
        trx = trx[[
            'id_transaction' ,
            'M_User_id_user' ,
            'M_Cabang_id_cabang',
            'transaction_tgl',
            'tgl'            ,
            'transaction_total' ,
            'transaction_refund',
            'order_type'     ,
            'is_refund'
        ]]

        trx.rename(columns={
            'id_transaction'     : 'raw_mart_sales_dashboard_id_transaction',
            'M_User_id_user'     : 'm_user_id_user',
            'M_Cabang_id_cabang' : 'm_cabang_id_cabang',
            'transaction_tgl'    : 'raw_mart_sales_dashboard_datetime',
            'tgl'                : 'raw_mart_sales_dashboard_date',
            'transaction_total'  : 'raw_mart_sales_dashboard_sales_value',
            'transaction_refund' : 'raw_mart_sales_dashboard_refund',
            'order_type'         : 'raw_mart_sales_dashboard_order_type',
            'is_refund'          : 'raw_mart_sales_dashboard_is_refund'
        }, inplace=True)

        df = pd.merge(trx, mdr, how='left', left_on='raw_mart_sales_dashboard_id_transaction', right_on='id_transaction')
        df = pd.merge(df, product, how='left', left_on='raw_mart_sales_dashboard_id_transaction', right_on='id_transaction').fillna(0)
        df = pd.merge(df, addon, how='left', left_on='raw_mart_sales_dashboard_id_transaction', right_on='id_transaction').fillna(0)
        df = pd.merge(df, commission, how='left', left_on='raw_mart_sales_dashboard_id_transaction', right_on='id_transaction').fillna(0)

        df = pds.renameIt(
            df[[
                'raw_mart_sales_dashboard_id_transaction',
                'm_user_id_user',
                'm_cabang_id_cabang',
                'raw_mart_sales_dashboard_datetime',
                'raw_mart_sales_dashboard_date',
                'raw_mart_sales_dashboard_sales_value',
                'raw_mart_sales_dashboard_is_refund',
                'raw_mart_sales_dashboard_refund',
                'raw_mart_sales_dashboard_order_type',
                'product_trx',
                'hpp_produk',
                'penjualan_kotor',
                'mdr',
                'payment_value',
                'hpp_addon',
                'komisi'
            ]],
            {
                'product_trx'       : 'raw_mart_sales_dashboard_product',
                'hpp_produk'        : 'raw_mart_sales_dashboard_hpp_product',
                'penjualan_kotor'   : 'raw_mart_sales_dashboard_gross_sales',
                'mdr'               : 'raw_mart_sales_dashboard_mdr',
                'payment_value'     : 'raw_mart_sales_dashboard_payment_value',
                'hpp_addon'         : 'raw_mart_sales_dashboard_hpp_addon',
                'komisi'            : 'raw_mart_sales_dashboard_commission'
            }
        )
        df['raw_mart_sales_dashboard_datetime'] =  df['raw_mart_sales_dashboard_datetime'].astype(str) + '+07:00'

        if child.shape[0] > 0:
            df = pd.merge(df, child, how='left', left_on='raw_mart_sales_dashboard_id_transaction', right_on='id_transaction').fillna(0)
            df['raw_mart_sales_dashboard_mdr'] = df['raw_mart_sales_dashboard_mdr'] + df['mdr']
            df['raw_mart_sales_dashboard_sales_value'] = df['raw_mart_sales_dashboard_sales_value'] + df['total']
            df.drop(child.columns.tolist(),axis=1, inplace=True)


        self.timelog['transform_raw'] = time.time() - _timestamp
        return df

    def write(self, df:pd.DataFrame):
        self.timelog['write_raw'] = pds.write(df, self.targetConUri, table='raw_mart_sales_dashboard', cols=[str(col) for col in df.columns])

    def delete(self, filters:list):
        _timestamp = time.time()
        sql = f"DELETE FROM mart_prd.raw_mart_sales_dashboard WHERE raw_mart_sales_dashboard_id_transaction IN ({','.join(filters)})"
        executePostgres(sql, self.targetConfig)
        _runtime = time.time() - _timestamp

        if "delete_raw" in self.timelog:
            self.timelog['delete_raw'] += _runtime
        else:
            self.timelog['delete_raw'] = _runtime


    def fetchPreviousRawDaily(self, filters:list):
        dfDelPandas = pds.fetch(self.targetConUri, [
            f""" SELECT m_user_id_user,
                        raw_mart_sales_dashboard_date,
                        raw_mart_sales_dashboard_order_type,
                        m_cabang_id_cabang
                FROM mart_prd.raw_mart_sales_dashboard
                WHERE raw_mart_sales_dashboard_id_transaction
                IN ({','.join(filter)})
            """ for filter in filters
        ])

        if len(dfDelPandas) > 0:
            dfDelPandas['raw_mart_sales_dashboard_date'] = pds.customDate(dfDelPandas['raw_mart_sales_dashboard_date'], '%Y-%m-%d')

            dfDelPandas = list(dfDelPandas.itertuples(index=False, name=None))
            dfDelPandas = [f"""{x}""" for x in dfDelPandas]

            return [dfDelPandas]
        else:
            return []

    def fetchPreviousRawHourly(self, filters:list):
        dfDelPandas = pds.fetch(self.targetConUri, [
            f""" SELECT m_user_id_user,
                        raw_mart_sales_dashboard_date,
                        date_part('hour',raw_mart_sales_dashboard_datetime) as hourly,
                        raw_mart_sales_dashboard_order_type,
                        m_cabang_id_cabang
                FROM mart_prd.raw_mart_sales_dashboard
                WHERE raw_mart_sales_dashboard_id_transaction
                IN ({','.join(filter)})
            """ for filter in filters
        ])

        if len(dfDelPandas) > 0:
            dfDelPandas['raw_mart_sales_dashboard_date'] = pds.customDate(dfDelPandas['raw_mart_sales_dashboard_date'], '%Y-%m-%d')
            dfDelPandas['hourly'] = dfDelPandas['hourly'].astype(int)

            dfDelPandas = list(dfDelPandas.itertuples(index=False, name=None))
            dfDelPandas = [f"""{x}""" for x in dfDelPandas]

            return [dfDelPandas]
        else:
            return []
