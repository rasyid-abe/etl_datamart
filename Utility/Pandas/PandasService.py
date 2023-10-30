import connectorx as cx, pandas as pd, pytz, tempfile, pyarrow as pa, time, urllib.parse, uuid
from pyspark.sql.functions import *
from Utility.sql import *
from typing import List, Dict
from datetime import datetime, date, timedelta
from logger import Logger
from functools import partial, reduce
LOGGER = Logger()

class PandasService():

    @staticmethod
    def getConnectionUri(user:str, password:str, host:str, port:str, dbname:str, type:str ) -> str:
        if type == "mysql":
            return  f"mysql://{user}:{urllib.parse.quote(password)}@{host}:{port}/{dbname}"
        elif type == "postgresql":
            return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
        else:
            raise ValueError(f"{type} is not supported!")
    
    @staticmethod
    def chunkIt(_list:List, size) -> List[list]: 
        return [_list[i:i + size] for i in range(0, len(_list),size)]

    @staticmethod
    def renameIt(df:pd.DataFrame, renameCols:Dict[str, str]) -> pd.DataFrame:
        return df.rename(columns=renameCols)

    @staticmethod
    def write(df:pd.DataFrame, uri:str, table:str,  cols:List[str] = [], mechanism:str='copy', schema:str = 'mart_prd', lowerCols = True, raiseIfEmpty = False, **kwargs):
        """
        This function will write the dataframe into selected table. It return -1 => dataframe is empty, float => time needed to write, raise ValueError if mechanism is not supported yet. 
        Current mechanics supported is copy.

        Caution : No exception is handled (yet) for development & debug purposes. Use it on your own risk.

            df : pandas dataframe you want to insert
            uri : the connection uri for the targets
            table : the tablename you want the df inserted into
            via : what method you want to write your dataframe to database, default is through copy
            schema : the schemaname where the table is, default is mart_prd
            lowerCols : lower the column name, default is true
            cols : name of cols you want to insert
            raiseIfEmpty : wheter to raise exception if df is empty / None is passed, default is False. If set to true, will raise, else will return 0
        """
        if mechanism == 'copy':
            res = PandasService._writeWithCopy(df, uri, table, raiseIfEmpty, cols=cols, schema=schema, lowerCols=lowerCols, **kwargs)
            LOGGER.info(f"Finished writing into {schema}.{table} using {mechanism}, took {res} second(s)")
            return res
        else:
            raise ValueError(f"{mechanism} is not supported!")
            
    @staticmethod
    def _writeWithCopy(df, uri:str, table:str, raiseIfEmpty, cols:List[str] = [],  schema:str = 'mart_prd' , lowerCols = True, separator = ',') -> float:
        # if df == None:
        #     return ValueError("Dataframe is empty!") if raiseIfEmpty else 0
        
        if df is not None and df.empty:
            return ValueError("Dataframe is empty!") if raiseIfEmpty else 0
        elif df is None:
            return ValueError("Dataframe is empty!") if raiseIfEmpty else 0


        if len(cols) == 0:
            cols = [str(col) for col in df.columns]

        
        start = time.time()
        with psycopg2.connect(uri) as conn:
            with conn.cursor() as cur:
                with tempfile.TemporaryFile(mode='w+t',newline='') as f:
                    df.to_csv(f, header=False, index=False, sep = separator)
                    f.seek(0)
                    cur.execute(f'SET search_path TO {schema}')
                    cur.copy_from(f, table, sep= separator, columns=tuple([col.lower() if lowerCols else col for col in cols]))
                conn.commit()
        return time.time() - start

    @staticmethod
    def fetch(uri , queries, to = "pandas", maxParalelism = 25) -> pd.DataFrame:
        """
            This function will return a object with a given Query / Queries

            uri: connection uri
            queries : could be SQL query string or list of SQL query string
            to : target object type, current support is to pandas
        """
        if type(queries) == list and len(queries) == 0:
            raise ValueError("Empty Queries ! Make sure you have at least 1 query in a list!")

        try:
            if to == "pandas":
                dtype_mapping = {
                    pa.int8(): pd.Int8Dtype(),
                    pa.int16(): pd.Int16Dtype(),
                    pa.int32(): pd.Int32Dtype(),
                    pa.int64(): pd.Int64Dtype(),
                    pa.uint8(): pd.UInt8Dtype(),
                    pa.uint16(): pd.UInt16Dtype(),
                    pa.uint32(): pd.UInt32Dtype(),
                    pa.uint64(): pd.UInt64Dtype(),
                    pa.bool_(): pd.BooleanDtype(),
                    pa.float32(): pd.Float32Dtype(),
                    pa.float64(): pd.Float64Dtype(),
                    pa.string(): pd.StringDtype(),
                }
                if type(queries) == list and type(queries[0]) != list and len(queries) > maxParalelism:
                    LOGGER.warning(f"List of un-parted query is too long, chunk-ing it from {len(queries)} into {maxParalelism} to prevent hitting DB max-connection")
                    df = []
                    # [query1, query2, ...., query10000] convert into parted[[query1,query2,....],[],[], ... maxParalelism[]]
                    for chunk in PandasService.chunkIt(queries, maxParalelism):
                        df.append(cx.read_sql(uri, chunk, return_type='arrow').to_pandas(split_blocks=False, date_as_object=False,types_mapper=dtype_mapping.get))
                    return pd.concat(df)
                
                elif type(queries) == list and type(queries[0]) == list and len(queries) > maxParalelism:
                    LOGGER.warning(f"List of parted query is too long, fetching {len(queries)} into {maxParalelism} steps, In order to prevent hitting DB max-connection")
                    df = []
                    for parted in PandasService.chunkIt(queries, maxParalelism):
                        # parted is list of list of list , parted[chunked[query1,query2,query3]]
                        for chunk in parted:
                            df.append(cx.read_sql(uri, chunk, return_type='arrow').to_pandas(split_blocks=False, date_as_object=False,types_mapper=dtype_mapping.get))
                    return pd.concat(df)
                else:
                    table = cx.read_sql(uri, queries, return_type='arrow')
                    return table.to_pandas(split_blocks=False, date_as_object=False,types_mapper=dtype_mapping.get)
            else:
                raise ValueError(f"{to} is not yet supported")
        except Exception as e:
            raise e
    
    @staticmethod
    def aggregate(df:pd.DataFrame, agg:Dict[str, str], group:List[str], renameCols:Dict[str, str] = {}, reArrangeOrder:List[str] =[], onlyProcess:List[str]= [], uuidColumn:str = None) -> pd.DataFrame:
        """
            This function return aggregated dataframe from given df

            df : your dataframe
            agg : the cols you need to aggregate, e.g. {'colname':'aggfunction'}. See Pandas docs to see list of supported aggregation function
            group : the group cols
            onlyProcess: only process selected row, by default it process all rows
            renameCols : rename the cols into new name. e.g. {'old':'new'} 
            reArrangeOrder : re-arrange the order of columns of your dataframe
        """

        temp = df.groupby(group).agg(agg).reset_index() if len(onlyProcess) == 0 else df[onlyProcess].groupby(group).agg(agg).reset_index()

        if bool(renameCols):
            # if we need to rename cols
            temp.rename(columns=renameCols, inplace=True)

        if len(reArrangeOrder):
            # if we need to re-arrange cols order
            temp = temp.reindex(columns=reArrangeOrder)

        if uuidColumn != None:
            temp[uuidColumn] = [uuid.uuid4() for x in range(temp.shape[0])]
        
        return temp

    @staticmethod
    def day(df:pd.DataFrame):
        return pd.to_datetime(df).dt.date
    
    @staticmethod
    def hour(df:pd.DataFrame):
        return pd.to_datetime(df).dt.hour
    
    @staticmethod
    def year(df:pd.DataFrame):
        return pd.to_datetime(df).dt.year
    
    @staticmethod
    def month(df:pd.DataFrame):
        return pd.to_datetime(df).dt.month
    
    @staticmethod
    def customDate(df:pd.DataFrame, pattern:str):
        return pd.to_datetime(df).dt.strftime(pattern)


    @staticmethod
    def insertHistory(start, end, row, runtime, param, additionalColumns:Dict):
        pass

    @staticmethod
    def getFirstDateOf(month:int, year:int):
        first_date = date(year, month, 1)
        return first_date

    @staticmethod
    def getLastDateOf(month:int, year:int):
        last_date =  date(year +1, 1, 1) - timedelta(days=1)  if month == 12 else date(year, month+1, 1) - timedelta(days=1) 
        return last_date