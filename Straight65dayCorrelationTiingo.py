# -*- coding: utf-8 -*-

import os
import urllib
import sqlalchemy as sql
import pyodbc
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import pandas_market_calendars as mcal
from datetime import date

load_dotenv()

# database connection parameters
server = os.getenv('MSSQL_SERVER')
database = 'StockOdds_Dev' # os.environ.get('DB_DATABASE')
db_table = 'Pairs_corr'
# username = os.getenv('MSSQL_USERNAME')
# password = os.getenv('MSSQL_PASSWORD')

# params = urllib.parse.quote_plus('DRIVER={SQL Server};SERVER='+server+
#                                  ';DATABASE='+database+';UID='+username+';PWD='+password)
# engine = sql.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params, fast_executemany=True)

# Ken's local connection code
# params = urllib.parse.quote_plus('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+
#                                  ';DATABASE='+database+';Trusted_Connection=yes;')
params = urllib.parse.quote_plus('DRIVER={ODBC Driver 13 for SQL Server};SERVER='+server+';DATABASE='+database+';Trusted_Connection=yes;')

engine = sql.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

def run_correlation_analysis():
    start_date = '2019-09-15'#
    end_date = date.today().strftime('%Y-%m-%d')# '2020-01-16'
    dates_list = mcal.get_calendar('NYSE').schedule(start_date=start_date, end_date=end_date)
    dates_list.index = dates_list.index.strftime('%Y-%m-%d')    # make index type object to match the data frames coming from database

    # # only look at the past 65 days, use tiingo data
    start_date1 = dates_list.index[-65]
    end_date1 = dates_list.index[-1]
    sym_data_tiingo = pd.read_sql(f"select * from raw_stock_tiingo_3yr " \
                              f"where date between '{start_date1}' and '{end_date1}'", engine)

    sym_data_tiingo['Date'] = pd.to_datetime(sym_data_tiingo['Date'])
    sym_data_tiingo.set_index(['MasterSymbol','Date'], inplace=True)
    sym_data_tiingo.index.rename('Symbol',level=0, inplace=True)
    sym_time_data_tiingo = sym_data_tiingo[['Open','Close','High','Low','TR','Volume']].sort_index()

    # pivot the frame to get Close prices of all symbols as columns
    df_pivot_tiingo = sym_time_data_tiingo.reset_index()
    df_pivot_tiingo = df_pivot_tiingo.pivot(index='Date', columns='Symbol', values='Close')
    # drop all stocks where data is incomplete
    df_pivot_tiingo_nonan = df_pivot_tiingo.dropna(axis=1, how='any')
    #df_pivot_tiingo_nonan.to_csv('df_pivot_tiingo_nonan.csv')

    corr_mtx_tiingo = df_pivot_tiingo_nonan.corr()

    # reshape into columns
    corr_mtx_tiingo_simplified = corr_mtx_tiingo.where(np.triu(np.ones(corr_mtx_tiingo.shape)).astype(np.bool))
    corr_mtx_tiingo_simplified = corr_mtx_tiingo_simplified.rename_axis('Symbol1') # rename one of the columns so that I can restack it
    cmts = corr_mtx_tiingo_simplified.stack().reset_index()
    cmts = cmts.rename(columns={"Symbol": "Symbol2", 0: "CorrCoeff"})

    # cmts.to_sql(db_table, con=engine, if_exists='replace', index=False)


    data = {'Symbol1': ['AAA', 'BBB', 'CCC', 'DDD'], 'Symbol2': ['EEE', 'FFF', 'GGG', 'HHH'],
            'CorrCoeff': ['.5555', '.6666', '.7777', '.8888']}
    df = pd.DataFrame(data)
    df.to_sql(db_table, con=engine, if_exists='replace', index=False)

    #cmts.to_sql('CorrelationMatrixTiingoSimplified', engine, if_exists='replace', index = False)
    #but the above to_sql didn't work because I was running out of memory, so I ended up copying to csv and then importing through SSMS
    #should also drop the rows with self-correlation to cut down on memory a little bit
    #cmts.to_csv('corr_mtx_tiingo_simplified.csv')


if __name__=='__main__':
    print('Program Entry')
    run_correlation_analysis()
    print('Program Exit')