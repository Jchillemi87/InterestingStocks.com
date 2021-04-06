# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.types import Text, String, SmallInteger, Numeric
import os
import json
import threading
import requests
from requests.exceptions import HTTPError

from io import StringIO

from datetime import date
from datetime import timedelta

from json_to_df import statements_to_df_qt, get_json_data
#import importlib
#importlib.reload(stocks) # every run

from alive_progress import alive_bar

import logging
logging.basicConfig(filename='main.log', encoding='utf-8')

import time
'''
tic = time.perf_counter()
    --function to time here--
toc = time.perf_counter()p
print(f"Downloaded the tutorial in {toc - tic:0.4f} seconds")
'''

#api_key='OeAFFmMliFG5orCUuwAKQ8l4WWFQ67YX'
api_key='604f9349dd39a0.52974186'

# %%
def get_files(path):
    return set(os.path.splitext(filename)[0] for filename in os.listdir(path))

# %%
def save_json(data,name):
    path = f'./data/{name}.json'
    with open(path, 'w') as json_file:
        json_file.write(data)
        #json.dump(data, json_file, ensure_ascii=False)

# %%
def get_api_data(url,params,session=None):
    if session is None:
        session = requests.Session()
        try:
            response = session.get(url, params=params)
            
            # If the response was successful, no Exception will be raised
            response.raise_for_status()
        except HTTPError as http_err:
            logging.warning(f'HTTP error occurred: {http_err}\nproblem with: {url}, {params}')
            return err
        except Exception as err:
            logging.warning(f'Other error occurred: {err}\nproblem with: {url}, {params}')  # Python 3.6
            return err
        else:
            return response.text

def get_fundamentals_data(symbol='AAPL', api_token=api_key):
    url = f'https://eodhistoricaldata.com/api/fundamentals/{symbol}.US'
    params = {'api_token': api_token}
    try:
        data = get_api_data(url=url,params=params)
        save_json(data=data,name=symbol)
        return data
    except Exception as err:
        return err

def get_eod_data(symbol='AAPL', api_token=api_key):
    url = f'https://eodhistoricaldata.com/api/eod/{symbol}'
    params = {'api_token': api_token, 'order': 'd', 'fmt': 'csv'}
    try:
        data = get_api_data(url=url,params=params)
        df = pd.read_csv(StringIO(data), skipfooter=1, parse_dates=[0], index_col=False, engine='python')
        df.to_csv(f'./EOD/{symbol}.csv', index=False)
        return df
    except Exception as err:
        return err

# %%
tickersdf=pd.read_csv('US_LIST_OF_SYMBOLS.csv')
symbols=set(tickersdf.loc[tickersdf['Type']=='Common Stock',['Code']]['Code'])
dataFiles = get_files('./data')
eodFiles = get_files('./EOD')
dataMiss=symbols-dataFiles
eodMiss=symbols-eodFiles
# %%
user = 'root'
password = 'root_password'
host = '192.168.1.201'
port = '3306'

def engine(database):
    return create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}')

def csv_to_df(symbol='AAPL',folder='./db/'):
    df=pd.read_csv(f'{folder}{symbol}.csv')
    return df

def df_to_mysql(df,symbol='AAPL',dbName = 'InterestingStocksFundamentals'):
    df.to_sql(name = symbol, con = engine(dbName), if_exists = 'replace', index = False,
    dtype={
        'Symbol': String(10),
        'LineItem': Text,
        'Year': SmallInteger,
        'Q1': Numeric(precision=15,decimal_return_scale=2),
        'Q2': Numeric(precision=15,decimal_return_scale=2),
        'Q3': Numeric(precision=15,decimal_return_scale=2),
        'Q4': Numeric(precision=15,decimal_return_scale=2),
        'Yearly': Numeric(precision=20,decimal_return_scale=2),
        'StatementType': String(20)
    })
# %%
def state_to_df(symbol='AAPL'):
    try:
        data=get_json_data(symbol)
        return statements_to_df_qt(symbol,data)
        #df.to_csv(f'./db/{symbol}.csv',index=False)
    except Exception as err:
        logging.warning(f'\n{type(err)}\nerror in state_to_df\narguments{err.args}')

def state_to_mysql(symbol):
    try:
        df=state_to_df(symbol)
        df_to_mysql(df,symbol=symbol)
    except Exception as err:
        logging.warning(f'\n{type(err)}\nerror in state_to_mysql\narguments{err.args}')

def update():
#    get_fundamentals_data(symbol='aapl')
    api_limiter=150000
    #dataMiss=['KSS','WLKP','FL','DAL','MO','CCL','T','TPR','PFG','WFC']
    
    with alive_bar(len(dataMiss), title='dataMiss', spinner='waves') as bar:
        for symbol in dataMiss:
            if(api_limiter>0):
                api_limiter-=1
                t = threading.Thread(target=get_fundamentals_data, args=(symbol,))
                t.start()
            bar()

    api_limiter=150000
    dataFiles = get_files('./data')
    eodMiss=dataFiles-eodFiles
    with alive_bar(len(eodMiss), title='eodMiss', spinner='waves') as bar:
        for symbol in eodMiss:
            if(api_limiter>0):
                api_limiter-=1
                t = threading.Thread(target=get_eod_data, args=(symbol,))
                t.start()
            bar()

    dbFiles = get_files('./db')
    dbMissing=dataFiles-dbFiles
    with alive_bar(len(dbMissing), title='dbMissing', spinner='waves') as bar:
        for symbol in dbMissing:
            t = threading.Thread(target=state_to_mysql, args=(symbol,))
            t.start()
            bar()

if __name__ == '__main__':
    #dbFiles = get_files('./db')
    dbMissing=dataFiles#-dbFiles
    with alive_bar(len(dbMissing), title='dbMissing', spinner='waves') as bar:
        for symbol in dbMissing:
            t = threading.Thread(target=state_to_mysql, args=(symbol,))
            t.start()
            bar()

# ***ctrl + / for mass commenting***
# async def load_json(ticker):
#     path = f'./data/{ticker}.json'
#     print(path)
#     async with aiofiles.open(path, mode='r') as json_file:
#         content = await json_file.read()
#         return(content)

# async def mainReadFiles():
#     data = await load_json('WLKP')
#     data = json.loads(data)
    
#     stock = Stock(ticker='WLKP', data=data)
#     return stock.mainYr

# loop = asyncio.get_event_loop()
# loop.run_until_complete(mainReadFiles())