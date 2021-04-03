# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import pandas as pd
import json
import threading
import requests
from requests.exceptions import HTTPError

from io import StringIO

from datetime import date
from datetime import timedelta

import stocks
import importlib
importlib.reload(stocks) # every run

from stocks import Stock

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
    print(data)
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
            print(f'HTTP error occurred: {http_err}\nproblem with: {url}, {params}')
        except Exception as err:
            print(f'Other error occurred: {err}\nproblem with: {url}, {params}')  # Python 3.6
        else:
            #if r.status_code == requests.codes.ok:
            return response.text

def get_fundamentals_data(symbol='AAPL', api_token=api_key):
    url = f'https://eodhistoricaldata.com/api/fundamentals/{symbol}.US'
    params = {'api_token': api_token}
    data = get_api_data(url=url,params=params)
    save_json(data=data,name=symbol)
    return data

def get_eod_data(symbol='AAPL', api_token=api_key):
    url = f'https://eodhistoricaldata.com/api/eod/{symbol}'
    params = {'api_token': api_token, 'order': 'd', 'fmt': 'csv'}
    data = get_api_data(url=url,params=params)
    df = pd.read_csv(StringIO(data), skipfooter=1, parse_dates=[0], index_col=False, engine='python')
    df.to_csv(f'./EOD/{symbol}.csv', index=False)
    return df


# %%
tickersdf=pd.read_csv('US_LIST_OF_SYMBOLS.csv')
symbols=set(tickersdf.loc[tickersdf['Type']=='Common Stock',['Code']]['Code'])
dataFiles = get_files('./data')
eodFiles = get_files('./EOD')
dataMiss=symbols-dataFiles
eodMiss=dataFiles-eodFiles


# %%
#extra=dataFiles-eodFiles
#for file in extra:
#    os.rename(f'./data/{file}.json',f'./backup/data/{file}.json')


# %%
def mainGetData(symbol):
    get_eod_data(symbol)
    #get_fundamentals_data(symbol)


if __name__ == '__main__':
#    get_fundamentals_data(symbol='aapl')
    api_limiter=500000
    #dataMiss=['KSS','WLKP','FL','DAL','MO','CCL','T','TPR','PFG','WFC']
    for symbol in eodMiss:
        if(api_limiter>0):
            api_limiter-=1
            t = threading.Thread(target=mainGetData, args=(symbol,))
            t.start()


# %%
async def load_json(ticker):
    path = f'./data/{ticker}.json'
    print(path)
    async with aiofiles.open(path, mode='r') as json_file:
        content = await json_file.read()
        return(content)

async def mainReadFiles():
    data = await load_json('WLKP')
    data = json.loads(data)
    
    stock = Stock(ticker='WLKP', data=data)
    return stock.mainYr

loop = asyncio.get_event_loop()
loop.run_until_complete(mainReadFiles())


