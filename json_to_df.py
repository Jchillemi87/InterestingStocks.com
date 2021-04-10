# %%
from importlib.util import find_spec
from os import name
import pandas as pd
import json
import threading
import numpy as np
from pandas.core.frame import DataFrame

from datetime import date, timedelta, datetime
import calendar
from dateutil.relativedelta import relativedelta

import logging

from pandas.core.tools import numeric
logging.basicConfig(filename='example.log', encoding='utf-8')

# %%
def get_last_price(d,EODdf):
    if d in EODdf['date']: return EODdf.loc[EODdf['date']==d]['Adjusted_close']
    day = datetime.fromisoformat(d)
    dates = EODdf['date'].tolist()
    dates = [datetime.fromisoformat(D) for D in dates]
    last = day+relativedelta(days=-10) #if we can't find a price within the last 10 days, just give up
    
    while (day not in dates) & (day >= last):
        day=day+relativedelta(days=-1)

    if day < last: return None
    day = day.date()
    day = day.isoformat()
    lastPrice = EODdf.loc[EODdf['date']==day]['Adjusted_close'].max()
    return lastPrice

def get_last_day(d):
    lastDay=calendar.monthrange(d.year, d.month)[1]
    return d.replace(day=lastDay)

def get_fiscal_quarters(symbol,fiscal_end,dates):
    try:
        dates = list(dates)
        year1=date.fromisoformat(dates[-1]).year
    except:
        logging.warning(f'\nerror in get_fiscal_quarters. Symbol: {symbol}, dates:{dates}')
        return
    fiscal_quarters = {}
    yr = date.today().year
    fiscal_end = datetime.strptime(fiscal_end,'%B').month
    
    qrts = {}
    while(yr>=year1):
        end=date(yr,fiscal_end,1)
        q4=get_last_day(end)
        q3=get_last_day(end+relativedelta(months=-3))
        q2=get_last_day(end+relativedelta(months=-6))
        q1=get_last_day(end+relativedelta(months=-9))

        qrts[yr]={'Q4':q4.isoformat(),'Q3':q3.isoformat(),'Q2':q2.isoformat(),'Q1':q1.isoformat()}
        
        yr-=1
    return qrts

def get_fiscal_years(symbol,dates):
    try:
        dates = list(dates)
        year1=date.fromisoformat(dates[-1]).year
    except:
        logging.warning(f'\nerror in get_fiscal_years. Symbol: {symbol}, dates:{dates}')
        return

    return {date.fromisoformat(d).year:{'Yearly':d} for d in dates}

# %%
def make_df(dates,statement):
    df=pd.DataFrame(columns=['Year','LineItem'])
    for year in dates:
        yeardf=pd.DataFrame(columns=['Year','LineItem'])
        for reportDate in dates[year]:
            if dates[year][reportDate] in statement.keys():
                qtrItems=pd.DataFrame([{'LineItem':k,reportDate:v} for (k,v) in statement[dates[year][reportDate]].items()])
                 #check if the date exists as a key in the statement data that was passed over
                 #if it does than use a list comprehension to create a dictionary with keys 'LineItem' and the Quarter name
                 #make a dataframe frame from the list comprehension
                yeardf=pd.merge(yeardf,qtrItems,on='LineItem',how='right')
                #add the qtr dataframe data to the dataframe for that year
        yeardf=yeardf.loc[~yeardf['LineItem'].isin(['date','filing_date','currency_symbol'])]
        yeardf['Year']=year #append the year
        df=pd.concat([df,yeardf]) #append to the bottom
    return df

def statements_to_df_qt(symbol,data):
    statements = data['Financials']
    #Make df for from both quarterly and yearly reports for all statements
    #reportDates for all quarterly statements and all yearly statements match so no need to do it for all 
    BS = statements['Balance_Sheet']['quarterly']
    try:
        reportDates=get_fiscal_quarters(symbol,fiscal_end=data['General']['FiscalYearEnd'],dates=BS.keys())
        BSDF=make_df(reportDates,BS)

        BSyr = statements['Balance_Sheet']['yearly']
        reportDatesYr=get_fiscal_years(symbol,dates=BSyr.keys())
        BSyrDF=make_df(reportDatesYr,BSyr)

        BSDF = pd.merge(BSDF,BSyrDF,on=['LineItem','Year'],how='outer')
        BSDF['StatementType']='Balance_Sheet'
    
        CF = statements['Cash_Flow']['quarterly']
        CFDF=make_df(reportDates,CF)
        CFyr = statements['Cash_Flow']['yearly']
        CFyrDF=make_df(reportDatesYr,CFyr)    
        CFDF = pd.merge(CFDF,CFyrDF,on=['LineItem','Year'],how='outer')
        CFDF['StatementType']='Cash_Flow'
    
        IS = statements['Income_Statement']['quarterly']
        ISDF=make_df(reportDates,IS)
        ISyr = statements['Income_Statement']['yearly']
        ISyrDF=make_df(reportDatesYr,ISyr)
        ISDF = pd.merge(ISDF,ISyrDF,on=['LineItem','Year'],how='outer')
        ISDF['StatementType']='Income_Statement'

        df=pd.concat([BSDF,CFDF,ISDF])
        df['Symbol']=symbol

        columnsOrder = ['Symbol', 'Year', 'LineItem', 'Q1', 'Q2', 'Q3', 'Q4', 'Yearly', 'StatementType']
        df = df.reindex(columns=columnsOrder)

        return df

    except Exception as err:
        logging.warning(f'\n{type(err)}\nerror in statements_to_df_qt.\narguments{err.args}')     # arguments stored in .args
        return err

def get_json_data(symbol='AAPL'):
    with open(f'/home/joseph/InterestingStocks.com/data/{symbol}.json') as json_file:
        return json.load(json_file)
# %%
#~~~~helper functions
def get_market_cap(price, shares): return price*shares
def get_ROE(income, equity): return (income/equity) if equity != 0 else 0
def get_PE(price, eps): return (price/eps) if eps != 0 else 0
def get_dividend_per_share(paid,shares): return (paid/shares) if shares != 0 else 0
def get_dividend_yield(price, paid): return (paid/price) if price != 0 else 0
def get_EPS(earnings, shares): return (earnings/shares) if shares != 0 else 0
def get_DE(longDebt, shortLong, book): return ((longDebt+shortLong)/book) if book != 0 else 0
def get_EPS_growth(newEPS, oldEPS): return (newEPS/oldEPS)-1 if oldEPS != 0 else 0
def get_PEG(PE, growth): return (PE/(growth*100)) if growth != 0 else 0
def get_current_ratio(assets, liabilities): return (assets/liabilities) if liabilities != 0 else 0
def get_BVPS(equity,shares): return (equity/shares) if shares != 0 else 0 
def get_PB(price, book): return (price/book) if book != 0 else 0
# %%
month={'January':1,'February':2,'March':3,'April':4,'May':5,'June':6,
'July':7,'August':8,'September':9,'October':10,'November':11,'December':12}

def get_fiscal_quarter(day,fiscal_end):
    if isinstance(day,str): day = date.fromisoformat(day)
    qtr={3:'Q1',6:'Q2',9:'Q3',12:'Q4'}
    
    if month.get(fiscal_end) == day.month: return 'Q4'

    return qtr.get((day.month-month.get(fiscal_end))%12)

def create_df(symbol,data,EODdf):
    statements = data['Financials']
    #Make df for from both quarterly and yearly reports for all statements
    #reportDates for all quarterly statements and all yearly statements match so no need to do it for all 
    
    BS=pd.DataFrame(statements['Balance_Sheet']['quarterly'].values())
    CF=pd.DataFrame(statements['Cash_Flow']['quarterly'].values())
    IS=pd.DataFrame(statements['Income_Statement']['quarterly'].values())
    EPS = pd.DataFrame(data['Earnings']['History'].values())
    EPS = EPS[['date', 'epsActual', 'epsEstimate', 'epsDifference', 'surprisePercent']]

    df = pd.merge(BS,CF,on=['date','filing_date','currency_symbol'])
    df = pd.merge(df,IS,on=['date','filing_date','currency_symbol','netIncome'])
    df = pd.merge(df,EPS,on=['date'])

    fiscal_end=data['General']['FiscalYearEnd']
    df['Fiscal_Quarter'] = df['date'].apply(lambda x: get_fiscal_quarter(x,fiscal_end))
    
    BSyr = statements['Balance_Sheet']['yearly']
    CFyr = statements['Cash_Flow']['yearly']
    ISyr = statements['Income_Statement']['yearly']

    BSyrdf=pd.DataFrame(BSyr.values())
    CFyrdf=pd.DataFrame(CFyr.values())
    ISyrdf=pd.DataFrame(ISyr.values())
    EPSyr = pd.DataFrame(data['Earnings']['Annual'].values())
    dfyr = pd.merge(BSyrdf,CFyrdf,on=['date','filing_date','currency_symbol'])
    dfyr = pd.merge(dfyr,ISyrdf,on=['date','filing_date','currency_symbol','netIncome'])
    dfyr = pd.merge(dfyr,EPSyr,on=['date'])

    dfyr['Fiscal_Quarter'] = 'Yearly'
    df = pd.concat([df,dfyr])
    EODdf.rename(columns={'Date':'date'},inplace=True)
    df=pd.merge(df,EODdf[['date','Adjusted_close']],on=['date'],how='left')
    df.rename(columns={'Adjusted_close':'Price'},inplace=True)

    emptyPrices=df['Price'].isna()
    df.loc[emptyPrices,'Price'] = df.apply(lambda x: get_last_price(x['date'],EODdf),axis=1)
    ##Convert Strings to datetime & floats in order to get ratios
    excluded=['date','filing_date','currency_symbol','Q1','Q2','Q3','Q4','Yearly']
    numeric = [col for col in df.columns if col not in excluded]
    df[['date','filing_date']] = df[['date','filing_date']].apply(pd.to_datetime, infer_datetime_format=True, errors='ignore')
    df[numeric]=df[numeric].apply(pd.to_numeric, errors='ignore')

    df['marketCapitalization'] = df.apply(lambda x: get_market_cap(x['Price'],x['commonStockSharesOutstanding']),axis=1)
    df['ROE'] = df.apply(lambda x: get_ROE(x['netIncome'],x['commonStockTotalEquity']),axis=1)
    df.rename(columns={'epsActual':'EPS'},inplace=True)
    df['PE'] = df.apply(lambda x: get_PE(x['Price'],x['EPS']),axis=1)
    df['dividends'] = df.apply(lambda x: get_dividend_per_share(x['dividendsPaid'],x['commonStockSharesOutstanding']),axis=1)
    df['dividendYield'] = df.apply(lambda x: get_dividend_yield(x['Price'],x['dividendsPaid']),axis=1)
    df['DE'] = df.apply(lambda x: get_DE(x['longTermDebtTotal'],x['shortLongTermDebtTotal'],x['totalStockholderEquity']),axis=1)
    df['currentRatio'] = df.apply(lambda x: get_current_ratio(x['totalCurrentAssets'],x['totalCurrentLiabilities']),axis=1)
    df['BVPS'] = df.apply(lambda x: get_BVPS(x['totalStockholderEquity'],x['commonStockSharesOutstanding']),axis=1)
    df['PB'] = df.apply(lambda x: get_PB(x['Price'],x['BVPS']),axis=1)

    ##Determine Growth from one quarter to another
    mask = df['Fiscal_Quarter'] != 'Yearly'
    df.loc[mask,'EPS_growth'] = df[mask]['EPS'].pct_change(periods=-4)
    
    ##Determine Growth from one year to next
    mask = df['Fiscal_Quarter'] == 'Yearly'
    df.loc[mask,'EPS_growth'] = df[mask]['EPS'].pct_change(periods=-1)
    
    df['PEG']=df.apply(lambda x: get_PEG(x['PE'],x['EPS_growth']),axis=1)
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    df['symbol'] = symbol

    return df

# %%
