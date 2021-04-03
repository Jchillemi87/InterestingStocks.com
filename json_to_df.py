# %%
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
logging.basicConfig(filename='example.log', encoding='utf-8')

# %%
def get_last_day(date):
    lastDay=calendar.monthrange(date.year, date.month)[1]
    return date.replace(day=lastDay)

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
        yeardf['Year']=year #append the year
        df=pd.concat([df,yeardf]) #append to the bottom
    return df

def statements_to_df(symbol,data):
    statements = data['Financials']
    #Make df for from both quarterly and yearly reports for all statements
    #reportDates for all quarterly statements and all yearly statements match so no need to do it for all 
    BS = statements['Balance_Sheet']['quarterly']
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

    df=pd.concat([BSDF,CFDF])
    df['Symbol']=symbol

    columnsOrder = ['Symbol', 'Year', 'LineItem', 'Q1', 'Q2', 'Q3', 'Q4', 'Yearly', 'StatementType']
    df = df.reindex(columns=columnsOrder)

    return df

def get_data(symbol='AAPL'):
    with open(f'./data/{symbol}.json') as json_file:
        return json.load(json_file)

if __name__ == "__main__":   
    data = get_data('AAPL')
    df=statements_to_df('AAPL',data)
    df

    #['Symbol', 'StatementType', 'LineItem', 'FiscalYear', 'Q1', 'Q2', 'Q3', 'Q4', 'Yearly']
    #db = pd.DataFrame(columns=columnNames)
# %%
