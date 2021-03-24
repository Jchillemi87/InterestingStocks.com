from datetime import date, timedelta
import pandas as pd
import numpy as np
from pandas.core.indexes.base import Index

def get_market_cap(price, shares): return price*shares
def get_ROE(income, equity): return income/equity
def get_PE(price, book): return price/book
def get_dividend_yield(price, paid): return paid/price
def get_EPS(earnings, shares): return earnings/shares
def get_DE(longDebt, shortLong, book): return (longDebt+shortLong)/book
def get_EPS_growth(newEPS, oldEPS): return (newEPS/oldEPS)-1
def get_PEG(PE, growth): return PE/growth
def get_current_ratio(assets, liabilities): return assets/liabilities
def get_PB(price, book): return price/book

def get_last_quarter(day=None,dtype='date'):
    if day is None:
        day = date.today()
    else:
        if isinstance(day,str):
            day = date.fromisoformat(day)

    q = (day.month-1)//3 + 1
    if q == 1:
        if dtype=='str': return f'{day.year-1}-Q4'
        if dtype=='date': return date(day.year-1, 12, 31).isoformat()

    if q == 2:
        if dtype=='str': return f'{day.year}-Q1'
        if dtype=='date': return date(day.year, 3, 31).isoformat()

    if q == 3:
        if dtype=='str': return f'{day.year}-Q2'
        if dtype=='date': return date(day.year, 6, 30).isoformat()

    if q == 4:
        if dtype=='str': return f'{day.year}-Q3'
        if dtype=='date': return date(day.year, 9, 30).isoformat()

def get_quarter(day=None,dtype='date'):
    if day is None:
        day = date.today()
    else:
        if isinstance(day,str):
            day = date.fromisoformat(day)

    q = (day.month-1)//3 + 1

    if q == 1:
        if dtype=='str': return f'{day.year}-Q1'
        if dtype=='date': return date(day.year, 3, 31).isoformat()
        

    if q == 2:
        if dtype=='str': return f'{day.year}-Q2'
        if dtype=='date': return date(day.year, 6, 30).isoformat()

    if q == 3:
        if dtype=='str': return f'{day.year}-Q3'
        if dtype=='date': return date(day.year, 9, 30).isoformat()

    if q == 4:
        if dtype=='str': return f'{day.year}-Q4'
        if dtype=='date': return date(day.year, 12, 31).isoformat()

def last_weekday(items, pivot):
    return min([i for i in items if i < pivot], key=lambda x: abs(x - pivot))

def get_weekday(x,y):
    days = []
    for date in x['Date']:
        days.append(last_weekday(y['Date'],date))
    return days

class Stock:
    def __init__(self, ticker, funds=None, eod=None):
        self.ticker = ticker
        self.eod=eod
        self.sharesYr=pd.DataFrame.from_dict(funds['outstandingShares']['annual'],orient='index')
        self.sharesQr=pd.DataFrame.from_dict(funds['outstandingShares']['quarterly'],orient='index')
        self.earningsYr=pd.DataFrame.from_dict(funds['Earnings']['Annual'],orient='index')
        self.earningsQr=pd.DataFrame.from_dict(funds['Earnings']['Annual'],orient='index')
        self.balanceYr = pd.DataFrame.from_dict(funds['Financials']['Balance_Sheet']['yearly'],orient='index',dtype=float)
        self.balanceQr = pd.DataFrame.from_dict(funds['Financials']['Balance_Sheet']['quarterly'],orient='index',dtype=float)
        self.incomeYr = pd.DataFrame.from_dict(funds['Financials']['Income_Statement']['yearly'],orient='index',dtype=float)
        self.incomeQr = pd.DataFrame.from_dict(funds['Financials']['Income_Statement']['quarterly'],orient='index',dtype=float)
        self.cashYr = pd.DataFrame.from_dict(funds['Financials']['Cash_Flow']['yearly'],orient='index',dtype=float)
        self.cashQr = pd.DataFrame.from_dict(funds['Financials']['Cash_Flow']['quarterly'],orient='index',dtype=float)

        self.sharesYr.rename(columns={'date':'Fiscal Quarter','dateFormatted':'date'},inplace=True)
        self.sharesQr.rename(columns={'date':'Fiscal Quarter','dateFormatted':'date'},inplace=True)

        self.sharesYr['Fiscal Quarter']=self.sharesYr['date'].apply(lambda x: get_quarter(x))
        self.sharesQr['Fiscal Quarter']=self.sharesQr['date'].apply(lambda x: get_quarter(x))

        self.earningsYr['Fiscal Quarter']=self.earningsYr['date'].apply(lambda x: get_last_quarter(x))
        self.earningsQr['Fiscal Quarter']=self.earningsQr['date'].apply(lambda x: get_last_quarter(x))

        self.balanceYr['Fiscal Quarter']=self.balanceYr['date'].apply(lambda x: get_last_quarter(x))
        self.balanceQr['Fiscal Quarter']=self.balanceQr['date'].apply(lambda x: get_last_quarter(x))
        self.incomeYr['Fiscal Quarter']=self.incomeYr['date'].apply(lambda x: get_last_quarter(x))
        self.balanceQr['Fiscal Quarter']=self.balanceQr['date'].apply(lambda x: get_last_quarter(x))
        self.cashYr['Fiscal Quarter']=self.cashYr['date'].apply(lambda x: get_last_quarter(x))
        self.cashQr['Fiscal Quarter']=self.cashQr['date'].apply(lambda x: get_last_quarter(x))

        self.balanceYr['totalDebt']=self.balanceYr.apply(lambda x: max(x[['shortTermDebt','shortLongTermDebt','shortLongTermDebtTotal']])+max(x[['longTermDebt','longTermDebtTotal']]),axis='columns')

        self.mainYr = pd.DataFrame(data={'Date':np.array(list(funds['Financials']['Balance_Sheet']['yearly'].keys()))},
        columns=['Date','Fiscal Quarter','Market Cap','PB','Yield','PE','ROE','DE','Current Ratio','EPS','PEG'])

        self.mainYr['Date'] = pd.to_datetime(self.mainYr['Date']) 

        #financial filing days maybe on weekends, resulting in no eod price
        mask=self.mainYr['Date'].isin(self.eod['Date'])
        self.mainYr['Week Day'] = self.mainYr[mask]['Date']
        
        kwargs = {'Week Day': lambda x: get_weekday(x,self.eod)}
        self.mainYr[~mask]=self.mainYr[~mask].assign(**kwargs)

        mask=(self.eod['Date']).isin(self.mainYr['Week Day'])
        self.mainYr['Fiscal Quarter'] = self.mainYr['Date'].apply(lambda x: get_last_quarter(day=x))
        self.eod[mask][['Adjusted_close']]

        self.mainYr=pd.merge(self.mainYr,self.eod.rename(columns={'Date': 'Week Day'})[['Week Day','Adjusted_close']],on='Week Day',how='left')
        self.mainYr=self.mainYr.rename(columns={'Adjusted_close':'Price'})

        self.balanceYr['date'] = pd.to_datetime(self.balanceYr['date'])
        self.balanceYr.index = pd.to_datetime(self.balanceYr.index)

        self.mainYr=pd.merge(self.mainYr.set_index('Date'),self.balanceYr[['totalStockholderEquity','totalLiab','totalCurrentLiabilities','totalCurrentAssets','totalDebt']],
        left_index=True,right_index=True,how='left')
        self.mainYr.rename(columns={'totalStockholderEquity': 'Book Value'},inplace=True)

        self.cashYr['date'] = pd.to_datetime(self.cashYr['date'])
        self.cashYr.index = pd.to_datetime(self.cashYr.index)
        self.mainYr=pd.merge(self.mainYr,self.cashYr[['dividendsPaid','netIncome']],left_index=True,right_index=True,how='left')
        print(self.mainYr[['dividendsPaid','netIncome']])

        self.sharesYr.set_index('Fiscal Quarter',inplace=True)
        self.mainYr.set_index('Fiscal Quarter',inplace=True)

        self.mainYr=pd.merge(self.mainYr,self.sharesYr['shares'],left_index=True,right_index=True,how='left')
        self.mainYr.rename(columns={'shares':'Shares Outstanding'},inplace=True)

        self.mainYr['Market Cap'] = self.mainYr['Shares Outstanding'] * self.mainYr['Price']
        self.mainYr['BVPS'] = self.mainYr['Book Value'] / self.mainYr['Shares Outstanding']
        self.mainYr['PB'] = self.mainYr['Price']/self.mainYr['BVPS']
        self.mainYr['DE'] = self.mainYr['totalDebt']/self.mainYr['Book Value']

        #self.mainYr['Dividend Yield'] = sum(self.cashQr['dividendsPaid'].head(4))
        #print(self.mainYr['Dividend Yield'])

        self.mainYr['ROE'] = self.mainYr.apply(lambda x: get_ROE(income=x['netIncome'],equity=x['Book Value']),axis='columns')

        print(self.mainYr['ROE'])
        
        self.earningsYr.set_index('Fiscal Quarter',inplace=True)
        
        self.mainYr=pd.merge(self.mainYr,self.earningsYr['epsActual'],left_index=True,right_index=True,how='left')
        self.mainYr.rename(columns={'epsActual':'EPS'},inplace=True)

        return 

        try:
            self.balanceQr[self.lastQtr]
        except KeyError:
            new = date.today() - timedelta(days=90)
            self.lastQtr = get_last_quarter(new.isoformat())

        self.sharesOutstanding = int(
            float(self.balanceQr[self.lastQtr]['commonStockSharesOutstanding']))
        self.bookValue = float(
            self.balanceQr[self.lastQtr]['totalStockholderEquity'])
        self.netIncome = float(self.incomeQr[self.lastQtr]['netIncome'])
        self.longTermDebt = float(self.balanceQr[self.lastQtr]['longTermDebt'])

        # longTerm liabilities obligations that must be met soon
        if self.balanceQr[self.lastQtr]['shortLongTermDebt'] is None:
            self.shortLongTermDebt = 0
        else:
            self.shortLongTermDebt = float(
                self.balanceQr[self.lastQtr]['shortLongTermDebt'])

        self.currentAssets = float(
            self.balanceQr[self.lastQtr]['totalCurrentAssets'])
        self.currentLiabilities = float(
            self.balanceQr[self.lastQtr]['totalCurrentLiabilities'])

        self.marketCap = get_market_cap(
            price=self.price, shares=self.sharesOutstanding)
        self.PB = get_PB(price=self.price, book=self.bookValue)
        #self.dividendYield = self.get_dividend_yield()
        self.PE = get_PE(price=self.price, book=self.bookValue)
        self.ROE = get_ROE(income=self.netIncome, book=self.bookValue)
        self.DE = get_DE(longDebt=self.longTermDebt,
                         shortLong=self.shortLongTermDebt, book=self.bookValue)
        self.current = get_current_ratio(
            assets=self.currentAssets, liabilities=self.currentLiabilities)

        self.EPS = get_EPS(earnings=self.netIncome,
                           shares=self.sharesOutstanding)
        self.oldEPS = get_EPS(earnings=self.netIncome,
                              shares=self.sharesOutstanding)
        #self.PEG = get_PEG(self.PE,get_EPS_growth(newEPS=self.EPS,))

    def get_market_cap(self):
        dates=self.balanceQr[self.balanceQr['commonStockSharesOutstanding'].notna()]['date']
        self.mainYr.set_index('Week Day')