# InterestingStocks.com
***) grab all active stock tickers

) download all relevant historical values (dividends, book value, shares, etc)
    -Market Cap Over $50million (Price X Outstanding Shares)
    -Price/Book Under 2
    -Dividend Yield >0%
    -Price/Earning Under 20
    -Return On Equity >5%
    -Debt/Equity < 1
    -Current Ratio > 1.5
    -Eps Growth This Year >0%
    -Peg < 2 (Peg = Pe/Ttm Earnings Growth Rate)


Earnings

EPS Growth
    How to calculate Growth:
        Subtract the initial EPS from the final EPS.
        Divide the change in EPS by the initial EPS.
        Multiply the result by 100 to calculate the EPS growth rate as a percentage.

Return on Equity:   (ROE is considered the return on net assets)
    net income / shareholders' equity. 


Debt / Equity = Long Term Debt + Current Portion of Long Term Debt) / Total Shareholders' Equity

Current Ratio = Current Liabilities / Current Assets
​
fundamentaldata -> get_balance_sheet_quarterly or get_balance_sheet_annual
commonStockSharesOutstanding
totalAssets
totalLiabilities
totalCurrentLiabilities
totalCurrentAssets
intangibleAssets
netIncome (for PE = P/EPS)

EPS = netIncome/commonStockSharesOutstanding
Book Value = totalShareholderEquity = (totalAssets-totalLiabilities)
Debt/Equity = (longTermDebt+currentLongTermDebt)/totalShareholderEquity

Earnings

Shares Outstanding


Book Value = Stockholders' Equity - Preferred Stock (total assets - intangible assets - liabilities)

EPS Growth
    How to calculate Growth:
        Subtract the initial EPS from the final EPS.
        Divide the change in EPS by the initial EPS.
        Multiply the result by 100 to calculate the EPS growth rate as a percentage



Company
StatementType
LineItem
FiscalYear
Q1, Q2, Q3, Q4


------------------------------------------------------------------------------
)store data into a SQL database
    -each stock should be its own timeseries based table
        -Fundamental data should be one database
            -further broken down into years + quarters
        -Price should be another

)set up filters for sql data

)determine what data from database is required for selected filters

)pull sql data using filter

)compute fields required for selected filters

)return filtered tickers
    - & - or -
)order filtered tickers by similarity

)Tableau/UX/UI