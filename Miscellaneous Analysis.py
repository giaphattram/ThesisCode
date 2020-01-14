# -*- coding: utf-8 -*-
"""
Created on Fri Apr  5 23:34:45 2019

@author: Admin
"""
import pandas as pd
import numpy as np
import os
os.getcwd()
os.chdir("D:\Master's Thesis\Data")
#%%
# =============================================================================
# SP500 Proxy Analysis
# =============================================================================
sp500 = pd.read_csv("./Indices/[index] SP500 Composite Index Return.csv")
sp500['caldt'] = pd.to_datetime(sp500.caldt, format = "%d/%m/%Y")
sp500['caldt'] = np.where(sp500.caldt.dt.is_month_end,\
                          sp500.caldt,\
                          sp500.caldt + pd.tseries.offsets.MonthEnd())

spProxy = sp_constituents.copy(deep = True)
spProxy = spProxy[spProxy.Date.isin(sp500.caldt.unique())]
spProxy['Date'] = np.where(spProxy.Date.dt.is_month_end, spProxy.Date, spProxy.Date + pd.tseries.offsets.MonthEnd())

numberStocksByYearBEFORECRSPMAPPING = sp_constituents.groupby(sp_constituents.Date.dt.year).permno.nunique().reset_index()



#Import stock return data from CRSP. Note that after removing returns = 'B' 'C', the number of stocks per year reduces
stockData = pd.read_csv("./Securities/[stock] CRSP Stock Monthly Return.csv")
stockData['date'] = pd.to_datetime(stockData.date, format = "%d/%m/%Y")
stockData = stockData[['PERMNO', 'date', 'RET', 'RETX']]
stockData = stockData[stockData.date.isin(sp500.caldt)]
stockData = stockData[stockData.RET != 'B']
stockData = stockData[stockData.RET != 'C']
stockData['RET'] = stockData.RET.astype(float)
stockData['RETX'] = stockData.RET.astype(float)
stockData['date'] = np.where(stockData.date.dt.is_month_end, stockData.date, stockData.date + pd.tseries.offsets.MonthEnd())


#Calculate weighted return for each stock
spProxy = pd.merge(left = spProxy, right = stockData,\
                   left_on = ['Date', 'permno'], \
                   right_on = ['date', 'PERMNO'],\
                   how = 'left', indicator = True)

numberStocksByYearAFTERCRSPMAPPING = spProxy.groupby(spProxy.Date.dt.year).permno.nunique().reset_index()
numberStocksByYear = pd.merge(left = numberStocksByYearBEFORECRSPMAPPING, right = numberStocksByYearAFTERCRSPMAPPING,\
                              on = 'Date', how = 'inner')

spProxy['weightedRet'] = spProxy.constituent_weight * spProxy.RET
spProxy['weightedRetx'] = spProxy.constituent_weight * spProxy.RETX

#calculate return at each day for the proxy
spProxy = spProxy.groupby('Date')['weightedRet', 'weightedRetx'].sum()
spProxy = spProxy.reset_index()

#merge with actual sp500 returns
spProxy = pd.merge(left = spProxy, right = sp500,\
                   left_on = 'Date', right_on = 'caldt',\
                   how = 'left')

spProxy.rename({'weightedRet': 'Proxy Return'}, axis = 1, inplace = True)
spProxy.rename({'sprtrn' : 'S&P 500 Composite Index Return'}, axis = 1, inplace = True)