#%%
# =============================================================================
# To do: Change the directory variable to the location of
# the folder "Thesis Submission_GiaPhatTram" 
# For example: E:\Students\Thesis\Thesis Submission - Gia Phat Tram
# =============================================================================
import os
directory = "D:\Thesis Submission - Gia Phat Tram"
os.chdir(directory)
#%%
# =============================================================================
# Import relevant packages
# =============================================================================
import pandas as pd
import numpy as np
#%%
# =============================================================================
# File Description:
# This file generates descriptive analysis for SP500 Proxy, 
# i.e. for Subsection 2.2 and Table 2 in the thesis
# Main resulting variables:
#   - numberStocksByYear: Table 2 Panel A
#   - spProxy: has three columns
#        + Date
#        + Proxy Return
#        + S&P 500 Composite Index Return from CRSP
#     which is used to generate result for Table 2 Panel B
# The last block of code will display all figures for Table 2 in console
# =============================================================================

#%%
# =============================================================================
# Section 1: Data Processing
# =============================================================================
#Import SP500 data from CRSP into variable sp500
sp500 = pd.read_csv("./data/Indices/[index] SP500 Composite Index Return.csv")
sp500['caldt'] = pd.to_datetime(sp500.caldt, format = "%d/%m/%Y")

#Slide column caldt (calendar date) to month end for easy mapping 
sp500['caldt'] = np.where(sp500.caldt.dt.is_month_end,\
                          sp500.caldt,\
                          sp500.caldt + pd.tseries.offsets.MonthEnd())

# Create a copy of sp_constituents to be used for the analysis in this file
# and name it spProxy
spProxy = sp_constituents.copy(deep = True)

#Keep only observations whose dates exist in sp500
spProxy = spProxy[spProxy.Date.isin(sp500.caldt.unique())]

#Shift column Date to month end 
spProxy['Date'] = np.where(spProxy.Date.dt.is_month_end, spProxy.Date, spProxy.Date + pd.tseries.offsets.MonthEnd())

# Create variable numberStocksByYearBEFORECRSPMAPPING that contains the number of stocks by year before CRSP mapping
numberStocksByYearBEFORECRSPMAPPING = sp_constituents.groupby(sp_constituents.Date.dt.year).permno.nunique().reset_index()



#Import stock return data from CRSP. Note that after removing returns = 'B' 'C', the number of stocks per year reduces
stockData = pd.read_csv("./data/Securities/[stock] CRSP Stock Monthly Return.csv")
stockData['date'] = pd.to_datetime(stockData.date, format = "%d/%m/%Y")
stockData = stockData[['PERMNO', 'date', 'RET', 'RETX']]
stockData = stockData[stockData.date.isin(sp500.caldt)]
stockData = stockData[stockData.RET != 'B']
stockData = stockData[stockData.RET != 'C']
stockData['RET'] = stockData.RET.astype(float)
stockData['RETX'] = stockData.RET.astype(float)
stockData['date'] = np.where(stockData.date.dt.is_month_end, stockData.date, stockData.date + pd.tseries.offsets.MonthEnd())



#Map stock data to spProxy
spProxy = pd.merge(left = spProxy, right = stockData,\
                   left_on = ['Date', 'permno'], \
                   right_on = ['date', 'PERMNO'],\
                   how = 'left', indicator = True)

# Create variable numberStocksByYearAFTERCRSPMAPPING that contains the number of stocks by year after CRSP mapping
numberStocksByYearAFTERCRSPMAPPING = spProxy.groupby(spProxy.Date.dt.year).permno.nunique().reset_index()

# Create variable numberStocksByYear that merge number of stocks by year before and after CRSP mapping
numberStocksByYear = pd.merge(left = numberStocksByYearBEFORECRSPMAPPING, right = numberStocksByYearAFTERCRSPMAPPING,\
                              on = 'Date', how = 'inner')
numberStocksByYear.rename({"permno_x": "Before CRSP", "permno_y": "After CRSP"}, axis = 1, inplace = True)
#Calculate weighted return for each stock using the imported stock data from crsp
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
spProxy = spProxy[['Date', 'Proxy Return', 'S&P 500 Composite Index Return']]
del sp500, numberStocksByYearAFTERCRSPMAPPING, numberStocksByYearBEFORECRSPMAPPING, stockData
#%%
# =============================================================================
# Section 2: Displaying Results in Console
# =============================================================================
print('Number of stocks by year before and after mapping to CRSP:')
print(numberStocksByYear)
print("Descriptive statistics of SP500 Proxy")
print(spProxy['Proxy Return'].describe())
print("Descriptive statistics of SP500 Composite Index Return from CRSP")
print(spProxy['S&P 500 Composite Index Return'].describe())