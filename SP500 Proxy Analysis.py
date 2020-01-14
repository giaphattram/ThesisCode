# -*- coding: utf-8 -*-
"""
Created on Sun Mar 24 10:44:52 2019

@author: Admin
"""
import pandas as pd
import numpy as np
#%%
exec(compile(open("D:\Master's Thesis\Code\ComputingIndexWeights.py", 'rb').read(), 'ComputingIndexWeights.py', 'exec'))
#sp_constituents = ComputingIndexWeights()
sp_constituents = ComputingIndexWeights()
crspStock = pd.read_csv("./Securities/[stock] CRPS - Daily Stock 2001 - 2018.csv")
compustatcrspmapping = pd.read_excel("./Securities/[stock] [handmade] CompustatCRSPMapping.xlsx")

#%%
sp_constituents['iid'] = sp_constituents.iid.astype(int)
sp_constituents = pd.merge(left = sp_constituents, right = compustatcrspmapping,\
                           left_on = ['gvkey', 'iid'], how = 'left',\
                           right_on = ['compustat_gvkey', 'compustat_iid'])
#%%
crspStock['date'] = pd.to_datetime(crspStock.date, format = "%d/%m/%Y")
crspStock = crspStock[['date', 'CUSIP', 'RET', 'RETX']]
sp_constituents = pd.merge(left = sp_constituents, right = crspStock,\
                           left_on = ['Date', 'crsp_cusip'], right_on = ['date', 'CUSIP'],
                           how = 'left')
sp_constituents = sp_constituents[['Date',  'conm', 'co_conm', 'constituent_weight',  'crsp_cusip', \
                    'RET', 'RETX']]
sp_constituents = sp_constituents.drop_duplicates(['Date', 'crsp_cusip'], keep = 'first')
sp_constituents = sp_constituents[sp_constituents.crsp_cusip.notnull()]
#%%
# Test if there are unique cusips for each date.
# Answer: no, but I have cleaned the data such that there are unique cusips now
gbObj1 = sp_constituents.groupby('Date').crsp_cusip.nunique()
uniqueGb = gbObj1.to_frame().reset_index()
gbObj2 = sp_constituents.groupby('Date').co_conm.count()
countGb = gbObj2.to_frame().reset_index()
gbObj = pd.merge(left = countGb, right = uniqueGb, on = 'Date', how = 'inner')
del gbObj1, gbObj2
del uniqueGb, countGb
gbObj['compare'] = gbObj.co_conm == gbObj.crsp_cusip
print('After cleaning up, there are now only unique cusips every day: {}'.\
      format(gbObj.compare.nunique() == 1))
dailyCusipCount = gbObj.copy(deep = True)
dailyCusipCount = dailyCusipCount[['Date', 'crsp_cusip']]
dailyCusipCount.rename({'crsp_cusip': 'unique_cusip_count'}, axis = 1, inplace = True)
#del gbObj

#%%
sp_constituents = sp_constituents[~sp_constituents.RET.isin(['C','B'])]
sp_constituents = sp_constituents[sp_constituents.RET.notnull()]
#sp_constituents = sp_constituents[~sp_constituents.RETX.isin(['C','B'])]
#sp_constituents = sp_constituents[sp_constituents.RETX.notnull()]
sp_constituents['RET'] = sp_constituents.RET.astype(float)
#sp_constituents['RETX'] = sp_constituents.RET.astype(float)

#%%
sp_constituents['weighted_ret'] = sp_constituents.constituent_weight * sp_constituents.RET
#sp_constituents['weighted_retx'] = sp_constituents.constituent_weight * sp_constituents.RETX
retResult = sp_constituents.groupby('Date').weighted_ret.sum()
retResult = retResult.reset_index()
#retxResult = sp_constituents.groupby('Date').weighted_retx.sum()
#retxResult = retxResult.reset_index()
#result = pd.merge(left = retResult, right = retxResult,\
                  on = 'Date', how = 'inner')
#del retResult, retxResult

#%%
sp500 = pd.read_csv("./Indices/[index] SP500 Composite Index Return.csv")
sp500['caldt'] = pd.to_datetime(sp500.caldt, format = "%d/%m/%Y")

result = pd.merge(left = result, right = sp500,\
                           left_on = 'Date', right_on = 'caldt', how = 'left')