# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import collections
import os
os.chdir(r"D:\Master's Thesis\Data")
#%%
def calculatingCAR():
    # =============================================================================
    # Prepare CCM Link Table    
    # =============================================================================
    ccm = pd.read_csv("./Securities/[stock] CCM Link Table.csv")
    
    ccm['LINKENDDT'] = np.where(ccm.LINKENDDT == 'E', '20181231', ccm.LINKENDDT)
    ccm['LINKENDDT'] = pd.to_datetime(ccm.LINKENDDT, format = "%Y%m%d")
    ccm['LINKDT'] = pd.to_datetime(ccm.LINKDT, format = "%Y%m%d")
    
    ccm = ccm[ccm.LINKDT != ccm.LINKENDDT]
    
    ccm.drop(['LINKPRIM', 'LINKTYPE', 'LPERMCO', 'LIID'], axis = 1, inplace = True)
    
    ccm = pd.melt(ccm, id_vars = ['gvkey', 'conm', 'LPERMNO'], value_name = 'Date')
    
    ccm.sort_values(['gvkey', 'LPERMNO'], inplace = True)
    
    ccm.set_index('Date', inplace = True)
    
    ccm = ccm.groupby(['gvkey', 'LPERMNO']).resample('D').ffill()
    
    ccm.reset_index(level = [0,1], drop = True, inplace = True)
    
    ccm.reset_index(inplace = True)
    
    ccm.rename({'LPERMNO': 'permno', 'conm':'firmname'}, axis = 1, inplace = True)
    #%%
    print('#############################################')
    print('#############################################')
    print('####### Preprocess Daily Stock Data          ')
    print('#############################################')
    print('#############################################')
    
    #%%
    #Import daily stock data
    dailyStock = pd.read_csv('./Securities/[stock] CRPS - Daily Stock 2001 - 2018.csv')
    
    #%%
    #Removing stocks that show up in the dataset in <=300 rows (equivalent to about 1 years)
    #rowCountByCusip = dailyStock.groupby('CUSIP').count()['date'].sort_values()
    #toberemovedCusips = rowCountByCusip[rowCountByCusip<=300].index
    #dailyStock.drop(dailyStock[dailyStock.CUSIP.isin(toberemovedCusips)].index, axis = 0, inplace = True)
    #del rowCountByCusip, toberemovedCusips
    
    #%%
    #Checking Exchange Code. Make sure all exchange codes = 1, 2 or 3
    #dailyStock = dailyStock[(dailyStock['EXCHCD'] == 1) | (dailyStock['EXCHCD'] == 2) | (dailyStock['EXCHCD'] == 3)]
    #checkEXCHD = True
    #for each in dailyStock['EXCHCD'].unique():
    #    if each not in [1,2,3]:
    #        checkEXCHD = False
    #print('Data contains only stocks from AMEX, NASDAQ, NYSE: ',checkEXCHD)
    #del each
    #del checkEXCHD
    
    #%%
    #Checking Share Code. Make sure all share code = 10 or 11
    #dailyStock = dailyStock[(dailyStock['SHRCD'] == 10) | (dailyStock['SHRCD'] == 11)]
    #checkSHRCD = True
    #for each in dailyStock['SHRCD'].unique():
    #    if each not in [10,11]:
    #        checkSHRCD = False
    #print('Data contains only stocks with Share ode 10 or 11: ',checkSHRCD)
    #del each, checkSHRCD               
    
    #%%
#    print("The number of missing CUSIP values: {}".format(dailyStock['CUSIP'].isnull().sum()))
    
    #%%
    # =============================================================================
    # Preprocess Earning Announcement Data
    # =============================================================================
    
    #Importing data and remove the last letter/digit from earning announcement cusip
    earningAnnouncement = pd.read_csv("./Securities/[stock] Quarterly Earning Announcements.csv")
    earningAnnouncement.cusip = earningAnnouncement.cusip.str.slice(start = 0, stop = 8)
    
    #Since from earning announcement data we are only interested in the stock and the earning announcement dates, we keep only these two variables
    earningAnnouncement = earningAnnouncement[['gvkey', 'cusip','rdq']]
#    earningAnnouncement = earningAnnouncement[['cusip','rdq']]
    earningAnnouncement = earningAnnouncement[~earningAnnouncement.rdq.isnull()]
    
    #%%
    #Preliminary look to see how many date matches from daily stock data and earning announcement
#    stockDates = dailyStock.date.unique()
#    earningDates = earningAnnouncement.rdq.unique()
#    stockDates = list(stockDates)
#    earningDates = list(earningDates)
#    stockDates = collections.Counter(stockDates)
#    earningDates = collections.Counter(earningDates)
#    print('The number of unique dates from daily stock data: ',dailyStock.date.nunique())
#    print('The number of unique reported dates from earning announcement data: ',earningAnnouncement.rdq.nunique())
#    print('The number of overlapping dates from two datasets: ',len(list((stockDates & earningDates).elements())))
#    del stockDates, earningDates
    #%%
    #Test
    earningAnnouncement['rdq'] = pd.to_datetime(earningAnnouncement.rdq, format = '%d/%m/%Y')
    earningAnnouncement = pd.merge(left = earningAnnouncement, right = ccm,\
                                   left_on = ['rdq', 'gvkey'], right_on = ['Date', 'gvkey'],\
                                   how = 'left')
    earningAnnouncement.drop(['gvkey', 'Date', 'variable'], axis = 1, inplace = True)
    #%%
    #Preliminary look to see how many permno matches from daily stock data and earning announcement
#    stockPermno = collections.Counter(list(dailyStock.PERMNO.unique()))
#    earningPermno = collections.Counter(list(earningAnnouncement.permno.unique()))
#    print('The number of cusip from daily stock data: ',dailyStock.PERMNO.nunique())
#    print('The number of cusip from earning announcement data: ',earningAnnouncement.permno.nunique())
#    print('The number of overlapping cusip from two datasets: ',len(list((stockPermno & earningPermno).elements())))
#    del stockPermno, earningPermno
    #%%
    #Preliminary look to see how many cusip matches from daily stock data and earning announcement
#    stockCusip = collections.Counter(list(dailyStock.CUSIP.unique()))
#    earningCusip = collections.Counter(list(earningAnnouncement.cusip.unique()))
#    print('The number of cusip from daily stock data: ',dailyStock.CUSIP.nunique())
#    print('The number of cusip from earning announcement data: ',earningAnnouncement.cusip.nunique())
#    print('The number of overlapping cusip from two datasets: ',len(list((stockCusip & earningCusip).elements())))
#    del stockCusip, earningCusip
    
    #%%
    # =============================================================================
    # Merging stock data with earning announcement
    # =============================================================================
    #Merge daily stock data with earning announcement data. Run this if there is enough RAM
    dailyStock['date'] = pd.to_datetime(dailyStock.date, format = "%d/%m/%Y")
    merged = pd.merge(left = dailyStock, right = earningAnnouncement, how = 'left', left_on = ['CUSIP', 'date'], right_on = ['cusip', 'rdq'], indicator = True)
    
    merged = pd.merge(left = merged, right = earningAnnouncement, how = 'left', left_on = ['PERMNO', 'date'], right_on = ['permno', 'rdq'])
    
    merged['rdq_y'] = np.where(merged.rdq_y.isnull()&merged.rdq_x.notnull(),\
                              merged.rdq_x, merged.rdq_y)
    merged.rename({'rdq_y': 'rdq'}, axis = 1, inplace = True)
    
    #%%
#        #Split the dailyStock into four pieces since RAM can't handle the whole at once
#        onequarter = dailyStock.shape[0]//4
#        dailyStock_firstQuarter = dailyStock[:onequarter]
#        dailyStock_secondQuarter = dailyStock[onequarter:(onequarter*2)]
#        dailyStock_thirdQuarter = dailyStock[(onequarter*2):(onequarter*3)]
#        dailyStock_fourthQuarter = dailyStock[(onequarter*3):]
#        del onequarter, dailyStock
    
    #%%
#        #Merge daily stock data with earning announcement. Merging is done by (cusip, date)
#        merged1 = pd.merge(left = dailyStock_firstQuarter, right = earningAnnouncement, how = 'left', left_on = ['CUSIP', 'date'], right_on = ['cusip', 'rdq'], indicator = True, copy = False)
#        del dailyStock_firstQuarter
#        merged2 = pd.merge(left = dailyStock_secondQuarter, right = earningAnnouncement, how = 'left', left_on = ['CUSIP', 'date'], right_on = ['cusip', 'rdq'], indicator = True, copy = False)
#        del dailyStock_secondQuarter
#        
#        #%%
#        merged3 = pd.merge(left = dailyStock_thirdQuarter, right = earningAnnouncement, how = 'left', left_on = ['CUSIP', 'date'], right_on = ['cusip', 'rdq'], indicator = True, copy = False)
#        del dailyStock_thirdQuarter
#        merged4 = pd.merge(left = dailyStock_fourthQuarter, right = earningAnnouncement, how = 'left', left_on = ['CUSIP', 'date'], right_on = ['cusip', 'rdq'], indicator = True, copy = False)
#        del dailyStock_fourthQuarter
#        
#        #%%
#        del earningAnnouncement
    
    #%%
    #Keeping only crucial variables
    merged = merged[['date', 'PERMNO', 'CUSIP', 'COMNAM', 'PRC', 'VOL', 'RET', 'SHROUT', 'RETX', 'rdq']]
#        merged1 = merged1[['date', 'PERMNO', 'CUSIP', 'COMNAM', 'PRC', 'VOL', 'RET', 'SHROUT', 'RETX', 'rdq']]
#        merged2 = merged2[['date', 'PERMNO', 'CUSIP', 'COMNAM', 'PRC', 'VOL', 'RET', 'SHROUT', 'RETX', 'rdq']]
#        merged3 = merged3[['date', 'PERMNO', 'CUSIP', 'COMNAM', 'PRC', 'VOL', 'RET', 'SHROUT', 'RETX', 'rdq']]
#        merged4 = merged4[['date', 'PERMNO', 'CUSIP', 'COMNAM', 'PRC', 'VOL', 'RET', 'SHROUT', 'RETX', 'rdq']]
#        
    #%%
    merged['window'] = pd.Series((np.where(~merged.groupby('PERMNO')['rdq'].shift(0).isnull(), True, False))|(np.where(~merged.groupby('PERMNO')['rdq'].shift(+1).isnull(), True, False))|(np.where(~merged.groupby('PERMNO')['rdq'].shift(-1).isnull(), True, False)))
#        merged1['window'] = pd.Series((np.where(~merged1.groupby('CUSIP')['rdq'].shift(0).isnull(), True, False))|(np.where(~merged1.groupby('CUSIP')['rdq'].shift(+1).isnull(), True, False))|(np.where(~merged1.groupby('CUSIP')['rdq'].shift(-1).isnull(), True, False)))
#        merged2['window'] = pd.Series((np.where(~merged2.groupby('CUSIP')['rdq'].shift(0).isnull(), True, False))|(np.where(~merged2.groupby('CUSIP')['rdq'].shift(+1).isnull(), True, False))|(np.where(~merged2.groupby('CUSIP')['rdq'].shift(-1).isnull(), True, False)))
#        merged3['window'] = pd.Series((np.where(~merged3.groupby('CUSIP')['rdq'].shift(0).isnull(), True, False))|(np.where(~merged3.groupby('CUSIP')['rdq'].shift(+1).isnull(), True, False))|(np.where(~merged3.groupby('CUSIP')['rdq'].shift(-1).isnull(), True, False)))
#        merged4['window'] = pd.Series((np.where(~merged4.groupby('CUSIP')['rdq'].shift(0).isnull(), True, False))|(np.where(~merged4.groupby('CUSIP')['rdq'].shift(+1).isnull(), True, False))|(np.where(~merged4.groupby('CUSIP')['rdq'].shift(-1).isnull(), True, False)))
#        
    #%%
    #This code: remove duplicates
    merged.drop_duplicates(['date','PERMNO'], keep = 'first', inplace = True)
#        merged1.drop_duplicates(['date','CUSIP'], keep = 'first', inplace = True)
#        merged2.drop_duplicates(['date','CUSIP'], keep = 'first', inplace = True)
#        merged3.drop_duplicates(['date','CUSIP'], keep = 'first', inplace = True)
#        merged4.drop_duplicates(['date','CUSIP'], keep = 'first', inplace = True)
    #%%
    #This code: combining the four pieces of code into one whole dataframe
#        merged = pd.concat([merged1, merged2, merged3, merged4], axis = 0, copy = False)
#        del merged1, merged2, merged3, merged4
    #%%
    print('The number of non-missing rdq multiplied by 3: ',3*np.sum(~merged.rdq.isnull()))
    print('The number of rows in windows: ',np.sum(merged.window))
    print('Difference: ',np.sum(merged.window)-np.sum(~merged.rdq.isnull())*3)
    print('The difference does not have to be 0, but should not be too big.')
    #%%
    #Extract only the rows in the windows around earning announcement dates
    merged = merged[merged.window]
    #%%
    #Fill 'rdq' column such that within the same window, 'rdq' contains the same date
    backwardShift = merged.groupby('PERMNO')['rdq'].shift(-1)
    forwardShift = merged.groupby('PERMNO')['rdq'].shift(+1)
    merged['rdq'].fillna(backwardShift, inplace = True)
    merged['rdq'].fillna(forwardShift, inplace = True)
    del backwardShift, forwardShift
    #%%
    #This block: cleaning data so far
    
    ##Drop rows that have missing values in all PRC, RET, RETX
    tempDrop = merged[(merged.PRC.isnull())&(merged.RET.isnull())&(merged.RETX.isnull())].index
    merged.drop(tempDrop, axis = 0, inplace = True)
    
    ##Drop rows that have RET in code C or B
    merged.drop(merged[merged.RET.isin(['C', 'B'])].index, axis = 0, inplace = True)
    
    ##Convert RET and RETX to float
    merged['RET'] = merged['RET'].astype(float) 
    merged['RETX'] = merged['RETX'].astype(float)
    
    ##Drop cusip that have <= 12 rows (equivalent to staying in the dataset for <= 1 year)
#    rowCountByCusip = merged.groupby('CUSIP').count()['date'].sort_values() #this variable shows that the number of rows per cusip varies from 1 row per cusip to maximum 288 rows per cusip
#    listOfCusipTobeRemoved = list(rowCountByCusip[rowCountByCusip<12].index)
#    merged.drop(merged[merged.CUSIP.isin(listOfCusipTobeRemoved)].index, axis = 0, inplace = True)
#    del rowCountByCusip, listOfCusipTobeRemoved
    #%%
    merged.reset_index(drop = True, inplace = True)
    #%%
    # =============================================================================
    # Merging (daily stock + earning announcement) with SP500 data      
    # =============================================================================
    
    #Importing data and keep crucial variables
    sp500DF = pd.read_csv("./Indices/[index] SP500 Daily 1984-2018.csv")
    sp500DF = sp500DF[['caldt', 'sprtrn']]
    sp500DF['caldt'] = pd.to_datetime(sp500DF.caldt, format = "%d/%m/%Y")
    #%%
    mergedStockEASP500 = pd.merge(left = merged, right = sp500DF, how = 'left', left_on = 'date', right_on='caldt', indicator = True, copy = False)
    print('There is no non-matching date from SP500 data: ',len(mergedStockEASP500._merge.unique())==1)
    mergedStockEASP500.drop(['_merge', 'caldt'], axis = 1, inplace = True)
    del merged, sp500DF
    #%%
    #Calculate CAR
    mergedStockEASP500['AR'] = mergedStockEASP500.RET - mergedStockEASP500.sprtrn
    temp_ThreeDayCAR = mergedStockEASP500.groupby(['PERMNO', 'rdq'])['AR'].sum()
    temp_ThreeDayCAR = temp_ThreeDayCAR.reset_index()
    temp_ThreeDayCAR.rename({'AR': 'CAR'}, axis = 1, inplace = True)
    #%%
    threeDayCARDf = pd.merge(left = mergedStockEASP500, right = temp_ThreeDayCAR, how ='left', on = ['PERMNO', 'rdq'], sort = 'PERMNO', indicator = True, copy = False)
    print('There is no non-matching CAR: ',len(threeDayCARDf._merge.unique())==1)
    del temp_ThreeDayCAR, mergedStockEASP500
    threeDayCARDf = threeDayCARDf[['date', 'PERMNO', 'CUSIP', 'COMNAM', 'rdq', 'RET', 'sprtrn', 'CAR']]
#    threeDayCARDf['date'] = pd.to_datetime(threeDayCARDf.date.str.slice(start = 3), format = '%m/%Y')
    
    return threeDayCARDf
    #%%

