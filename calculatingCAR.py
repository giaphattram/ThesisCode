##%%
## =============================================================================
## To do: Change the directory variable to the location of
## the folder "Thesis Submission_GiaPhatTram" 
## For example: E:\Students\Thesis\Thesis Submission - Gia Phat Tram
## =============================================================================
#import os
#directory = r"D:\Thesis Submission - Gia Phat Tram"
#os.chdir(directory)
#%%
# =============================================================================
# File Description:
# This file contains one function that returns a variable that contains the three-day CAR for each stock in the dataset
# Key columns in this variable:
#   - date
#   - permno: stock identifier
#   - rdq: reported date quarterly
#   - CAR: three-day CAR around rdq 
#   - slided_rdq: sliding rdq to the end of the previous quarter - to allow for mapping
#             to fund holding data to calculate AFP 
# =============================================================================
#%%
# =============================================================================
# Import relevant libraries
# =============================================================================
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import collections
#%%
def calculatingCAR():
    # =============================================================================
    # Prepare CCM Link Table which allows for mapping stock data from CRSP
    # to earnings announcements from Compustat
    # =============================================================================
    ccm = pd.read_csv("./Data/Securities/[stock] CCM Link Table.csv")
    
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
    # =============================================================================
    # Preprocess Earning Announcement Data from Compustat
    # Resulting variable: earningAnnouncement
    # =============================================================================
    
    #Importing earnings announcements data from Compustat
    earningAnnouncement = pd.read_csv("./Data/Securities/[stock] Quarterly Earning Announcements.csv")
    
    #CRSP drops the 9th (checksum) digit in CUSIPs and keeps historical CUSIPs, 
    #while Compustat uses the full, 9-digit CUSIP and keeps only the most current CUSIP
    #https://wrds-www.wharton.upenn.edu/pages/support/applications/linking-databases/linking-crsp-and-compustat/
    #Therefore, drops the last digit/letter of the cusip from compustat
    earningAnnouncement.cusip = earningAnnouncement.cusip.str.slice(start = 0, stop = 8)
    
    #Since from earning announcement data we are only interested in the stock and the earning announcement dates, 
    # we keep only these three variables: company/stock identifier (gvkey, cusip) and announcemnet dates (rdq)
    earningAnnouncement = earningAnnouncement[['gvkey', 'cusip','rdq']]
    
    #Remove observations with missing rdq
    earningAnnouncement = earningAnnouncement[~earningAnnouncement.rdq.isnull()]
    
    #Format the column rdq from string to datetime
    earningAnnouncement['rdq'] = pd.to_datetime(earningAnnouncement.rdq, format = '%d/%m/%Y')
    
    # Map permno from CCM Table to variable earningAnnouncement
    earningAnnouncement = pd.merge(left = earningAnnouncement, right = ccm,\
                                   left_on = ['rdq', 'gvkey'], right_on = ['Date', 'gvkey'],\
                                   how = 'left')
    
    # Drop variables that will not be used hereafter
    earningAnnouncement.drop(['gvkey', 'Date', 'variable'], axis = 1, inplace = True)
    
    #%%
    # =============================================================================
    # Merging stock data with earning announcement, resulting in variable merged
    # =============================================================================
    #Import daily stock data from CRSP
    dailyStock = pd.read_csv('./Data/Securities/[stock] CRPS - Daily Stock 2001 - 2018.csv')
 
    #Format the date variable from string to datetime
    dailyStock['date'] = pd.to_datetime(dailyStock.date, format = "%d/%m/%Y")
    
    #Map daily stock data to earning announcement data using CUSIP. Warning: RAM-consuming
    merged = pd.merge(left = dailyStock, right = earningAnnouncement, how = 'left', left_on = ['CUSIP', 'date'], right_on = ['cusip', 'rdq'], indicator = True)

    #Map daily stock data to earning announcement data using permno     
    merged = pd.merge(left = merged, right = earningAnnouncement, how = 'left', left_on = ['PERMNO', 'date'], right_on = ['permno', 'rdq'])
    
    # We use CCM Linking Table to map stock data to earning announcement
    # CCM mapped permno to earningAnnouncement, which in turn is used to map to stock data from CRSP
    # However, some observations can fail to be mapped
    # Therefore, I map stock data to earningAnnouncement a second time for the observations 
    # that fail to be mapped by permno  
    merged['rdq_y'] = np.where(merged.rdq_y.isnull()&merged.rdq_x.notnull(),\
                              merged.rdq_x, merged.rdq_y)
    merged.rename({'rdq_y': 'rdq'}, axis = 1, inplace = True)
      
    #Keeping only crucial variables
    merged = merged[['date', 'PERMNO', 'CUSIP', 'COMNAM', 'PRC', 'VOL', 'RET', 'SHROUT', 'RETX', 'rdq']]      
    
    #Create a column named "window" that has value 1 if the observations are of a three-day window around an earnings announcement
    merged['window'] = pd.Series((np.where(~merged.groupby('PERMNO')['rdq'].shift(0).isnull(), True, False))|(np.where(~merged.groupby('PERMNO')['rdq'].shift(+1).isnull(), True, False))|(np.where(~merged.groupby('PERMNO')['rdq'].shift(-1).isnull(), True, False)))
  
    #remove duplicates
    merged.drop_duplicates(['date','PERMNO'], keep = 'first', inplace = True)

    #Only keep observations in the windows around earning announcement dates
    merged = merged[merged.window]
    
    #Fill 'rdq' column such that within the same window, 'rdq' contains the same date
    backwardShift = merged.groupby('PERMNO')['rdq'].shift(-1)
    forwardShift = merged.groupby('PERMNO')['rdq'].shift(+1)
    merged['rdq'].fillna(backwardShift, inplace = True)
    merged['rdq'].fillna(forwardShift, inplace = True)
    del backwardShift, forwardShift
    #%%
    # =============================================================================
    # Cleaning variable merged
    # =============================================================================
  
    ##Drop rows that have missing values in all PRC, RET, RETX
    tempDrop = merged[(merged.PRC.isnull())&(merged.RET.isnull())&(merged.RETX.isnull())].index
    merged.drop(tempDrop, axis = 0, inplace = True)
    
    ##Drop rows that have RET in code C or B
    merged.drop(merged[merged.RET.isin(['C', 'B'])].index, axis = 0, inplace = True)
    
    ##Convert RET and RETX to float
    merged['RET'] = merged['RET'].astype(float) 
    merged['RETX'] = merged['RETX'].astype(float)
    
    # Reset index so that the index would start from 0
    merged.reset_index(drop = True, inplace = True)
    #%%
    # =============================================================================
    # Merging variable merged (daily stock + earning announcement) with SP500 data      
    # =============================================================================    
    #Importing sp500 data and keep only date and return
    sp500DF = pd.read_csv("./Data/Indices/[index] SP500 Daily 1984-2018.csv")
    sp500DF = sp500DF[['caldt', 'sprtrn']]
    sp500DF['caldt'] = pd.to_datetime(sp500DF.caldt, format = "%d/%m/%Y")
    
    #Merge sp500 return to the merged variable, resulting in variable mergedStockEASP500
    mergedStockEASP500 = pd.merge(left = merged, right = sp500DF, how = 'left', left_on = 'date', right_on='caldt', indicator = True, copy = False)
    print('There is no non-matching date from SP500 data: ',len(mergedStockEASP500._merge.unique())==1)
    mergedStockEASP500.drop(['_merge', 'caldt'], axis = 1, inplace = True)
    del merged, sp500DF
    
    #Calculate CAR in a temporary variable temp_ThreeDayCAR
    mergedStockEASP500['AR'] = mergedStockEASP500.RET - mergedStockEASP500.sprtrn
    temp_ThreeDayCAR = mergedStockEASP500.groupby(['PERMNO', 'rdq'])['AR'].sum()
    temp_ThreeDayCAR = temp_ThreeDayCAR.reset_index()
    temp_ThreeDayCAR.rename({'AR': 'CAR'}, axis = 1, inplace = True)
    
    #Merge 3-day CAR from temp_ThreeDayCAR to mergedStockEASP500, resulting in the variable threeDayCARDf
    threeDayCARDf = pd.merge(left = mergedStockEASP500, right = temp_ThreeDayCAR, how ='left', on = ['PERMNO', 'rdq'], sort = 'PERMNO', indicator = True, copy = False)
    print('There is no non-matching CAR: ',len(threeDayCARDf._merge.unique())==1)
    del temp_ThreeDayCAR, mergedStockEASP500
    
    #Keep only essential columns
    threeDayCARDf = threeDayCARDf[['date', 'PERMNO', 'CUSIP', 'COMNAM', 'rdq', 'RET', 'sprtrn', 'CAR']]
    
    #%%    
    # Return the variable threeDayCARDf whenever this function is called
    return threeDayCARDf

