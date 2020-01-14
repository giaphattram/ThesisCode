import pandas as pd
pd.set_option('display.max_columns', 30)
import numpy as np
import os
import datetime
from datetime import date
os.chdir("D:\Master's Thesis\Data")
os.getcwd()
#%%
def ComputingIndexWeights():
    index_constituents = pd.read_csv("./Indices/[index] Compustat Index Constituents 2001-2019.csv", dtype = {'iid': str})
    
    #filter out S&P 500 constituents following this rule
    #"After December 1994, select just SPMIM=10 to point to S&P500 constituents." from this article
    #https://wrds-www.wharton.upenn.edu/pages/support/applications/programming-examples-and-other-topics/sp-500-datasets-and-constituents/
    sp_constituents = index_constituents.copy(deep = True)
    sp_constituents = sp_constituents[sp_constituents.spmi == 10]
    
    #CRSP drops the 9th (checksum) digit in CUSIPs and keeps historical CUSIPs, 
    #while Compustat uses the full, 9-digit CUSIP and keeps only the most current CUSIP
    #https://wrds-www.wharton.upenn.edu/pages/support/applications/linking-databases/linking-crsp-and-compustat/
    sp_constituents['CUSIP'] = sp_constituents.co_cusip.str.slice(start = 0, stop = 8, step = 1)
    sp_constituents['CUSIP'] = sp_constituents.CUSIP.astype(str)
    sp_constituents['from'] = pd.to_datetime(sp_constituents['from'], format = "%d/%m/%Y")
    sp_constituents['thru'] = pd.to_datetime(sp_constituents['thru'], format = "%d/%m/%Y")
    del index_constituents
    
    #Remove any constituents whose thru <= "2001-01-01", i.e. having exit date = first date of the sample period
    sp_constituents = sp_constituents[(sp_constituents.thru > "2001-01-01") | (sp_constituents.thru.isnull())]
    
    ###unique columns:
    #['gvkey', 'iid', 'gvkeyx', 'from', 'thru', 'conm', 'indextype', 'tic',
    #       'spii', 'spmi', 'indexcat', 'co_conm', 'co_tic', 'co_cusip', 'co_cik',
    #       'co_sic', 'co_naics'],
    
    
    #%%
    stockData = pd.read_csv("./Securities/[stock] Compustat - Daily Stock 2001 - 2018.csv", dtype = {'iid': str})
    stockData['datadate'] = pd.to_datetime(stockData.datadate, format = "%d/%m/%Y")
    #%%
#    print("Checking --")
#    spCusip = set(sp_constituents.co_cusip.unique())
#    stockCusip = set(stockData.cusip.unique())
#    print("len(spCusip): {}".format(len(spCusip)))
#    print("len(spCusip.intersection(stockCusip)): {}".format(len(spCusip.intersection(stockCusip))))
#    print("The cusip that appear in sp_constituents but not stockCusip: {}".format(spCusip.difference(stockCusip)))
    # result: two gvkey-iid pairs have missing cusip: 24609-01 and 14489-02
    
    # 14489-02 exists in stockData. conm = "DELL TECHNOLOGIES INC", cusip = "24703L103"
    # Fill in conm and cusip for this pair in sp_constituents
    tempIndex = sp_constituents[(sp_constituents.gvkey == 14489)&(sp_constituents.iid == '02')]
    tempIndex = tempIndex[tempIndex.co_cusip.isnull()].index
    sp_constituents.at[tempIndex, 'co_cusip'] = "24703L103"
    sp_constituents.at[tempIndex, 'conm'] = "DELL TECHNOLOGIES INC"
    del tempIndex
    
    # 24609-01 does not exist in stockData. Plus, it enters and exits sp_constituents on the same day => remove
    tempIndex = sp_constituents[(sp_constituents.gvkey == 24609)&(sp_constituents.iid == '01')]
    if (len(tempIndex) == 1) & (tempIndex['from'].iloc[0] == tempIndex.thru.iloc[0]):
        sp_constituents.drop(tempIndex.index, axis = 0, inplace = True)
#    print("Rechecking --")
#    spCusip = set(sp_constituents.co_cusip.unique())
#    stockCusip = set(stockData.cusip.unique())
#    print("len(spCusip): {}".format(len(spCusip)))
#    print("len(spCusip.intersection(stockCusip)): {}".format(len(spCusip.intersection(stockCusip))))
#    print("All Cusips that appear in sp_constituents now also appear in stockdata: {}".format(len(spCusip.difference(stockCusip))==0))
    del tempIndex
    
    #%%
    # =============================================================================
    # Removing variables before transformation
    # Fill in '31/12/2018' for missing 'thru'
    # =============================================================================
    #%%
#    print("From the result above, CUSIP is sufficient to map sp_constituents to stockData")
#    print("\t => Keep co_cusip and co_conm, remove other stock identifiers")
#    print("Unique index type: {}".format(sp_constituents.indextype.unique()))
#    print("Unique spmi: {}".format(sp_constituents.spmi.unique()))
#    print("Unique tic: {}".format(sp_constituents.tic.unique()))
#    print("Unique indexcat: {}".format(sp_constituents.indexcat.unique()))
#    print("Unique indexcat: {}".format(sp_constituents.spii.unique()))
#    print("Unique gvkeyx: {}".format(sp_constituents.gvkeyx.unique()))
#    print("if there is only one index type, spmi and tic, then the data is strictly SP500")
#    print("Removing these variables and other security identifier...")
    sp_constituents.drop(["indextype", "spmi", "tic", "spii", "gvkeyx", "indexcat", "co_cik", "co_sic", "co_naics", "CUSIP"], axis = 1, inplace = True)
    
    #Fill the missing 'thru' date with '31/08/2018'
    emptyThru = sp_constituents[sp_constituents.thru.isnull()]
    sp_constituents.at[emptyThru.index, 'thru'] = pd.to_datetime('31/12/2018', format = "%d/%m/%Y")
    
    del emptyThru
    
    #%%
    # =============================================================================
    # Transforming
    # =============================================================================
    #sp_constituents = sp_constituents.copy(deep = True)
    
    sp_constituents = pd.melt(sp_constituents, id_vars = ['gvkey', 'iid', 'conm', 'co_conm', 'co_cusip', 'co_tic'], \
                               value_name = 'Date') #value_vars = ['from', 'thru'], 
    sp_constituents.drop('variable', axis = 1, inplace = True)
    
    sp_constituents.sort_values(['co_cusip', 'Date'], inplace = True)
    
    sp_constituents['Date'] = pd.to_datetime(sp_constituents.Date)
    
    sp_constituents.set_index('Date', inplace = True)
    
    sp_constituents = sp_constituents.groupby(['gvkey', 'iid', 'conm', 'co_conm', 'co_cusip', 'co_tic']).resample('D').ffill()
    
    sp_constituents = sp_constituents.reset_index(level = [0,1,2,3,4,5], drop = True)
    
    sp_constituents = sp_constituents.reset_index()
    
    #%%
    # =============================================================================
    # Test whether cusip and company names are mapped between 
    # Compustat Index Constituents and Compustat Stock Data
    # Generate variable "compustatIndexStockMapping"
    # =============================================================================
#    testConstituents = sp_constituents.copy(deep = True)
#    testConstituents = testConstituents[['co_cusip', 'co_conm']]
#    testConstituents .drop_duplicates(keep = 'first', inplace = True)
#    testStocks = stockData.copy(deep = True)
#    testStocks = testStocks[['cusip', 'conm']]
#    testStocks.drop_duplicates(keep = 'first', inplace = True)
#    compustatIndexStockMapping = pd.merge(left = testConstituents, right = testStocks,\
#                                   left_on = 'co_cusip', right_on = 'cusip',  
#                                   how = 'left') 
#                        #_merge should only have "both" since cusip \
#                        #is used to merge, and cusip has been checked above
#    
#    compustatIndexStockMapping['compare_conm'] = (compustatIndexStockMapping.co_conm == compustatIndexStockMapping.conm)
##    print('unique cusip and conm are mapped successfully between Compustat sp_constituents and Compustat Stock data: '.format(compustatIndexStockMapping.compare_conm.nunique()==1))
#    
#    del testConstituents, testStocks
#    del compustatIndexStockMapping
    #%%
    stockData = stockData[['datadate', 'gvkey', 'iid', 'cusip', 'cshoc', 'prccd', 'exchg']]
    
    stockData['market_val'] = stockData.cshoc * stockData.prccd
    
    
    #%%
    sp_constituents = pd.merge(left = sp_constituents, right = stockData,\
             left_on = ['Date', 'gvkey', 'iid'], right_on = ['datadate', 'gvkey', 'iid'],\
             how = 'left')
    sp_constituents.drop('datadate', axis=1, inplace = True)
    
    print('Unique exchang codes after merging: {}'.format(sp_constituents.exchg.unique()))
    #Remove any exchg outside the US: 0, 7
    sp_constituents = sp_constituents[~sp_constituents.exchg.isin([0,7])]
    
    #Remove rows before 2001-01-01 and after 2018-12-21
    sp_constituents = sp_constituents[(sp_constituents.Date >= '2001-01-01')&(sp_constituents.Date <= '2018-12-31')]
    
    
    #%%
    sp_constituents.at[sp_constituents[(sp_constituents.gvkey == 33439) \
                                       & (sp_constituents.iid == '01') \
                                       & (sp_constituents.Date == '2018-06-01')].index,\
                                        'cshoc']\
        = stockData[(stockData.gvkey == 33439) \
                                       & (stockData.iid == '01') \
                                       & (stockData.datadate == '2018-06-05')].cshoc.iloc[0]
                    
    sp_constituents.at[sp_constituents[(sp_constituents.gvkey == 33439) \
                                       & (sp_constituents.iid == '01') \
                                       & (sp_constituents.Date == '2018-06-01')].index,\
                                        'market_val']\
        = sp_constituents[(sp_constituents.gvkey == 33439) \
                                       & (sp_constituents.iid == '01') \
                                       & (sp_constituents.Date == '2018-06-01')].cshoc.iloc[0] *\
          sp_constituents[(sp_constituents.gvkey == 33439) \
                       & (sp_constituents.iid == '01') \
                       & (sp_constituents.Date == '2018-06-01')].prccd.iloc[0]
                          
#    sp_constituents.at[sp_constituents[(sp_constituents.gvkey == 33439) \
#                                       & (sp_constituents.iid == '01') \
#                                       & (sp_constituents.Date == '2018-06-01')].index,\
#                                        'constituent_weight']\
#        = sp_constituents[(sp_constituents.gvkey == 33439) \
#                                       & (sp_constituents.iid == '01') \
#                                       & (sp_constituents.Date == '2018-06-01')].market_val.iloc[0] *\
#          sp_constituents[(sp_constituents.gvkey == 33439) \
#                       & (sp_constituents.iid == '01') \
#                       & (sp_constituents.Date == '2018-06-01')].total_market_val.iloc[0]
    
    gbObj = sp_constituents.groupby('Date').market_val.sum()
    gbObj = gbObj.to_frame().reset_index()
    gbObj.rename({'market_val': 'total_market_val'}, axis =1, inplace = True)
    sp_constituents = pd.merge(left = sp_constituents, right = gbObj, \
                               on = 'Date', how = 'left')
    del gbObj
    
    #Remove rows where total_market_val = 0, these are non-trading days
    sp_constituents = sp_constituents[sp_constituents.total_market_val != 0]
    
    #Calculate weights
    sp_constituents['constituent_weight'] = sp_constituents.market_val / sp_constituents.total_market_val
    #%%
    #Sometimes constituent_weight is not calculated, so use the earliest or latest weight 
    ffilled_weight = sp_constituents.groupby(['gvkey', 'iid']).constituent_weight.ffill()
    ffilled_weight.rename('ffilled_weight', inplace = True)
    sp_constituents = pd.concat([sp_constituents, ffilled_weight], axis = 1)
    sp_constituents['constituent_weight'] = np.where(sp_constituents.constituent_weight.isnull(),\
                                                     sp_constituents.ffilled_weight,\
                                                     sp_constituents.constituent_weight)
    bfilled_weight = sp_constituents.groupby(['gvkey', 'iid']).constituent_weight.bfill()
    bfilled_weight.rename("bfilled_weight", inplace = True)
    sp_constituents = pd.concat([sp_constituents, bfilled_weight], axis = 1)
    sp_constituents['constituent_weight'] = np.where(sp_constituents.constituent_weight.isnull(),\
                                                     sp_constituents.bfilled_weight,\
                                                     sp_constituents.constituent_weight)
    
    #%%
    #Keep only essential columns, ready to be merged to CRSP Mutual Funds data
    sp_constituents = sp_constituents[['Date', 'gvkey', 'iid', 'conm', 'co_conm',\
                                       'cusip', 'co_tic', 'constituent_weight']]
    #%%
    #Map CRSP's permno
    ccm = pd.read_csv("./Securities/[stock] CCM Link Table.csv")
    
    ccm['LINKENDDT'] = np.where(ccm.LINKENDDT == 'E', '20181231', ccm.LINKENDDT)
    
    ccm['LINKENDDT'] = pd.to_datetime(ccm.LINKENDDT, format = "%Y%m%d")
    ccm['LINKDT'] = pd.to_datetime(ccm.LINKDT, format = "%Y%m%d")
    
    ccm.drop(['LINKPRIM', 'LINKTYPE', 'LPERMCO'], axis = 1, inplace = True)
    
    ccm = pd.melt(ccm, id_vars = ['gvkey', 'conm', 'LIID', 'LPERMNO'], value_name = 'Date')
    
    ccm.sort_values(['gvkey', 'LIID', 'LPERMNO'], inplace = True)
    
    ccm.set_index('Date', inplace = True)
    
    ccm = ccm[ccm.gvkey.isin(sp_constituents.gvkey.unique())]
    
    ccm = ccm.groupby(['gvkey', 'LIID', 'LPERMNO']).resample('D').ffill()
    
    ccm.reset_index(level = [0,1,2], drop = True, inplace = True)
    
    ccm.reset_index(inplace = True)
    
    ccm.rename({'LIID': 'iid', 'LPERMNO': 'permno', 'conm':'firmname'}, axis = 1, inplace = True)
    
    sp_constituents = pd.merge(left = sp_constituents, right = ccm,\
                               on = ['gvkey', 'iid', 'Date'], how = 'left')
    
    sp_constituents.sort_values(['gvkey', 'iid', 'Date'], inplace = True)
    
    
    ffilled = sp_constituents.groupby(['gvkey', 'iid']).permno.ffill()
    ffilled.rename('ffilled_permno', inplace = True)
    
    sp_constituents = pd.concat([sp_constituents, ffilled], axis = 1)
    
    sp_constituents['permno'] = np.where(sp_constituents.permno.isnull(),\
                                         sp_constituents.ffilled_permno,\
                                         sp_constituents.permno)
    
    sp_constituents['permno'] = np.where(sp_constituents.gvkey == 326688,\
                                         17676, sp_constituents.permno)
    
    sp_constituents.drop("ffilled_permno", axis = 1, inplace = True)
    
    #%%

    return sp_constituents.copy(deep = True)

#%%
