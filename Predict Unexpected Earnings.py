# -*- coding: utf-8 -*-
"""
Created on Wed Mar 27 14:02:08 2019

@author: Admin
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime
os.getcwd()
os.chdir("D:\Master's Thesis\Data")

# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# # # # # Data Cleaning
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
#%%
# =============================================================================
# Preparing IBES-CRSP Link Table
# Note: keep only score 1 and score 2
# =============================================================================
ibes_crsp_link = pd.read_csv("./Securities/[stock] ibes_crsp_link (from wrds).csv", sep = ";")
#ibes_crsp_link.info()
ibes_crsp_link['sdate'] = pd.to_datetime(ibes_crsp_link.sdate, format = '%d-%b-%y')
ibes_crsp_link['edate'] = pd.to_datetime(ibes_crsp_link.edate, format = '%d-%b-%y')

#Keep only score 1 and 2
ibes_crsp_link = ibes_crsp_link[ibes_crsp_link.SCORE.isin([1,2])]

#Melt the dates
ibes_crsp_link = pd.melt(ibes_crsp_link, id_vars = ['TICKER', 'PERMNO', 'NCUSIP', 'SCORE'], value_name = 'Date').drop('variable', axis = 1)
ibes_crsp_link = ibes_crsp_link.sort_values(['PERMNO', 'Date'])
ibes_crsp_link.set_index('Date', inplace = True)
ibes_crsp_link = ibes_crsp_link.groupby(['TICKER', 'PERMNO', 'NCUSIP', 'SCORE']).resample('D').ffill()
ibes_crsp_link = ibes_crsp_link.reset_index(level = [0,1,2,3], drop = True)
ibes_crsp_link = ibes_crsp_link.reset_index()
ibes_crsp_link = ibes_crsp_link.sort_values(['PERMNO', 'Date'])

ibes_crsp_link_score1 = ibes_crsp_link.copy(deep = True)
ibes_crsp_link_score1 = ibes_crsp_link_score1[ibes_crsp_link_score1.SCORE == 1]

ibes_crsp_link_score2 = ibes_crsp_link.copy(deep = True)
ibes_crsp_link_score2 = ibes_crsp_link_score2[ibes_crsp_link_score2.SCORE == 2]

del ibes_crsp_link

#%%
#%%
# =============================================================================
# Use IBES-CRSP Link Table to map Permno to IBES data
# =============================================================================
esp_estimates = pd.read_csv("./Securities/[stock] IBES - ESP Estimates.csv")

esp_estimates['ANNDATS'] = pd.to_datetime(esp_estimates.ANNDATS, format = "%d/%m/%Y")
esp_estimates['FPEDATS'] = pd.to_datetime(esp_estimates.FPEDATS, format = "%d/%m/%Y")
esp_estimates['ANNDATS_ACT'] = pd.to_datetime(esp_estimates.ANNDATS_ACT, format = "%d/%m/%Y")

esp_estimates = pd.merge(left = esp_estimates, right = ibes_crsp_link_score1,\
                         left_on = ['CUSIP', 'ANNDATS'], \
                         right_on = ['NCUSIP', 'Date'], how = 'left').\
                         drop(['TICKER_y', 'Date', 'NCUSIP', 'SCORE'], axis = 1)
esp_estimates.rename({'TICKER_x':'TICKER'}, axis = 1, inplace = True)


esp_estimates = pd.merge(left = esp_estimates, right = ibes_crsp_link_score2,\
                         left_on = ['TICKER', 'ANNDATS'], \
                         right_on = ['TICKER', 'Date'], how = 'left').\
                         drop(['Date', 'NCUSIP', 'SCORE'], axis = 1)

del ibes_crsp_link_score1, ibes_crsp_link_score2

print("Score1 and Score2 map to unique rows in eps_esimates: {}".format(\
      esp_estimates[esp_estimates.PERMNO_x.notnull()&esp_estimates.PERMNO_y.notnull()].shape[0]==0))

esp_estimates['PERMNO'] = np.where(esp_estimates.PERMNO_x.isnull(), \
                                   esp_estimates.PERMNO_y,\
                                   esp_estimates.PERMNO_x)

esp_estimates.drop(['PERMNO_x', 'PERMNO_y'], axis = 1, inplace = True)

#drop rows where there is no PERMNO or no actual
esp_estimates = esp_estimates[(esp_estimates.PERMNO.notnull())&(esp_estimates.ACTUAL.notnull())]

#%%
# =============================================================================
# Attempting to identify companies that have two announcements in the same quarter
# But fail
# =============================================================================
#esp_estimates['FPEQuarter'] = esp_estimates.FPEDATS.dt.month
#
#conditions = [esp_estimates.FPEQuarter.isin([3,6,9,12]) & (esp_estimates.ANNDATS_ACT.dt.month==1),\
#              esp_estimates.FPEQuarter.isin([3,6,9,12]) & (esp_estimates.ANNDATS_ACT.dt.month==2),\
#              esp_estimates.FPEQuarter.isin([3,6,9,12]) & (esp_estimates.ANNDATS_ACT.dt.month==3),\
#              esp_estimates.FPEQuarter.isin([3,6,9,12]) & (esp_estimates.ANNDATS_ACT.dt.month==4),\
#              esp_estimates.FPEQuarter.isin([3,6,9,12]) & (esp_estimates.ANNDATS_ACT.dt.month==5),\
#              esp_estimates.FPEQuarter.isin([3,6,9,12]) & (esp_estimates.ANNDATS_ACT.dt.month==6),\
#              esp_estimates.FPEQuarter.isin([3,6,9,12]) & (esp_estimates.ANNDATS_ACT.dt.month==7),\
#              esp_estimates.FPEQuarter.isin([3,6,9,12]) & (esp_estimates.ANNDATS_ACT.dt.month==8),\
#              esp_estimates.FPEQuarter.isin([3,6,9,12]) & (esp_estimates.ANNDATS_ACT.dt.month==9),\
#              esp_estimates.FPEQuarter.isin([3,6,9,12]) & (esp_estimates.ANNDATS_ACT.dt.month==10),\
#              esp_estimates.FPEQuarter.isin([3,6,9,12]) & (esp_estimates.ANNDATS_ACT.dt.month==11),\
#              esp_estimates.FPEQuarter.isin([3,6,9,12]) & (esp_estimates.ANNDATS_ACT.dt.month==12),\
#              esp_estimates.FPEQuarter.isin([1,4,7,10]) & (esp_estimates.ANNDATS_ACT.dt.month==1),\
#              esp_estimates.FPEQuarter.isin([1,4,7,10]) & (esp_estimates.ANNDATS_ACT.dt.month==2),\
#              esp_estimates.FPEQuarter.isin([1,4,7,10]) & (esp_estimates.ANNDATS_ACT.dt.month==3),\
#              esp_estimates.FPEQuarter.isin([1,4,7,10]) & (esp_estimates.ANNDATS_ACT.dt.month==4),\
#              esp_estimates.FPEQuarter.isin([1,4,7,10]) & (esp_estimates.ANNDATS_ACT.dt.month==5),\
#              esp_estimates.FPEQuarter.isin([1,4,7,10]) & (esp_estimates.ANNDATS_ACT.dt.month==6),\
#              esp_estimates.FPEQuarter.isin([1,4,7,10]) & (esp_estimates.ANNDATS_ACT.dt.month==7),\
#              esp_estimates.FPEQuarter.isin([1,4,7,10]) & (esp_estimates.ANNDATS_ACT.dt.month==8),\
#              esp_estimates.FPEQuarter.isin([1,4,7,10]) & (esp_estimates.ANNDATS_ACT.dt.month==9),\
#              esp_estimates.FPEQuarter.isin([1,4,7,10]) & (esp_estimates.ANNDATS_ACT.dt.month==10),\
#              esp_estimates.FPEQuarter.isin([1,4,7,10]) & (esp_estimates.ANNDATS_ACT.dt.month==11),\
#              esp_estimates.FPEQuarter.isin([1,4,7,10]) & (esp_estimates.ANNDATS_ACT.dt.month==12),\
#              esp_estimates.FPEQuarter.isin([2,5,8,11]) & (esp_estimates.ANNDATS_ACT.dt.month==1),\
#              esp_estimates.FPEQuarter.isin([2,5,8,11]) & (esp_estimates.ANNDATS_ACT.dt.month==2),\
#              esp_estimates.FPEQuarter.isin([2,5,8,11]) & (esp_estimates.ANNDATS_ACT.dt.month==3),\
#              esp_estimates.FPEQuarter.isin([2,5,8,11]) & (esp_estimates.ANNDATS_ACT.dt.month==4),\
#              esp_estimates.FPEQuarter.isin([2,5,8,11]) & (esp_estimates.ANNDATS_ACT.dt.month==5),\
#              esp_estimates.FPEQuarter.isin([2,5,8,11]) & (esp_estimates.ANNDATS_ACT.dt.month==6),\
#              esp_estimates.FPEQuarter.isin([2,5,8,11]) & (esp_estimates.ANNDATS_ACT.dt.month==7),\
#              esp_estimates.FPEQuarter.isin([2,5,8,11]) & (esp_estimates.ANNDATS_ACT.dt.month==8),\
#              esp_estimates.FPEQuarter.isin([2,5,8,11]) & (esp_estimates.ANNDATS_ACT.dt.month==9),\
#              esp_estimates.FPEQuarter.isin([2,5,8,11]) & (esp_estimates.ANNDATS_ACT.dt.month==10),\
#              esp_estimates.FPEQuarter.isin([2,5,8,11]) & (esp_estimates.ANNDATS_ACT.dt.month==11),\
#              esp_estimates.FPEQuarter.isin([2,5,8,11]) & (esp_estimates.ANNDATS_ACT.dt.month==12)]
#
#choices = [1,1,1,2,2,2,3,3,3,4,4,4,\
#           4,1,1,1,2,2,2,3,3,3,4,4,\
#           4,4,1,1,1,2,2,2,3,3,3,4]
#
#esp_estimates['ANNDATS_ACT_CalQ'] = np.select(conditions, choices)
#%%
# =============================================================================
# Section: for estimators that make many estimates, keep the last estimate 
# in the intended quarter according to FPI
# Assumption 1: months in FPEDATS indicate fiscal quarter of the stock
# Assumption 2: actual announcement is for the quarter in which the announcement takes places
#               Assumption 2 is not correct. For some companies, e.g. CUSIP 36720410
#               can have two announcements in the same quarter, one for the current quarter and
#               one for the previous quarter. The data is so unsystematic I couldn't find 
#               a way to control for this yet. Fortunately, assumption 2 largely holds for 
#               most of the time 
# =============================================================================
#temp = esp_estimates.loc[esp_estimates.groupby(['TICKER', 'CUSIP', 'ESTIMATOR', 'FPI', 'ANNDATS_ACT']).ANNDATS.idxmax()]

esp_estimates = esp_estimates[['PERMNO', 'TICKER', 'CUSIP', 'CNAME', 'ESTIMATOR', 'FPI',\
                               'FPEDATS', 'ANNDATS', 'VALUE', 'ACTUAL', 'ANNDATS_ACT',]]

# Align the quarter for all rows to be calendar quarters
esp_estimates['fpemonth'] = esp_estimates.FPEDATS.dt.month

conditions = [esp_estimates.fpemonth.isin([1,4,7,10]),\
              esp_estimates.fpemonth.isin([2,5,8,11]),\
              esp_estimates.fpemonth.isin([3,6,9,12])]

choices = [pd.offsets.DateOffset(months = 1),\
           pd.offsets.DateOffset(months = 2),\
           pd.offsets.DateOffset(months = 0)]

esp_estimates['align_month'] = np.select(conditions, choices)


esp_estimates['FPEDATS'] = np.where(esp_estimates.fpemonth.isin([1,4,7,10,2,5,8,11]),\
                                    esp_estimates.FPEDATS - esp_estimates.align_month,\
                                    esp_estimates.FPEDATS)
esp_estimates['ANNDATS'] = np.where(esp_estimates.fpemonth.isin([1,4,7,10,2,5,8,11]),\
                                    esp_estimates.ANNDATS - esp_estimates.align_month,\
                                    esp_estimates.ANNDATS)
esp_estimates['ANNDATS_ACT'] = np.where(esp_estimates.fpemonth.isin([1,4,7,10,2,5,8,11]),\
                                     esp_estimates.ANNDATS_ACT - esp_estimates.align_month,\
                                     esp_estimates.ANNDATS_ACT)

#slide actual announcement date back according to FPI
conditions = [esp_estimates.FPI == 6,\
              esp_estimates.FPI == 7,\
              esp_estimates.FPI == 8,\
              esp_estimates.FPI == 9]

choices = [pd.offsets.DateOffset(months = 3),\
           pd.offsets.DateOffset(months = 6),\
           pd.offsets.DateOffset(months = 9),\
           pd.offsets.DateOffset(months = 12)]

esp_estimates['slided_actual_annc']= np.select(conditions, choices)

del conditions, choices

esp_estimates['slided_actual_annc'] = esp_estimates.ANNDATS_ACT - esp_estimates.slided_actual_annc

#slide this to the end of the intended quarter
esp_estimates['slided_actual_annc'] = np.where(esp_estimates.slided_actual_annc.dt.is_quarter_end,\
                                               esp_estimates.slided_actual_annc,\
                                               esp_estimates.slided_actual_annc + pd.tseries.offsets.QuarterEnd())

esp_estimates_pristine = esp_estimates.copy(deep = True)
#keep only rows where estimate announcement date is in the same quarter with 
# slided_actual_annc
esp_estimates = esp_estimates[(esp_estimates.ANNDATS.dt.quarter == esp_estimates.slided_actual_annc.dt.quarter)&\
                              (esp_estimates.ANNDATS.dt.year == esp_estimates.slided_actual_annc.dt.year)]

esp_estimates_before = esp_estimates.copy(deep = True)
print('Before idxmax, the dataset length is {}'.format(esp_estimates.shape[0]))


# For each stock, at each actual announcement and by each estimator, 
# keep only the rows of the last estimate annoucements according to FPI
esp_estimates = esp_estimates.loc[esp_estimates.groupby(['PERMNO', 'ESTIMATOR', 'FPI', 'ANNDATS_ACT']).ANNDATS.idxmax()]
print('After idxmax, the dataset length is {}'.format(esp_estimates.shape[0]))

# Check
temp1 = esp_estimates.groupby(['PERMNO','ESTIMATOR', 'ANNDATS_ACT']).FPI.count().reset_index().rename({'FPI':'row_count'}, axis =1)
temp2 = esp_estimates.groupby(['PERMNO','ESTIMATOR', 'ANNDATS_ACT']).FPI.nunique().reset_index().rename({'FPI':'unique_FPI'}, axis =1 )
temp = pd.merge(left = temp1, right = temp2, on = ['PERMNO', 'ESTIMATOR', 'ANNDATS_ACT'], how = 'inner')
temp['check'] = temp.row_count == temp.unique_FPI
print("After idxmax, for each stock, each estimator and each actual announcement, there are now only unique FPI: {}".format(temp[temp.check==False].shape[0]==0))
del temp, temp1, temp2

esp_estimates.drop(['fpemonth', 'align_month'], axis = 1, inplace = True)
#%%
# =============================================================================

# =============================================================================


#%%
# =============================================================================
# Sliding date variables
# =============================================================================

#Sliding ANNDATS to quarter end
esp_estimates['slided_ANNDATS'] = np.where(esp_estimates.ANNDATS.dt.is_quarter_end,\
                                           esp_estimates.ANNDATS,\
                                           esp_estimates.ANNDATS + pd.tseries.offsets.QuarterEnd())
# Check if slided_ANNDATS == slided_actual_annc
# Answer: yes. Uncomment to run and check
#temp = esp_estimates.slided_ANNDATS == esp_estimates.slided_actual_annc
#temp = esp_estimates[temp==False]
#temp[temp==False].shape
#del temp
esp_estimates.drop('slided_actual_annc', axis =1, inplace = True)

#Sliding ANNDATS_ACT to quarter end
esp_estimates['slided_ANNDATS_ACT'] = np.where(esp_estimates.ANNDATS_ACT.dt.is_quarter_end,\
                                               esp_estimates.ANNDATS_ACT,\
                                               esp_estimates.ANNDATS_ACT + pd.tseries.offsets.QuarterEnd())

#%%
#Find the mean of estimates for each stock-fpi-earning annoucement
temp = esp_estimates.copy(deep = True)

temp = temp.groupby(['PERMNO', 'FPI', 'ANNDATS_ACT']).agg({'TICKER':'first',
                    'CUSIP': 'first', 'CNAME': 'first', 'FPEDATS':'first',\
                    'ANNDATS':'first', 'VALUE': 'mean', 'ACTUAL': 'mean',\
                    'slided_ANNDATS': 'first', 'slided_ANNDATS_ACT':'first'}).reset_index()
temp.rename({'ACTUAL':'ExpectedACTUAL'}, axis = 1, inplace = True) #because the estimate is the median or mean

#in few instances, for each permno and FPI, there could be many earning announcement in the same quarter
# For expedient purposes, just take the last earning announcement within a quarter
temp.sort_values(['PERMNO', 'FPI', 'ANNDATS_ACT'], inplace = True)
temp = temp.groupby(['PERMNO', 'FPI', 'slided_ANNDATS_ACT']).last()
temp.reset_index(inplace = True)



temp['UE'] = temp.VALUE - temp.ExpectedACTUAL

temp = temp[['PERMNO', 'FPI', 'CNAME', 'ANNDATS', 'slided_ANNDATS','slided_ANNDATS_ACT', 'UE']]

ibesUE6 = temp.copy(deep = True)
ibesUE6  = ibesUE6 [ibesUE6 .FPI == 6].drop('FPI', axis = 1)

ibesUE6.rename({'slided_ANNDATS': 'estimate3M', 'slided_ANNDATS_ACT': 'announcement3M', 'UE': 'UE3M'}, axis = 1, inplace = True)

ibesUE7 = temp.copy(deep = True)
ibesUE7 = ibesUE7[ibesUE7.FPI == 7].drop('FPI', axis = 1)

ibesUE7.rename({'slided_ANNDATS': 'estimate6M', 'slided_ANNDATS_ACT': 'announcement6M', 'UE': 'UE6M'}, axis = 1, inplace = True)


ibesUE8 = temp.copy(deep = True)
ibesUE8 = ibesUE8[ibesUE8.FPI == 8].drop('FPI', axis =1)

ibesUE8.rename({'slided_ANNDATS': 'estimate9M', 'slided_ANNDATS_ACT': 'announcement9M', 'UE': 'UE9M'}, axis = 1, inplace = True)


ibesUE9 = temp.copy(deep = True)
ibesUE9 = ibesUE9[ibesUE9.FPI == 9].drop('FPI', axis =1)

ibesUE9.rename({'slided_ANNDATS': 'estimate12M', 'slided_ANNDATS_ACT': 'announcement12M', 'UE': 'UE12M'}, axis = 1, inplace = True)

del temp

#%%
#Assume mfHoldings from the other file
#%%
# =============================================================================
# Calculate aggregated active weight and aggregated squared active weight
# Result: variable aggrHoldings
# =============================================================================
holdings = mfHoldings.copy(deep = True)

holdings.replace([np.inf, -np.inf], np.nan, inplace = True)

holdings3M = mfHoldings.copy(deep = True)
holdings3M = holdings3M[['crsp_portno', 'security_name', 'slided_report_dt', 'passiveWeight3M']]

holdings3M['slided_report_dt'] = holdings3M.slided_report_dt + pd.offsets.DateOffset(months = 3)
holdings3M['slided_report_dt'] = np.where(holdings3M.slided_report_dt.dt.is_quarter_end,\
                                          holdings3M.slided_report_dt,\
                                          holdings3M.slided_report_dt + pd.tseries.offsets.QuarterEnd())

holdings6M = mfHoldings.copy(deep = True)
holdings6M = holdings6M[['crsp_portno', 'security_name', 'slided_report_dt', 'passiveWeight6M']]

holdings6M['slided_report_dt'] = holdings6M.slided_report_dt + pd.offsets.DateOffset(months = 6)
holdings6M['slided_report_dt'] = np.where(holdings6M.slided_report_dt.dt.is_quarter_end,\
                                          holdings6M.slided_report_dt,\
                                          holdings6M.slided_report_dt + pd.tseries.offsets.QuarterEnd())

holdings12M = mfHoldings.copy(deep = True)
holdings12M = holdings12M[['crsp_portno', 'security_name', 'slided_report_dt', 'passiveWeight12M']]

holdings12M['slided_report_dt'] = holdings12M.slided_report_dt + pd.offsets.DateOffset(months = 12)
holdings12M['slided_report_dt'] = np.where(holdings12M.slided_report_dt.dt.is_quarter_end,\
                                          holdings12M.slided_report_dt,\
                                          holdings12M.slided_report_dt + pd.tseries.offsets.QuarterEnd())

holdings.drop(['passiveWeight12M', 'passiveWeight3M', 'passiveWeight6M'], axis = 1, inplace = True)

holdings = pd.merge(left = holdings, right = holdings3M,\
                      on = ['crsp_portno', 'security_name', 'slided_report_dt'],\
                      how = 'left')

holdings = pd.merge(left = holdings, right = holdings6M,\
                      on = ['crsp_portno', 'security_name', 'slided_report_dt'],\
                      how = 'left')

holdings = pd.merge(left = holdings, right = holdings12M,\
                      on = ['crsp_portno', 'security_name', 'slided_report_dt'],\
                      how = 'left')

holdings.rename({'passiveWeight12M': 'aligned_passiveWeight12M', 'passiveWeight3M': 'aligned_passiveWeight3M', 'passiveWeight6M': 'aligned_passiveWeight6M'}, axis = 1,inplace = True)

holdings['activeWeight3M'] = holdings.weight - holdings.aligned_passiveWeight3M
holdings['activeWeight6M'] = holdings.weight - holdings.aligned_passiveWeight6M
holdings['activeWeight12M'] = holdings.weight - holdings.aligned_passiveWeight12M

holdings['activeWeight3MSquared'] = holdings.activeWeight3M * holdings.activeWeight3M 
holdings['activeWeight6MSquared'] = holdings.activeWeight6M * holdings.activeWeight6M
holdings['activeWeight12MSquared'] = holdings.activeWeight12M * holdings.activeWeight12M

holdings = holdings[['crsp_portno', 'permno', 'security_name', 'slided_report_dt', 'activeWeight3M', 'activeWeight6M', 'activeWeight12M', 'activeWeight3MSquared',\
                     'activeWeight6MSquared', 'activeWeight12MSquared']]

aggrHoldings = holdings.groupby(['security_name', 'slided_report_dt']).agg({'permno': 'first', 'activeWeight3M': 'sum', \
                               'activeWeight6M': 'sum', 'activeWeight12M':'sum', 'activeWeight3MSquared': 'sum',\
                               'activeWeight6MSquared': 'sum', 'activeWeight12MSquared': 'sum'})
aggrHoldings.reset_index(inplace = True)


#in the future I may want to consider mapping by names 
aggrHoldings = aggrHoldings[aggrHoldings.permno.notnull()]
aggrHoldings.replace([np.inf, -np.inf], np.nan, inplace = True)

aggrHoldings.dropna(inplace = True)

#Remove where all three squared aggregated weights = 0. Like due to missing values
aggrHoldings = aggrHoldings[(aggrHoldings.activeWeight3MSquared!=0)|(aggrHoldings.activeWeight6MSquared!=0)|(aggrHoldings.activeWeight12MSquared!=0)]


#%%
# =============================================================================
# merge aggreHoldings to ibesUE
# =============================================================================
aggrHoldings = pd.merge(left = aggrHoldings, right = ibesUE6,\
                        left_on = ['slided_report_dt', 'permno'],\
                        right_on = ['estimate3M', 'PERMNO'],\
                        how = 'left')

aggrHoldings = pd.merge(left = aggrHoldings, right = ibesUE7,\
                        left_on = ['slided_report_dt', 'permno'],\
                        right_on = ['estimate6M', 'PERMNO'],\
                        how = 'left')


aggrHoldings = pd.merge(left = aggrHoldings, right = ibesUE9,\
                        left_on = ['slided_report_dt', 'permno'],\
                        right_on = ['estimate12M', 'PERMNO'],\
                        how = 'left')

aggrHoldings.drop(['PERMNO_x', 'PERMNO_y', 'PERMNO', 'CNAME_x', \
                   'CNAME_y', 'CNAME', 'ANNDATS_x', 'ANNDATS_y', 'ANNDATS'],\
                     axis = 1, inplace = True)

aggrHoldings.drop(['estimate6M', 'estimate3M', 'estimate12M'], axis = 1, inplace = True)

# Remove rows where none of the earning surprised can be mapped
aggrHoldings = aggrHoldings[aggrHoldings.UE3M.notnull()|aggrHoldings.UE6M.notnull()|aggrHoldings.UE12M.notnull()]

#%%
# =============================================================================
# 
# =============================================================================
temp = aggrHoldings.copy(deep = True)
temp = aggrHoldings[['activeWeight3M', 'activeWeight6M', 'activeWeight12M', 'UE3M', 'UE6M', 'UE12M']]
temp.corr()

#%%
from linearmodels.panel import PanelOLS
temp = aggrHoldings.copy(deep = True)
#temp = temp.replace([np.inf, -np.inf], np.nan)
temp = temp.dropna()
temp = temp.set_index(['permno', 'slided_report_dt'])

#mod = PanelOLS(temp.UE12M, temp[['activeWeight3MSquared', 'activeWeight6MSquared', 'activeWeight12MSquared']], time_effects = False, entity_effects = True)
####
#Only time effect
####
#mod = PanelOLS(temp.UE3M, temp[['activeWeight3M', 'activeWeight6M', 'activeWeight12M']], time_effects = True)
#mod = PanelOLS(temp.UE3M, temp[['activeWeight3M']], time_effects = True)
#mod = PanelOLS(temp.UE3M, temp[['activeWeight6M']], time_effects = True)
#mod = PanelOLS(temp.UE3M, temp[['activeWeight12M']], time_effects = True)


#mod = PanelOLS(temp.UE6M, temp[['activeWeight3M', 'activeWeight6M', 'activeWeight12M']], time_effects = True)
#mod = PanelOLS(temp.UE6M, temp[['activeWeight3M']], time_effects = True)
#mod = PanelOLS(temp.UE6M, temp[['activeWeight6M']], time_effects = True)
#mod = PanelOLS(temp.UE6M, temp[['activeWeight12M']], time_effects = True)


#mod = PanelOLS(temp.UE12M, temp[['activeWeight3M', 'activeWeight6M', 'activeWeight12M']], time_effects = True)
#mod = PanelOLS(temp.UE12M, temp[['activeWeight3M']], time_effects = True)
#mod = PanelOLS(temp.UE12M, temp[['activeWeight6M']], time_effects = True)
#mod = PanelOLS(temp.UE12M, temp[['activeWeight12M']], time_effects = True)


#mod = PanelOLS(temp.UE3M, temp[['activeWeight3MSquared', 'activeWeight6MSquared', 'activeWeight12MSquared']], time_effects = True)
#mod = PanelOLS(temp.UE3M, temp[['activeWeight3MSquared']], time_effects = True)
#mod = PanelOLS(temp.UE3M, temp[['activeWeight6MSquared']], time_effects = True)
#mod = PanelOLS(temp.UE3M, temp[['activeWeight12MSquared']], time_effects = True)


mod = PanelOLS(temp.UE6M, temp[['activeWeight3MSquared', 'activeWeight6MSquared', 'activeWeight12MSquared']], time_effects = True)
#mod = PanelOLS(temp.UE6M, temp[['activeWeight3MSquared']], time_effects = True)
#mod = PanelOLS(temp.UE6M, temp[['activeWeight6MSquared']], time_effects = True)
#mod = PanelOLS(temp.UE6M, temp[['activeWeight12MSquared']], time_effects = True)


#mod = PanelOLS(temp.UE12M, temp[['activeWeight3MSquared', 'activeWeight6MSquared', 'activeWeight12MSquared']], time_effects = True)
#mod = PanelOLS(temp.UE12M, temp[['activeWeight3MSquared']], time_effects = True)
#mod = PanelOLS(temp.UE12M, temp[['activeWeight6MSquared']], time_effects = True)
#mod = PanelOLS(temp.UE12M, temp[['activeWeight12MSquared']], time_effects = True)


####
#Only entity effect
####
#mod = PanelOLS(temp.UE3M, temp[['activeWeight3M', 'activeWeight6M', 'activeWeight12M']], entity_effects = True)
#mod = PanelOLS(temp.UE3M, temp[['activeWeight3M']], entity_effects = True)
#mod = PanelOLS(temp.UE3M, temp[['activeWeight6M']], entity_effects = True)
#mod = PanelOLS(temp.UE3M, temp[['activeWeight12M']], entity_effects = True)


#mod = PanelOLS(temp.UE6M, temp[['activeWeight3M', 'activeWeight6M', 'activeWeight12M']], entity_effects = True)
#mod = PanelOLS(temp.UE6M, temp[['activeWeight3M']], entity_effects = True)
#mod = PanelOLS(temp.UE6M, temp[['activeWeight6M']], entity_effects = True)
#mod = PanelOLS(temp.UE6M, temp[['activeWeight12M']], entity_effects = True)


#mod = PanelOLS(temp.UE12M, temp[['activeWeight3M', 'activeWeight6M', 'activeWeight12M']], entity_effects = True)
#mod = PanelOLS(temp.UE12M, temp[['activeWeight3M']], entity_effects = True)
#mod = PanelOLS(temp.UE12M, temp[['activeWeight6M']], entity_effects = True)
#mod = PanelOLS(temp.UE12M, temp[['activeWeight12M']], entity_effects = True)


#mod = PanelOLS(temp.UE3M, temp[['activeWeight3MSquared', 'activeWeight6MSquared', 'activeWeight12MSquared']], entity_effects = True)
#mod = PanelOLS(temp.UE3M, temp[['activeWeight3MSquared']], entity_effects = True)
#mod = PanelOLS(temp.UE3M, temp[['activeWeight6MSquared']], entity_effects = True)
#mod = PanelOLS(temp.UE3M, temp[['activeWeight12MSquared']], entity_effects = True)


#mod = PanelOLS(temp.UE6M, temp[['activeWeight3MSquared', 'activeWeight6MSquared', 'activeWeight12MSquared']], entity_effects = True)
#mod = PanelOLS(temp.UE6M, temp[['activeWeight3MSquared']], entity_effects = True)
#mod = PanelOLS(temp.UE6M, temp[['activeWeight6MSquared']], entity_effects = True)
#mod = PanelOLS(temp.UE6M, temp[['activeWeight12MSquared']], entity_effects = True)


#mod = PanelOLS(temp.UE12M, temp[['activeWeight3MSquared', 'activeWeight6MSquared', 'activeWeight12MSquared']], entity_effects = True)
#mod = PanelOLS(temp.UE12M, temp[['activeWeight3MSquared']], entity_effects = True)
#mod = PanelOLS(temp.UE12M, temp[['activeWeight6MSquared']], entity_effects = True)
#mod = PanelOLS(temp.UE12M, temp[['activeWeight12MSquared']], entity_effects = True)


#Both entity and time effect
#mod = PanelOLS(temp.UE3M, temp[['activeWeight3M', 'activeWeight6M', 'activeWeight12M']], entity_effects = True, time_effects = True)
#mod = PanelOLS(temp.UE3M, temp[['activeWeight3M']], entity_effects = True, time_effects = True)
#mod = PanelOLS(temp.UE3M, temp[['activeWeight6M']], entity_effects = True, time_effects = True)
#mod = PanelOLS(temp.UE3M, temp[['activeWeight12M']], entity_effects = True, time_effects = True)


#mod = PanelOLS(temp.UE6M, temp[['activeWeight3M', 'activeWeight6M', 'activeWeight12M']], entity_effects = True, time_effects = True)
#mod = PanelOLS(temp.UE6M, temp[['activeWeight3M']], entity_effects = True, time_effects = True)
#mod = PanelOLS(temp.UE6M, temp[['activeWeight6M']], entity_effects = True, time_effects = True)
#mod = PanelOLS(temp.UE6M, temp[['activeWeight12M']], entity_effects = True, time_effects = True)


#mod = PanelOLS(temp.UE12M, temp[['activeWeight3M', 'activeWeight6M', 'activeWeight12M']], entity_effects = True, time_effects = True)
#mod = PanelOLS(temp.UE12M, temp[['activeWeight3M']], entity_effects = True, time_effects = True)
#mod = PanelOLS(temp.UE12M, temp[['activeWeight6M']], entity_effects = True, time_effects = True)
#mod = PanelOLS(temp.UE12M, temp[['activeWeight12M']], entity_effects = True, time_effects = True)


#mod = PanelOLS(temp.UE3M, temp[['activeWeight3MSquared', 'activeWeight6MSquared', 'activeWeight12MSquared']], entity_effects = True, time_effects = True)
#mod = PanelOLS(temp.UE3M, temp[['activeWeight3MSquared']], entity_effects = True, time_effects = True)
#mod = PanelOLS(temp.UE3M, temp[['activeWeight6MSquared']], entity_effects = True, time_effects = True)
#mod = PanelOLS(temp.UE3M, temp[['activeWeight12MSquared']], entity_effects = True, time_effects = True)


#mod = PanelOLS(temp.UE6M, temp[['activeWeight3MSquared', 'activeWeight6MSquared', 'activeWeight12MSquared']], entity_effects = True, time_effects = True)
#mod = PanelOLS(temp.UE6M, temp[['activeWeight3MSquared']], entity_effects = True, time_effects = True)
#mod = PanelOLS(temp.UE6M, temp[['activeWeight6MSquared']], entity_effects = True, time_effects = True)
#mod = PanelOLS(temp.UE6M, temp[['activeWeight12MSquared']], entity_effects = True, time_effects = True)


#mod = PanelOLS(temp.UE12M, temp[['activeWeight3MSquared', 'activeWeight6MSquared', 'activeWeight12MSquared']], entity_effects = True, time_effects = True)
#mod = PanelOLS(temp.UE12M, temp[['activeWeight3MSquared']], entity_effects = True, time_effects = True)
#mod = PanelOLS(temp.UE12M, temp[['activeWeight6MSquared']], entity_effects = True, time_effects = True)
#mod = PanelOLS(temp.UE12M, temp[['activeWeight12MSquared']], entity_effects = True, time_effects = True)


res = mod.fit(cov_type = 'clustered', cluster_entity = True)
#res = mod.fit(cov_type = 'clustered', cluster_time = True)
#res = mod.fit(cov_type = 'clustered', cluster_entity = True, cluster_time = True)
#res = mod.fit(cov_type = 'robust')
print(res)

del temp

























