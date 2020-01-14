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
pd.options.display.max_columns = 10
import numpy as np
import statsmodels
import statsmodels.api as sm
import seaborn as sns
import datetime
from scipy import stats
import gc
gc.enable()


#%%
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# File Description: AFP Predictive Panel Data Regression
# Main outcome: Section 5 allows for choosing models and generating results
# which are displayed in console
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
#%%
# ==========================================================================================================================================================
# ==========================================================================================================================================================
# Section 1: compute expected returns for funds using Carhart and Fama-French factor models
# Main outcome variable: mfCarhartExpectedReturns and mfFFExpectedReturns
# ==========================================================================================================================================================
# ==========================================================================================================================================================

ThreeFactorModel = pd.read_excel("./Data/Factor Models/F-F_Research_Data_Factors.xlsx")
Momentum = pd.read_excel("./Data/Factor Models/F-F_Momentum_Factor.xlsx")

FactorModel = pd.merge(left = ThreeFactorModel, right = Momentum, \
                       on = 'month_year', how = 'inner', indicator = True)
print('Merging Momentum and Three factor model successful: {}'.format(FactorModel._merge.nunique()==1))
FactorModel.drop("_merge", axis = 1, inplace = True)


#Convert month_year to pandas datetime and slide it to month end
FactorModel['date'] = pd.to_datetime(FactorModel.month_year, format = "%Y%m")
FactorModel['date'] = FactorModel.date + pd.tseries.offsets.MonthEnd()
FactorModel = FactorModel[FactorModel.date.dt.year >= 2001]
FactorModel.drop('month_year', axis = 1, inplace = True)
FactorModel = FactorModel[['date', 'Mkt-RF', 'SMB', 'HML', 'RF', 'Mom']]
print("No missing data for Momentum: ",FactorModel[FactorModel.Mom.isin([-99.99, -999])].shape[0]==0)
del ThreeFactorModel, Momentum


tempMfReturns = mfReturns.copy(deep = True)
tempMfReturns = tempMfReturns[tempMfReturns.crsp_portno.isin(mfSummary.crsp_portno.unique())]
tempMfReturns['slided_caldt'] = np.where(tempMfReturns.caldt.dt.is_month_end, \
                                         tempMfReturns.caldt,\
                                         tempMfReturns.caldt + pd.tseries.offsets.MonthEnd(0))

#####
# Estimate the timeseries expected return of the funds using Carhart and Fama-French models.
# The factor loadings are estimated by rolling-window timeseries regression of fund net returns on 
# factor-mimicking portfolios iin the previous three years 
#####
expectedCarhartReturnList = []
expectedFFReturnList = []
for eachPortno, portnoGroup in tempMfReturns.groupby('crsp_portno'): # loop through each fund at a time
    tempPortnoGroup = portnoGroup.copy(deep = True)
    tempPortnoGroup = tempPortnoGroup.sort_values('slided_caldt')
    tempFirstDate = tempPortnoGroup.slided_caldt.iloc[0]
    for currentCaldt in tempPortnoGroup.slided_caldt: # loop through each quarter at a time
        if currentCaldt >= (tempFirstDate + pd.DateOffset(years = 3)):
            # choose the latest three year window for the current quarter in the loop
            tempReturnWindow = portnoGroup[portnoGroup.slided_caldt.between\
                                           (left = currentCaldt - pd.DateOffset(years = 3), \
                                            right = currentCaldt, inclusive = True)]
            # merge with factor mimicmking portfolio returns 
            tempReturnWindow = pd.merge(left = tempReturnWindow, right = FactorModel,\
                                        left_on = 'slided_caldt', right_on = 'date',\
                                        how = 'left')
            
            #Estimate expected return using Carhart Model
            tempX = tempReturnWindow[['Mkt-RF', 'SMB', 'HML', 'Mom']]
            tempy = tempReturnWindow['mret']
            tempModel = sm.OLS(tempy,tempX).fit()
            tempCarhartExpectedReturn = tempModel.predict(FactorModel\
                                            [FactorModel.date == currentCaldt]\
                                            [['Mkt-RF', 'SMB', 'HML', 'Mom']])
            expectedCarhartReturnList.append([eachPortno, currentCaldt, tempCarhartExpectedReturn.iloc[0]])
            
            #Estimate expected return using Fama-French Model
            tempX = tempReturnWindow[['Mkt-RF', 'SMB', 'HML']]
            tempy = tempReturnWindow['mret']
            tempModel = sm.OLS(tempy,tempX).fit()
            tempFFExpectedReturn = tempModel.predict(FactorModel\
                                            [FactorModel.date == currentCaldt]\
                                            [['Mkt-RF', 'SMB', 'HML']])
            expectedFFReturnList.append([eachPortno, currentCaldt, tempFFExpectedReturn.iloc[0]])
            
            del tempReturnWindow, tempX, tempy, tempModel, tempCarhartExpectedReturn, tempFFExpectedReturn
    del tempPortnoGroup, tempFirstDate
            
del eachPortno, portnoGroup, FactorModel, currentCaldt

            
mfCarhartExpectedReturns = pd.DataFrame(expectedCarhartReturnList)
mfCarhartExpectedReturns.columns = ['crsp_portno', 'slided_caldt', 'Carhart_expected_ret']

mfFFExpectedReturns = pd.DataFrame(expectedFFReturnList)
mfFFExpectedReturns.columns = ['crsp_portno', 'slided_caldt', 'FF_expected_ret']

del expectedCarhartReturnList, expectedFFReturnList
 
# ==========================================================================================================================================================
# Section 2: Calculate Carhart alpha and FF alpha, fund return in excess of expected return 
# Main outcome variable: tempMfReturns
# ==========================================================================================================================================================

tempMfReturns = pd.merge(left = tempMfReturns, right = mfCarhartExpectedReturns,\
                         on = ['crsp_portno', 'slided_caldt'], how = 'left')

tempMfReturns['CarhartAlpha'] = tempMfReturns.mret - tempMfReturns.Carhart_expected_ret

tempMfReturns = tempMfReturns[tempMfReturns.Carhart_expected_ret.notnull()]

tempMfReturns.drop('Carhart_expected_ret', axis = 1, inplace = True)

#del mfCarhartExpectedReturns

tempMfReturns = pd.merge(left = tempMfReturns, right = mfFFExpectedReturns,\
                         on = ['crsp_portno', 'slided_caldt'], how = 'left')

tempMfReturns['FFAlpha'] = tempMfReturns.mret - tempMfReturns.FF_expected_ret

tempMfReturns = tempMfReturns[tempMfReturns.FF_expected_ret.notnull()]

tempMfReturns.drop('FF_expected_ret', axis = 1, inplace = True)

#del mfFFExpectedReturns

#%%
# ==========================================================================================================================================================
# ==========================================================================================================================================================
# Section 3: Prepare fund characteristics for the panel data regressions
# Main outcome variable: tempMfReturns
# ==========================================================================================================================================================
# ==========================================================================================================================================================


# =============================================================================
# Prepare tempMfSummary to get fund characteristics:
# - NAV is the sum of NAVs across all share classes
# - TNA is the sum of TNAs across all share classes
# - exp_ratio is the tna-weighted avearge of exp_ratio across all share classes
# - turn_ratio is the tna-weighted average of turn_ratio across all share clases
# - age is the age of the oldest share class of a fund
# =============================================================================

# Calculate tna-weighted expense ratio and turnover ratio.
# Resulting variable: mfRatio
mfRatio = mfSummary.copy(deep = True)
mfRatio = mfRatio[['crsp_portno', 'crsp_fundno', 'monthyear', 'exp_ratio', 'turn_ratio', 'tna_latest']]
mfRatio.rename({'tna_latest': 'tna'}, axis = 1, inplace = True)

mfSumValue = mfSummary.copy(deep = True)
mfSumValue = mfSumValue[['crsp_portno', 'crsp_fundno', 'caldt', 'monthyear', 'nav_latest', 'tna_latest']]
mfSumValue = mfSumValue.groupby(['crsp_portno', 'monthyear'])[['nav_latest', 'tna_latest']].sum()
mfSumValue.reset_index(inplace = True)

mfRatio = pd.merge(left = mfRatio, right = mfSumValue,\
                   on = ['crsp_portno', 'monthyear'], how = 'left')
del mfSumValue
mfRatio['tna_weight'] = mfRatio.tna / mfRatio.tna_latest
mfRatio.sort_values(['crsp_portno', 'monthyear'], inplace = True)
mfRatio['weighted_exp_ratio'] = mfRatio.exp_ratio * mfRatio.tna_weight
mfRatio['weighted_turn_ratio'] = mfRatio.turn_ratio * mfRatio.tna_weight

mfRatio = mfRatio.groupby(['crsp_portno', 'monthyear'])['weighted_exp_ratio', 'weighted_turn_ratio'].sum()
mfRatio.reset_index(inplace = True)
mfRatio['slided_caldt'] = pd.to_datetime(mfRatio.monthyear, format = "%m/%Y")
mfRatio['slided_caldt'] = mfRatio.slided_caldt + pd.tseries.offsets.MonthEnd()
mfRatio.drop('monthyear', axis = 1, inplace = True)

#Calculate NAV and TNA by summing NAV and TNA across share classes of a given fund
#Resulting variable: mfValue
mfValue = mfSummary.copy(deep = True)
mfValue['slided_caldt'] = np.where(mfValue.caldt.dt.is_month_end,\
                                         mfValue.caldt,\
                                         mfValue.caldt + pd.tseries.offsets.MonthEnd(0))
mfValue = mfValue[['crsp_portno', 'crsp_fundno', 'slided_caldt', 'nav_latest', 'tna_latest']]
mfValue = mfValue.groupby(['crsp_portno', 'slided_caldt'])['nav_latest', 'tna_latest'].sum().reset_index()

# Merge mfRatio and mfValue into mfRatioValue
mfRatioValue = pd.merge(left = mfRatio, right = mfValue, on = ['crsp_portno', 'slided_caldt'], how = 'left')
mfRatioValue = mfRatioValue[['slided_caldt', 'crsp_portno', 'weighted_exp_ratio', \
                             'weighted_turn_ratio', 'nav_latest', 'tna_latest']]

# Resample expense and turnover ratio from quarterly to monthly to use the higher frequency of return data
# This is actually redundant, because AFP is on a quarterly basis
# Resulting variable: resampledFundSummary
resampledFundSummary = pd.DataFrame(columns = mfRatioValue.columns)
for eachPortno, eachGroup in mfRatioValue.groupby('crsp_portno'):
    tempGroup = eachGroup.copy(deep = True)
    tempGroup.sort_values('slided_caldt', inplace = True)
    tempGroup.set_index('slided_caldt', inplace = True)
    tempGroup = tempGroup.resample('d').ffill()
    tempGroup = tempGroup.reset_index()
    tempGroup = tempGroup[tempGroup.slided_caldt.dt.is_month_end]
    resampledFundSummary = pd.concat([resampledFundSummary, tempGroup], axis = 0)
    del tempGroup

# =============================================================================
# Create variable age and merge to resampledFundSummary
# =============================================================================
tempAge = mfSummary.copy(deep = True)
tempAge = tempAge[['crsp_portno', 'age']]
tempAge = tempAge.groupby('crsp_portno').age.max().reset_index()
tempAge['age'] = tempAge.age.dt.days / 365

resampledFundSummary = pd.merge(left = resampledFundSummary, right = tempAge,
                                on = 'crsp_portno', how = 'left')
# =============================================================================
# Merge resampledFundSummary to tempMfReturns
# Log-transform NAV, TNA, and Age
# =============================================================================

tempMfReturns = pd.merge(left = tempMfReturns, right = resampledFundSummary,\
                         on = ['crsp_portno', 'slided_caldt'], how = 'left')
#del resampledFundSummary
tempMfReturns['nav_latest'] = np.log(tempMfReturns.nav_latest)
tempMfReturns['tna_latest'] = np.log(tempMfReturns.tna_latest)
tempMfReturns['age'] = np.log(tempMfReturns.age.astype(float))
tempMfReturns.rename({'nav_latest': 'log(nav)', 'tna_latest':'log(tna)',\
                      'age':'log(Age)', 'exp_ratio': 'Expense Ratio',\
                      'turn_ratio': 'Turnover Ratio'}, axis = 1, inplace = True)
    
#%%
# =============================================================================
# Section 4: Prepare four panel dataframes ready for panel data regression by 
# merging fund AFP (the output of the function "calculateTrade_BasedAFP" and  "calculateIndexBasedAFP")
# to tempMfReturns
# Main outcome variables: PanelDataIndexBased, PanelDataTrade3M, PanelDataTrade6M,
# PanelDataTrade12M
# =============================================================================    
    
PanelDataIndexBased = pd.merge(left = tempMfReturns, right = indexAFP_frame,
                               left_on = ['crsp_portno', 'slided_caldt'],\
                               right_on = ['crsp_portno', 'slided_report_dt'],
                               how = 'inner').drop(['slided_report_dt', 'caldt']\
                                            , axis =1)
    
PanelDataTrade3M = pd.merge(left = tempMfReturns, right = tradeAFP3M_frame,\
                            left_on = ['crsp_portno', 'slided_caldt'],\
                            right_on = ['crsp_portno','slided_report_dt'],\
                            how = 'inner').drop(['slided_report_dt', 'caldt']\
                                            , axis =1)

PanelDataTrade6M = pd.merge(left = tempMfReturns, right = tradeAFP6M_frame,\
                            left_on = ['crsp_portno', 'slided_caldt'],\
                            right_on = ['crsp_portno','slided_report_dt'],\
                            how = 'inner').drop(['slided_report_dt', 'caldt']\
                                            , axis =1)


PanelDataTrade12M = pd.merge(left = tempMfReturns, right = tradeAFP12M_frame,\
                            left_on = ['crsp_portno', 'slided_caldt'],\
                            right_on = ['crsp_portno','slided_report_dt'],\
                            how = 'inner').drop(['slided_report_dt', 'caldt']\
                                            , axis =1)

#%%
# ==========================================================================================================================================================
# ==========================================================================================================================================================
# Section 5: Panel Data Regression Analysis 
# This section has two parts.
# In part 1 there is only one independent variable which is AFP
# In part 2 there are additionally fund characteristics and 
#           first order lag of dependent variable in the independent variables
# In each part, there are three action points:
#   - Choose AFP type: index-based, 3/6/12-month trade-based
#   - Choose the dependent variable: Carhart alpha or FF alpha
#   - Choose standard error clustering option: by fund or by time
# To make the choice, simply comment / uncomment corresponding lines of code
# ==========================================================================================================================================================
# ==========================================================================================================================================================

#%%
# =============================================================================
# Part 1: Panel Data Regression with AFP as the only independent variable
# =============================================================================
from linearmodels.panel import PanelOLS

# Please comment / uncomment the following if you want to perform the analysis with 
    # (1) index-based AFP
temp = PanelDataIndexBased.copy(deep = True)
    # (2) 3-month trade-based AFP
#temp = PanelDataTrade3M.copy(deep = True)
    # (3) 6-month trade-based AFP
#temp = PanelDataTrade6M.copy(deep = True)
    # (4) 12-month trade-based AFP
#temp = PanelDataTrade12M.copy(deep = True)


#Preparing the variables
temp = temp.set_index(['crsp_portno', 'slided_caldt'])
temp = temp[temp.AFP != -np.inf]
temp = temp[temp.AFP != np.inf]

# Please comment / uncomment the following if you want to choose for the dependent variable
# (1) Carhart Alpha
#mod = PanelOLS(temp.CarhartAlpha, temp.AFP, time_effects = True)
# (2) Fama-French Alpha
mod = PanelOLS(temp.FFAlpha, temp.AFP, time_effects = True)

# Please comment / uncomment the following if you want to choose for the standard errors
# (1) to be clustered by fund
#res = mod.fit(cov_type = 'clustered', cluster_entity = True)
# (2) to be clustered by time
res = mod.fit(cov_type = 'clustered', cluster_time = True)

#Print regression result
print(res)

del temp
#%%
# =============================================================================
# Part 2: Panel Data Regression with multiple independent variables:
# - AFP
# - Fund characteristics: log(TNA), log(Age), expense ratio, turnover ratio
# - First order lag of dependent variable
# 
# =============================================================================
from linearmodels.panel import PanelOLS

# Please comment / uncomment the following if you want to perform the analysis with 
    # (1) index-based AFP
temp = PanelDataIndexBased.copy(deep = True)
    # (2) 3-month trade-based AFP
#temp = PanelDataTrade3M.copy(deep = True)
    # (3) 6-month trade-based AFP
#temp = PanelDataTrade6M.copy(deep = True)
    # (4) 12-month trade-based AFP
#temp = PanelDataTrade12M.copy(deep = True)


#Preparing the variables
temp = temp.replace([np.inf, -np.inf], np.nan)
temp['CarhartAlpha1L'] = temp.groupby('crsp_portno')['CarhartAlpha'].shift(1)
temp['FFAlpha1L'] = temp.groupby('crsp_portno')['FFAlpha'].shift(1)
temp = temp.dropna()
temp = temp.set_index(['crsp_portno', 'slided_caldt'])

# Please comment / uncomment the following if you want to choose for the dependent variable
# (1) Carhart Alpha
#mod = PanelOLS(temp.CarhartAlpha, temp[['AFP', 'log(tna)',\
#                                        'log(Age)','weighted_exp_ratio',\
#                                        'weighted_turn_ratio', 'CarhartAlpha1L']], time_effects = True)
# 
# (2) FF Alpha
mod = PanelOLS(temp.FFAlpha, temp[['AFP', 'log(tna)',\
                                        'log(Age)', 'weighted_exp_ratio',\
                                        'weighted_turn_ratio', 'FFAlpha1L']], time_effects = True)
    

# Please comment / uncomment the following if you want to choose for the standard errors
# (1) to be clustered by fund
#res = mod.fit(cov_type = 'clustered', cluster_entity = True)
# (2) to be clustered by time
res = mod.fit(cov_type = 'clustered', cluster_time = True)

# Print regression result
print(res)

del temp

