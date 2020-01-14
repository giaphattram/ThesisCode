import pandas as pd
pd.options.display.max_columns = 10
import numpy as np
import statsmodels
import statsmodels.api as sm
import seaborn as sns
import datetime
from scipy import stats
import time
import gc
gc.enable()
import os
os.chdir("D:\Master's Thesis\Data")
os.getcwd()

#%%
# =============================================================================
# Section 1
# Process Mutual Fund Summary data
# Main variable: mfSummary
# =============================================================================

mfSummary = pd.read_csv("./Mutual Funds/[MF] Mutual Fund Summary 2001 - 2018.csv")

chosenFundByPer_Com = mfSummary.per_com.between(left = 95, right = 100, inclusive = True)


chosenFundByCrspObjectiveCode = mfSummary.crsp_obj_cd.isin(['EDYG', 'EDYI',\
                                                            'EDYB', 'EDCM',\
                                                            'EDCI', 'EDCS'])

#chosenSectorFunds = mfSummary.crsp_obj_cd.str.startswith("EDS")

mfSummary = mfSummary[chosenFundByPer_Com&(chosenFundByCrspObjectiveCode)]# | chosenSectorFunds)]

mfSummary = mfSummary[mfSummary.crsp_portno.notnull()]

# remove funds whose names containing index 
mfSummary = mfSummary[(mfSummary.fund_name.str.find('index')== -1)&\
                    (mfSummary.fund_name.str.find('Index')== -1)&\
                    (mfSummary.fund_name.str.find('S&P')== -1)&\
                    (mfSummary.fund_name.str.find('DOW')== -1)&\
                    (mfSummary.fund_name.str.find('Wilshire')== -1)&\
                    (mfSummary.fund_name.str.find('Russell')== -1)]

# remove funds whose names containing balanced or international
mfSummary = mfSummary[(mfSummary.fund_name.str.find('balanced') == -1)&\
                      (mfSummary.fund_name.str.find('Balanced') == -1)&\
                      (mfSummary.fund_name.str.find('international') == -1)&\
                      (mfSummary.fund_name.str.find('International') == -1)]


mfSummary['caldt'] = pd.to_datetime(mfSummary.caldt, format = "%d/%m/%Y")

mfSummary['first_offer_dt'] = pd.to_datetime(mfSummary.first_offer_dt, format = "%d/%m/%Y")

mfSummary['end_dt'] = pd.to_datetime(mfSummary.end_dt, format = "%d/%m/%Y")


mfSummary = mfSummary.sort_values(by = ['crsp_portno', 'crsp_fundno', 'caldt'])

mfSummary = mfSummary[~(mfSummary.fund_name.isnull() & mfSummary.crsp_portno.isnull() \
                      & mfSummary.ticker.isnull() & mfSummary.ncusip.isnull())]

mfSummary = mfSummary[['crsp_portno', 'crsp_fundno', 'crsp_obj_cd', 'caldt', 'nav_latest',\
       'nav_latest_dt', 'tna_latest', 'tna_latest_dt', 'asset_dt', 'per_com', \
       'per_pref', 'per_conv', 'per_eq_oth', 'cusip8', \
       'crsp_cl_grp', 'fund_name', 'ticker', 'ncusip', \
       'first_offer_dt', 'end_dt', 'dead_flag', 'actual_12b1', 'max_12b1', \
       'exp_ratio', 'mgmt_fee', 'turn_ratio', 'fiscal_yearend', 'sales_restrict']]

mfSummary['nav_latest'].replace(to_replace = -99, value = np.nan, inplace = True)
mfSummary['tna_latest'].replace(to_replace = -99, value = np.nan, inplace = True)
mfSummary['turn_ratio'].replace(to_replace = -99, value = np.nan, inplace = True)
mfSummary['exp_ratio'].replace(to_replace = -99, value = np.nan, inplace = True)

mfSummary['nav_latest'] = mfSummary.nav_latest.apply(lambda element: float(element) if isinstance(element, str) else element)

print("The total number of selected unique funds between 2001 and 2018: ", mfSummary.crsp_portno.nunique())

del chosenFundByCrspObjectiveCode, chosenFundByPer_Com

#%%
# =============================================================================
# Section 3.1
# Import Mutual Fund Portfolio Holdings data
# Make the dataset smaller by keeping on the funds that have been selected
# for the variable mfSummary. The selection method can be seen above.
# =============================================================================
mfHoldings = pd.read_csv("./Mutual Funds/[MF] CRSP Mutual Fund Holdings 2001 - 2018.csv", dtype = {'security_name': str, \
                         'cusip': str})
chosenFundList = mfSummary.crsp_portno.unique()
mfHoldings = mfHoldings[mfHoldings.crsp_portno.isin(chosenFundList)]
del chosenFundList

#%%
# =============================================================================
# Section 3.2
#Find the answers for two questions:
#1. The variable market_val in mutual fund holdings - is it market cap or value of the shares held?
#   Answer: It is the value of the share held
#2. Is the price (market_val / nbr_shares) the same for all funds at the same point in time
#   Answer: Largely yes. Sometimes there is some really minor differences in the decimal points
#
#The answers are arrived at by analysing two securities, namely microsoft and tencent holdings ltd
# =============================================================================
#Do the test on microsoft
#microsoftMFHoldings = mfHoldings.copy(deep = True)
#microsoftMFHoldings = microsoftMFHoldings[microsoftMFHoldings.cusip == '59491810']
#microsoftMFHoldings['price'] = microsoftMFHoldings.market_val / microsoftMFHoldings.nbr_shares
#microsoftMFHoldings = microsoftMFHoldings[['crsp_portno', 'report_dt', 'eff_dt', 'nbr_shares', 'market_val', 'price', 'security_name', 'cusip']]
#microsoftMFHoldings.sort_values(['report_dt', 'eff_dt'], inplace = True)

#Do the test on a security with no other identifier: Tencent Holdings Ltd ORD
#tencentHoldings = mfHoldings.copy(deep = True)
#tencentHoldings = tencentHoldings[tencentHoldings.security_name == 'Tencent Holdings Ltd ORD']
#tencentHoldings['price'] = tencentHoldings.market_val / tencentHoldings.nbr_shares
#tencentHoldings = tencentHoldings[['crsp_portno', 'report_dt', 'eff_dt', 'nbr_shares', 'market_val', 'price', 'security_name', 'cusip']]
#tencentHoldings.sort_values(['report_dt', 'eff_dt'], inplace = True)
#
#del microsoftMFHoldings, tencentHoldings
#%%
# =============================================================================
# Section: additional steps to clean holdings data
# find funds and dates (original report_dt) where sum(percent_tna) between 5% and 95% percentile 
# =============================================================================
total_percent_tna = mfHoldings.groupby(['crsp_portno', 'report_dt']).percent_tna.sum()

#Remove (crsp_portno, report_dt) wherein sum(percent_tna) = 0.
#Typically, the reason is due to missing value
total_percent_tna = total_percent_tna[total_percent_tna != 0]

print('5th percentile: ',total_percent_tna.quantile(0.05))
print('95th percentile: ',total_percent_tna.quantile(0.95))


total_percent_tna = total_percent_tna[total_percent_tna.between\
                                      (left = total_percent_tna.quantile(0.05), \
                                       right = total_percent_tna.quantile(0.95), \
                                       inclusive = True)]
                                      
mfHoldings.set_index(['crsp_portno', 'report_dt'], inplace = True)
                                      
mfHoldings = mfHoldings.loc[total_percent_tna.index]

mfHoldings = mfHoldings.reset_index()

del total_percent_tna

#%%
#%%
# =============================================================================
# Section 4.1: Generating variables 'security_price', 'month_year_str', 'month_year'
# =============================================================================
mfHoldings['security_price'] = mfHoldings.market_val / mfHoldings.nbr_shares

# =============================================================================
# Section 4.2: Generating Variables 'slided_report_dt'
# #Because mfHoldings has holding data on monthly basis, not just quarterly, two treatments are done:
#    - For each fund, each security, and each quarter, choose the observation closest to the end of the quarter
#    - Then create a variable 'slided_report_dt' that offsets all report_dt to the end of the quarter. 
#        This means that:
#            + if report_dt is 28/02, then it slides to 31/03
#            + if report_dt is 30/12, then it slides to 31/12
# Doing so allow for extracting the last active weight within a quarter
# =============================================================================

#Create a variable 'report_yearquarter' to extract year and quarter from report_dt
mfHoldings['report_dt'] = pd.to_datetime(mfHoldings.report_dt, format = '%d/%m/%Y')
mfHoldings['report_yearquarter'] = mfHoldings.report_dt.dt.year.astype(str) + 'Q' + mfHoldings.report_dt.dt.quarter.astype(str)
mfHoldings['report_yearquarter'] = mfHoldings.report_yearquarter.astype(str) # convert from Object to str to reduce memory

# Using 'report_yearquarter' and groupby, select the latest date in each quarter 
# This will be done again below using permno
mfHoldings = mfHoldings.sort_values(['crsp_portno', 'security_name', 'report_dt'])
mfHoldings = mfHoldings.loc[mfHoldings.groupby(['crsp_portno', 'security_name', 'report_yearquarter']).report_dt.idxmax()]



#Create dummy "isQuarterEnd' to indicate if report_dt is quarter end
    #The reason is, if report_dt is quarter end, then + pd.tseries.offsets.QuarterEnd()
    #will offset the report_dt to the next quarter end.
    #So only use +pd..QuarterEnd() on the report_dt that is not quarter end
#Create 'slided_report_dt'
mfHoldings['slided_report_dt'] = np.where(~mfHoldings.report_dt.dt.is_quarter_end, \
                                  mfHoldings.report_dt + pd.tseries.offsets.QuarterEnd(), \
                                  mfHoldings.report_dt)
#%%
# Remove some columns to keep mfHoldings light
#mfHoldings.drop(['ticker', 'coupon', 'maturity_dt', 'eff_dt', 'security_rank'], axis = 1, inplace = True)

#%%
crspStockHeader = pd.read_csv("./Securities/[stock] CRSP Stock Header.csv")
crspStockHeader.drop('HTICK', axis = 1, inplace = True)
crspStockHeader['BEGDAT'] = pd.to_datetime(crspStockHeader.BEGDAT, format = "%Y%m%d")
crspStockHeader['ENDDAT'] = pd.to_datetime(crspStockHeader.ENDDAT, format = "%Y%m%d")
crspStockHeader = pd.melt(crspStockHeader, id_vars = ['PERMNO', 'PERMCO', 'CUSIP', 'HCOMNAM'], value_name = "Date")
crspStockHeader.sort_values(['PERMCO', 'CUSIP', 'variable'], inplace = True)
crspStockHeader.set_index('Date', inplace = True)
crspStockHeader = crspStockHeader.groupby(['PERMNO', 'PERMCO', 'CUSIP']).resample('D').ffill()
crspStockHeader.reset_index(level = [0,1,2], drop = True, inplace = True)
crspStockHeader.reset_index(inplace = True)
#%%
# =============================================================================
# Section: Fill in missing Permno in 2 ways:
#   - Sometimes permno is available for some dates and misisng for other dates. 
#     In this case, use ffill and bfill
#   - Sometimes permno is not available but cusip is
#     In this case use crsp stock header to map permno to these cusip
# =============================================================================
mfHoldings_wPermno = mfHoldings.copy(deep = True)
mfHoldings_woPermno = mfHoldings.copy(deep = True)

mfHoldings_wPermno = mfHoldings_wPermno[mfHoldings_wPermno.permno.notnull()]
mfHoldings_woPermno = mfHoldings_woPermno[mfHoldings_woPermno.permno.isnull()]

#Map permno to cusip
mfHoldings_woPermno_wCUSIP = mfHoldings_woPermno.copy(deep = True)
mfHoldings_woPermno_woCUSIP = mfHoldings_woPermno.copy(deep = True)

mfHoldings_woPermno_wCUSIP = mfHoldings_woPermno_wCUSIP[mfHoldings_woPermno_wCUSIP.cusip.notnull()]
mfHoldings_woPermno_woCUSIP = mfHoldings_woPermno_woCUSIP[mfHoldings_woPermno_woCUSIP.cusip.isnull()]
    
mfHoldings_woPermno_wCUSIP = pd.merge(left = mfHoldings_woPermno_wCUSIP, right = crspStockHeader,\
                                      left_on = ['report_dt', 'cusip'], right_on = ['Date', 'CUSIP'],\
                                      how = 'left')

mfHoldings_woPermno_wCUSIP['permno'].fillna(mfHoldings_woPermno_wCUSIP.PERMNO, inplace = True)
mfHoldings_woPermno_wCUSIP.drop(['Date', 'PERMNO', 'PERMCO', 'CUSIP', 'HCOMNAM'], \
                                 axis = 1, inplace = True)
mfHoldings_woPermno_wCUSIP.drop('variable', axis = 1, inplace = True)

mfHoldings_woPermno = pd.concat([mfHoldings_woPermno_wCUSIP, mfHoldings_woPermno_woCUSIP], axis = 0)
del mfHoldings_woPermno_woCUSIP, mfHoldings_woPermno_wCUSIP

#Map permno to permco
#Actually, if permno is not available, permco is not available too. I've checked

# Map permno to security_name
# At a date for a security_name, there could be many permno => take care of this later
# Only about 20000 obs will have permno added anyway 

#crspStockHeader = crspStockHeader[['Date', 'PERMNO', 'HCOMNAM']]
#crspStockHeader.drop_duplicates(keep = 'first', inplace = True)
#
#mfHoldings_woPermno = pd.merge(left = mfHoldings_woPermno, right = crspStockHeader,\
#                               left_on = ['report_dt', 'security_name'], \
#                               right_on = ['Date', 'HCOMNAM'], how = 'left')
#mfHoldings_woPermno['permno'].fillna(mfHoldings_woPermno.PERMNO, inplace = True)
#mfHoldings_woPermno.drop(['Date', 'PERMNO', 'PERMCO', 'CUSIP', 'HCOMNAM', 'variable'], axis =1, inplace = True)


#Combine into mfHoldings
mfHoldings = pd.concat([mfHoldings_wPermno, mfHoldings_woPermno], axis = 0)
del crspStockHeader
# =============================================================================
# Now that as many permno as possible have been mapped, do some cleaning operations:
# - for each fund-permno-quarter, choose the row whose report_dt closest to quarter end
# - for each fund-permno-report_dt, choose the row with the last eff_dt
# =============================================================================
mfHoldings['eff_dt'] = pd.to_datetime(mfHoldings.eff_dt, format = "%d/%m/%Y")

mfHoldings_wPermno = mfHoldings.copy(deep = True)
mfHoldings_woPermno = mfHoldings.copy(deep = True)

mfHoldings_wPermno = mfHoldings_wPermno[mfHoldings_wPermno.permno.notnull()]
mfHoldings_woPermno = mfHoldings_woPermno[mfHoldings_woPermno.permno.isnull()]

#For each fund-security-quarter, choose the last reported holding in that quarter
# This has been done before with security_name. Now that there are more permno, 
# we do it on permno level
mfHoldings_wPermno.sort_values(['crsp_portno', 'permno', 'report_dt'], inplace = True)
mfHoldings_wPermno = mfHoldings_wPermno.groupby(['crsp_portno', 'permno', 'report_yearquarter']).last()
mfHoldings_wPermno.reset_index(inplace = True)
#mfHoldings_wPermno = mfHoldings_wPermno.loc[mfHoldings_wPermno.groupby(['crsp_portno', 'permno', 'report_yearquarter']).report_dt.idxmax()] # this takes too long

# Occasionally, a fund-stock-reportdate can have two eff-dt, in this case we choose 
# the last eff_dt
mfHoldings_wPermno.sort_values(['crsp_portno', 'permno', 'report_dt', 'eff_dt'], inplace = True)
mfHoldings_wPermno = mfHoldings_wPermno.groupby(['crsp_portno' ,'permno', 'report_dt']).last()
mfHoldings_wPermno.reset_index(inplace = True)
#mfHoldings_wPermno = mfHoldings_wPermno.loc[mfHoldings_wPermno.groupby(['crsp_portno', 'report_dt', 'permno']).eff_dt.idxmax()] #this takes too long

print("crsp_portno-report_dt-permno has no duplicate: ",\
      mfHoldings_wPermno[mfHoldings_wPermno.duplicated(['crsp_portno', 'report_dt', 'permno'], keep = False)].shape[0] == 0)

mfHoldings_wPermno = mfHoldings_wPermno[mfHoldings_woPermno.columns]

mfHoldings = pd.concat([mfHoldings_wPermno, mfHoldings_woPermno], axis = 0)

del mfHoldings_woPermno, mfHoldings_wPermno

#%%
dailyStock = pd.read_csv('./Securities/[stock] CRPS - Daily Stock 2001 - 2018.csv')
dailyStock['date'] = pd.to_datetime(dailyStock.date, format = "%d/%m/%Y")
dailyStock = dailyStock[dailyStock.PERMNO.isin(mfHoldings.permno.unique())]
dailyStock = dailyStock[dailyStock.date.isin(mfHoldings.report_dt.unique())]
dailyStock = dailyStock[['PERMNO', 'date', 'PRC']]

mfHoldings = pd.merge(left = mfHoldings, right = dailyStock,\
                      left_on = ['permno', 'report_dt'], right_on = ['PERMNO', 'date'],\
                      how = 'left')

mfHoldings['security_price'] = np.where((mfHoldings.market_val == 0) & mfHoldings.nbr_shares > 0,\
                                     mfHoldings.PRC, mfHoldings.security_price)

mfHoldings['market_val'] = np.where((mfHoldings.market_val == 0) & mfHoldings.nbr_shares > 0,\
                                     mfHoldings.PRC * mfHoldings.nbr_shares, mfHoldings.market_val)

mfHoldings.drop(['PERMNO', 'date'], axis = 1, inplace = True)

del dailyStock
#%%
# =============================================================================
# Section 4.3: Shift price back to adjust for passive weight 
# =============================================================================
mfHoldings_wPermno = mfHoldings.copy(deep = True)
mfHoldings_woPermno = mfHoldings.copy(deep = True)

mfHoldings_wPermno = mfHoldings_wPermno[mfHoldings_wPermno.permno.notnull()]
mfHoldings_woPermno = mfHoldings_woPermno[mfHoldings_woPermno.permno.isnull()]


priceFromForward3M = mfHoldings_wPermno.copy(deep = True)
priceFromForward3M = priceFromForward3M[['crsp_portno', 'security_name', 'slided_report_dt', 'security_price']]
priceFromForward3M.rename({'security_price': 'priceFromForward3M'}, axis = 1, inplace = True)
priceFromForward3M['slided_report_dt'] = priceFromForward3M.slided_report_dt - pd.offsets.DateOffset(months = 3)
priceFromForward3M['slided_report_dt'] = np.where(priceFromForward3M.slided_report_dt.dt.is_quarter_end,\
                                                  priceFromForward3M.slided_report_dt,\
                                                  priceFromForward3M.slided_report_dt + pd.tseries.offsets.QuarterEnd())

priceFromForward6M = mfHoldings_wPermno.copy(deep = True)
priceFromForward6M = priceFromForward6M[['crsp_portno', 'security_name', 'slided_report_dt', 'security_price']]
priceFromForward6M.rename({'security_price': 'priceFromForward6M'}, axis = 1, inplace = True)
priceFromForward6M['slided_report_dt'] = priceFromForward6M.slided_report_dt - pd.offsets.DateOffset(months = 6)
priceFromForward6M['slided_report_dt'] = np.where(priceFromForward6M.slided_report_dt.dt.is_quarter_end,\
                                                  priceFromForward6M.slided_report_dt,\
                                                  priceFromForward6M.slided_report_dt + pd.tseries.offsets.QuarterEnd())

priceFromForward12M = mfHoldings_wPermno.copy(deep = True)
priceFromForward12M = priceFromForward12M[['crsp_portno', 'security_name', 'slided_report_dt', 'security_price']]
priceFromForward12M.rename({'security_price': 'priceFromForward12M'}, axis = 1, inplace = True)
priceFromForward12M['slided_report_dt'] = priceFromForward12M.slided_report_dt - pd.offsets.DateOffset(months = 12)
priceFromForward12M['slided_report_dt'] = np.where(priceFromForward12M.slided_report_dt.dt.is_quarter_end,\
                                                  priceFromForward12M.slided_report_dt,\
                                                  priceFromForward12M.slided_report_dt + pd.tseries.offsets.QuarterEnd())

mfHoldings_wPermno = pd.merge(left = mfHoldings_wPermno, right = priceFromForward3M,\
                              on = ['crsp_portno', 'security_name', 'slided_report_dt'], how = 'left')
mfHoldings_wPermno = pd.merge(left = mfHoldings_wPermno, right = priceFromForward6M,\
                              on = ['crsp_portno', 'security_name', 'slided_report_dt'], how = 'left')
mfHoldings_wPermno = pd.merge(left = mfHoldings_wPermno, right = priceFromForward12M,\
                              on = ['crsp_portno', 'security_name', 'slided_report_dt'], how = 'left')

del priceFromForward3M, priceFromForward6M, priceFromForward12M


mfHoldings_woPermno['priceFromForward3M'] = np.nan
mfHoldings_woPermno['priceFromForward6M'] = np.nan
mfHoldings_woPermno['priceFromForward12M'] = np.nan

mfHoldings_wPermno = mfHoldings_wPermno[mfHoldings_woPermno.columns]

mfHoldings = pd.concat([mfHoldings_wPermno, mfHoldings_woPermno], axis = 0)

del mfHoldings_wPermno, mfHoldings_woPermno
#%%
# =============================================================================
# Section 4.4
# Test if percent_tna = market_val / (sum of market_val)
# Answer: Largely correct. Most of the time the differences are from rounding
# up the decimals. Sometimes the difference can be slightly larger; this typically
# happens when there are many missing values for percent_tna within the same day.
# Even when this is the case, the difference is still negligible 
# Conclusion: just calculate a variable weight = market_val / (sum of market_val)
# Especially when market_val doesn't have missing value
# =============================================================================
#Copy paste from above to keep the variable light for the merging operation
mfHoldings['month_year'] = mfHoldings.slided_report_dt - pd.tseries.offsets.MonthBegin()

tempHoldings = mfHoldings.copy(deep = True)
total_market_val = tempHoldings.groupby(['crsp_portno', 'slided_report_dt']).market_val.sum()
total_market_val = total_market_val.to_frame().reset_index()
total_market_val.rename({'market_val' : 'total_market_val'}, axis = 1, inplace = True)
tempHoldings = pd.merge(left = tempHoldings, right = total_market_val, on = ['crsp_portno', 'slided_report_dt'], how = 'left')
tempHoldings['weight'] = tempHoldings.market_val / tempHoldings.total_market_val

tempHoldings['value3M'] = tempHoldings.nbr_shares * tempHoldings.priceFromForward3M
tempHoldings['value6M'] = tempHoldings.nbr_shares * tempHoldings.priceFromForward6M
tempHoldings['value12M'] = tempHoldings.nbr_shares * tempHoldings.priceFromForward12M

tempHoldings['passiveWeight3M'] = tempHoldings.value3M.divide(tempHoldings.total_market_val)
tempHoldings['passiveWeight6M'] = tempHoldings.value6M.divide(tempHoldings.total_market_val)
tempHoldings['passiveWeight12M'] = tempHoldings.value12M.divide(tempHoldings.total_market_val)

tempHoldings.drop(['value3M', 'value6M', 'value12M'], axis = 1, inplace = True)

mfHoldings = tempHoldings.copy(deep = True)

#**************************************
# Super important: try using percent_tna instead of weight
#**************************************
#mfHoldings['weight'] = mfHoldings.percent_tna


del total_market_val, tempHoldings

#%%
# =============================================================================
# Section 4.3
# Import dataframe threeDayCARDf from calculatingCAR.py
# Generate a variable 'slided_rdq' that slides the earning announcement date to the last day
# of the previous quarter. 
# This variable 'slided_rdq' will be mapped to 'slided_report_dt' to calculate AFP
# =============================================================================

#Run calculatingCAR.py and get threeDayCARDf
exec(compile(open("D:\Master's Thesis\Code\calculatingCAR.py", 'rb').read(), 'calculatingCAR.py', 'exec'))
threeDayCARDf = calculatingCAR()

#Slide the rdq to the first day of the quarter
#threeDayCARDf['slided_rdq'] = pd.to_datetime(threeDayCARDf.rdq, format = '%d/%m/%Y')
#threeDayCARDf['slided_rdq'] = threeDayCARDf.slided_rdq - pd.tseries.offsets.QuarterBegin(startingMonth = 1)
threeDayCARDf['slided_rdq'] = threeDayCARDf.rdq - pd.tseries.offsets.QuarterBegin(startingMonth = 1)

#Note that the 2 lines above will allow CAR of all three months of the next quarter to be available for AFP calculation
#Run the line below if I want to strictly allow only two months after a quarter end 
threeDayCARDf = threeDayCARDf[~threeDayCARDf.rdq.dt.month.isin([7, 9, 12, 3])]
              
#Move the slided rdq back one day so that the day becomes the last day of the previous quarter
threeDayCARDf['slided_rdq'] = threeDayCARDf.slided_rdq - pd.DateOffset(1)




#%%
#%%
# =============================================================================
# Section 8: Import and Calculate Mutual Fund Monthly Returns 
# =============================================================================
mfReturns = pd.read_csv('./Mutual Funds/[MF] Mutual Fund Monthly Returns 2001 - 2018.csv')

mfReturns = mfReturns[~(mfReturns.mret == 'R')]

mfReturns['mret'] = mfReturns.mret.astype(float)

mfReturns['caldt'] = pd.to_datetime(mfReturns.caldt, format = "%d/%m/%Y")

mfFundPortMapping = pd.read_excel("./Mutual Funds/[MF] CRSP Portno-Fundno Mapping.xlsx")

mfFundPortMapping['StartDate'] = pd.to_datetime(mfFundPortMapping.StartDate, format = "%Y%m%d")

mfFundPortMapping['EndDate'] = pd.to_datetime(mfFundPortMapping.EndDate, format = "%Y%m%d")

mfFundPortMapping = pd.melt(mfFundPortMapping, \
                                           id_vars = ['crsp_fundno', 'crsp_portno', 'Fund Name'],\
                                           value_name = 'Date').drop('variable', axis = 1)

mfFundPortMapping.sort_values(['crsp_fundno', 'crsp_portno', 'Date'], inplace = True)

mfFundPortMapping.set_index('Date', inplace = True)

mfFundPortMapping = mfFundPortMapping.groupby(['crsp_fundno', 'crsp_portno']).resample('D').ffill()

mfFundPortMapping.reset_index(level = [0,1], drop = True, inplace = True)

mfFundPortMapping.reset_index(inplace = True)

mfReturns = pd.merge(left = mfReturns, right = mfFundPortMapping,\
                     left_on = ['caldt', 'crsp_fundno'],\
                     right_on = ['Date', 'crsp_fundno'],\
                     how = 'left')

mfReturns.drop('Date', axis = 1, inplace = True)

mfReturns = mfReturns[mfReturns.crsp_portno.notnull()]

mfReturns = mfReturns[mfReturns.crsp_portno.isin(mfSummary.crsp_portno.unique())]

# replace where mtna = 'T' with np.nan convert mtna to float
mfReturns['mtna'] = np.where(mfReturns.mtna == 'T', np.nan, mfReturns.mtna)
mfReturns['mtna'] = mfReturns.mtna.astype(float)

# Remove rows with missing mret
mfReturns = mfReturns[mfReturns.mret.notnull()]

mfReturns.sort_values(['crsp_portno', 'caldt'], inplace = True)

missingTNA = mfReturns[(mfReturns.mtna.isnull())|(mfReturns.mtna == 0)|\
                 (mfReturns.mtna == -99)][['caldt', 'crsp_portno']].drop_duplicates(keep = 'first')

missingTNA = missingTNA.set_index(['caldt', 'crsp_portno'])

mfReturns.set_index(['caldt', 'crsp_portno'], inplace = True)

print('Before spliting: ',mfReturns.shape)

missingTNA = mfReturns.loc[missingTNA.index]

mfReturns.drop(missingTNA.index, axis = 0, inplace = True)
mfReturns.reset_index(inplace = True)

print('After spliting, missingTNA: ',missingTNA.shape)
print('After spliting, mfReturns: ',mfReturns.shape)

missingTNA['mtna'] = np.where(missingTNA.mtna.isin([-99, 0]),\
                              np.nan, missingTNA.mtna)

missingTNA.reset_index(inplace = True)

missingTNA_min_tna = missingTNA.groupby(['caldt', 'crsp_portno']).mtna.min().reset_index()
missingTNA_min_tna.rename({'mtna': 'min_tna'}, axis = 1, inplace = True)

missingTNA = pd.merge(left = missingTNA, right = missingTNA_min_tna,\
                      on = ['caldt', 'crsp_portno'], how = 'left')

missingTNA['mtna'] = np.where(missingTNA.mtna.isnull(), missingTNA.min_tna, missingTNA.mtna)

missingTNA['mtna'] = np.where(missingTNA.mtna.isnull(), 1, missingTNA.mtna)

missingTNA.drop('min_tna', axis = 1, inplace = True)

print("Before concat, len(mfReturns): ",len(mfReturns))
print("Before concat, len(missingTNA): ",len(missingTNA))

mfReturns = pd.concat([mfReturns, missingTNA], axis = 0)

print("After concat, len(mfReturns): ", len(mfReturns))

crsp_portno_tna = mfReturns.groupby(['crsp_portno', 'caldt']).mtna.sum()\
                            .reset_index().rename({'mtna':'total_mtna'}, axis = 1)

mfReturns = pd.merge(left = mfReturns, right = crsp_portno_tna,\
                     on = ['crsp_portno', 'caldt'], how = 'left')


mfReturns.sort_values(['crsp_portno', 'caldt'], inplace= True)

mfReturns['weighted_mret'] = mfReturns.mret.multiply(mfReturns.mtna.divide(mfReturns.total_mtna))

mfReturns = mfReturns.groupby(['crsp_portno', 'caldt'])['weighted_mret'].sum()

mfReturns = mfReturns.to_frame().reset_index()

mfReturns.rename({"weighted_mret" : "mret"}, axis = 1, inplace = True)


print('mfReturns contains {} duplicates'.format(mfReturns.duplicated(keep = False).sum()))

del mfFundPortMapping
del crsp_portno_tna, missingTNA, missingTNA_min_tna

#%%
# =============================================================================
# Section 
# Descriptive Statistics
# =============================================================================
mfSummary = mfSummary[mfSummary.crsp_portno.isin(mfHoldings.crsp_portno.unique())]
#===Calculate Age
#Calculating descriptive statistics for age
#Method: calculate the distance between the date of the last NAV reported and the date when a shareclass (crsp_fundno) of a fund is open.
#Then, for each crsp_portno, choose the crsp_fundno with the maximum age. Take this is the age of the fund.
#In other word, the age of the fund is the distance between the day of the last NAV reported and the day the first shareclass of that fund is open
mfSummary['age'] = mfSummary.end_dt - mfSummary.first_offer_dt
mfAge = mfSummary.groupby('crsp_fundno').first()
mfAge.reset_index(inplace = True)
mfAge = mfAge[['crsp_portno', 'crsp_fundno', 'age']]
mfAge['age'] = mfAge['age'].dt.days/365
mfAge = mfAge.groupby('crsp_portno')['age'].max()
mfAge = mfAge.to_frame().reset_index()

#===Calculate Summary Statistics
mfSummary['monthyear'] = mfSummary.caldt.dt.strftime('%m/%Y')
mfRatio = mfSummary.copy(deep = True)
mfRatio = mfRatio[['crsp_portno', 'crsp_fundno', 'monthyear', 'exp_ratio', 'turn_ratio', 'tna_latest']]
mfRatio.rename({'tna_latest': 'tna'}, axis = 1, inplace = True)

mfValue = mfSummary.copy(deep = True)
mfValue = mfValue[['crsp_portno', 'crsp_fundno', 'caldt', 'monthyear', 'nav_latest', 'tna_latest']]
mfValue = mfValue.groupby(['crsp_portno', 'monthyear'])[['nav_latest', 'tna_latest']].sum()
mfValue.reset_index(inplace = True)

mfRatio = pd.merge(left = mfRatio, right = mfValue,\
                   on = ['crsp_portno', 'monthyear'], how = 'left')
mfRatio['tna_weight'] = mfRatio.tna / mfRatio.tna_latest
mfRatio.sort_values(['crsp_portno', 'monthyear'], inplace = True)
mfRatio['weighted_exp_ratio'] = mfRatio.exp_ratio * mfRatio.tna_weight
mfRatio['weighted_turn_ratio'] = mfRatio.turn_ratio * mfRatio.tna_weight

mfRatio = mfRatio.groupby(['crsp_portno', 'monthyear'])['weighted_exp_ratio', 'weighted_turn_ratio'].sum()
mfRatio.reset_index(inplace = True)

mfDescriptiveStats = pd.merge(left = mfRatio, right = mfValue, on = ['crsp_portno', 'monthyear'], how = 'left')
mfDescriptiveStats = pd.merge(left = mfDescriptiveStats, right = mfAge, on = 'crsp_portno', how = 'left')

tempReturns = mfReturns.copy(deep = True)
#tempReturns['quarteryear'] = tempReturns.caldt.dt.year.astype(str) + "/" + tempReturns.caldt.dt.quarter.astype(str)
tempReturns['quarter'] = tempReturns.caldt.dt.quarter
tempReturns['year'] = tempReturns.caldt.dt.year
tempReturns['mret'] = tempReturns.mret + 1
tempReturns = tempReturns.groupby(['crsp_portno', 'quarter', 'year'])['mret'].prod()
tempReturns = tempReturns.to_frame().reset_index()
tempReturns['mret'] = tempReturns.mret - 1

conditions = [tempReturns.quarter == 1,
              tempReturns.quarter == 2,
              tempReturns.quarter == 3,
              tempReturns.quarter == 4]

choices = ["03","06","09","12"]

tempReturns['month'] = np.select(conditions, choices)
tempReturns['monthyear'] = tempReturns.month + "/" + tempReturns.year.astype(str)
tempReturns.drop(['quarter', 'year', 'month'], axis = 1, inplace = True)

 

mfDescriptiveStats = mfDescriptiveStats.describe().T
mfDescriptiveStats.drop('crsp_portno', axis = 0, inplace = True)
mfDescriptiveStats = pd.concat([mfDescriptiveStats, tempReturns.describe().T], axis = 0)
mfDescriptiveStats.drop('crsp_portno', axis = 0, inplace = True)
mfDescriptiveStats.drop(['count', 'min', 'max'], axis = 1, inplace = True)


mfDescriptiveStats = mfDescriptiveStats.T

mfDescriptiveStats['weighted_exp_ratio'] = mfDescriptiveStats.weighted_exp_ratio * 100
mfDescriptiveStats['weighted_turn_ratio'] = mfDescriptiveStats.weighted_turn_ratio * 100
mfDescriptiveStats['mret'] = mfDescriptiveStats.mret * 100

mfDescriptiveStats = mfDescriptiveStats.T


# =============================================================================
# #=== Calculate Correlation
# =============================================================================
#First, calculate quarterly holding returns

mfMerged = pd.merge(left = mfRatio, right = mfValue, on = ['crsp_portno', 'monthyear'], how = 'inner')
mfMerged = pd.merge(left = mfMerged, right = mfAge, on = 'crsp_portno', how = 'inner')
mfMerged = pd.merge(left = mfMerged, right = tempReturns, on = ['crsp_portno', 'monthyear'], how = 'inner')

#Correlation is calculated in two ways. The first way follows the paper.

#Correlation - First way
#The paper calculates the time-series average of the cross-sectional correlation.
tempGroupbyObj = mfMerged.groupby('monthyear')['nav_latest', 'tna_latest', 'age', 'weighted_exp_ratio', 'weighted_turn_ratio', 'mret']
tempCorrList = []
for name, grouped in tempGroupbyObj:
    tempCorrList.append(grouped[['nav_latest', 'tna_latest', \
                                 'age', 'weighted_exp_ratio', \
                                 'weighted_turn_ratio', 'mret']]\
                        .corr(method = 'spearman').values.flatten())
tempCorr = pd.DataFrame(tempCorrList, columns = np.arange(1,37))
tempCorr = list(tempCorr.mean())#.to_frame().unstack()
mfCorrelationPaper = pd.DataFrame(columns = ['nav_latest', 'tna_latest', 'age', 'weighted_exp_ratio', 'weighted_turn_ratio', 'mret'], index = ['nav_latest', 'tna_latest', 'age', 'weighted_exp_ratio', 'weighted_turn_ratio', 'mret'])
count = 0
for index in mfCorrelationPaper.index:
    for column in mfCorrelationPaper.columns:
        mfCorrelationPaper.loc[index][column] = tempCorr[count]
        count += 1


#Correlation - Second way
#Another way to compute correlation: cross-sectional correlation of the time-series average of these variables
tempGroupbyObj = mfMerged.groupby('crsp_portno')['nav_latest', 'tna_latest', 'age', 'weighted_exp_ratio', 'weighted_turn_ratio'].mean()
mfCorrelationAlternative = tempGroupbyObj.corr()
#mfCorrelationFlattened = mfCorrelationAlternative.values.flatten()

writer = pd.ExcelWriter("MFDescriptiveStats.xlsx")
mfDescriptiveStats.to_excel(writer, sheet_name = 'Summary')
mfCorrelationPaper.to_excel(writer, sheet_name = 'Correlation_Paper')
mfCorrelationAlternative.to_excel(writer, sheet_name = 'Correlation_Alternative')
writer.save()

del name, grouped, count, index, column
del tempCorrList, tempGroupbyObj, tempCorr
del mfAge, mfRatio, mfValue, tempReturns
del mfMerged

del mfCorrelationAlternative, mfCorrelationPaper, mfDescriptiveStats

#%%
# =============================================================================
# Section 4.4: Attempting to understand domestic equity, foreign equity, bonds
# =============================================================================
mfHoldings['is_fixed_income'] = np.where((~mfHoldings.coupon.isnull())|(~mfHoldings.maturity_dt.isnull()), True, False)
print('The number of unique security names: {}'.format(mfHoldings.security_name.nunique()))
print('The number of unique fixed income assets (based on non-na security names): {}'.format(mfHoldings[mfHoldings.is_fixed_income == True]['security_name'].nunique()))
print('Number of unique permno in mutual fund holdings data: {}'.format(mfHoldings.permno.nunique()))
print('Number of unique permno in calculated CAR dataset: {}'.format(threeDayCARDf.PERMNO.nunique()))
print('Number of unique permno in the intersection of two datasets: ',len(list(set(mfHoldings.permno.unique()).intersection(set(threeDayCARDf.PERMNO.unique())))))
set1 = set(mfHoldings[~mfHoldings.maturity_dt.isnull()].index)
set2 = set(mfHoldings[~mfHoldings.coupon.isnull()].index)
print(len(set1))
print(len(set2))
len(list(set1.intersection(set2)))

#%%
# =============================================================================
# Section 5.1
# Import and process Factor Model
# =============================================================================
ThreeFactorModel = pd.read_excel("./Factor Models/F-F_Research_Data_Factors.xlsx")
Momentum = pd.read_excel("./Factor Models/F-F_Momentum_Factor.xlsx")

FactorModel = pd.merge(left = ThreeFactorModel, right = Momentum, \
                       on = 'month_year', how = 'inner', indicator = True)
print('Merging Momentum and Three factor model successful: {}'.format(FactorModel._merge.nunique()==1))
FactorModel.drop("_merge", axis = 1, inplace = True)
#liq = pd.read_csv("./Factor Models/liq_data_1962_2017.txt", delim_whitespace=True)
liq = pd.read_csv("./Factor Models/liq_data_1962_2018.txt", delim_whitespace=True)
liq = liq.replace(-99, np.nan)
liq['Month'] = liq.Month.astype(str)
#liq['Year'] = 
liq = liq[liq.Month.str.slice(stop = 4).astype(int) >= 2001]
liq.rename({'Month': 'month_year', 'TradedLiq(LIQ_V)':'Liq'}, axis =1 ,inplace = True)
liq['month_year'] = liq.month_year.astype(int)
FactorModel = pd.merge(left = FactorModel, right = liq,\
                       on = 'month_year', how = 'left', indicator = True)
#del liq

#Convert month_year to pandas datetime and slide it to month end
FactorModel['date'] = pd.to_datetime(FactorModel.month_year, format = "%Y%m")
FactorModel['date'] = FactorModel.date + pd.tseries.offsets.MonthEnd()
FactorModel = FactorModel[FactorModel.date.dt.year >= 2001]
FactorModel.drop('month_year', axis = 1, inplace = True)
FactorModel = FactorModel[['date', 'Mkt-RF', 'SMB', 'HML', 'RF', 'Mom', 'Liq']]
print("No missing data for Momentum: ",FactorModel[FactorModel.Mom.isin([-99.99, -999])].shape[0]==0)
del ThreeFactorModel, Momentum

# =============================================================================
# Section 5.2
# Create a variable slided_date that slide the date to quarterly dates when
# AFP is computed. This slided_date is for mapping.
# Create variable FactorPortfolioReturns: calculate the returns from 
# holding the factor portfolios during the intended period
# =============================================================================
conditions = [FactorModel.date.dt.month.isin([3,6,9,12]),\
              FactorModel.date.dt.month.isin([1,4,7,10]),\
              FactorModel.date.dt.month.isin([2,5,8,11])]

choices = [pd.offsets.DateOffset(months = 3),\
           pd.offsets.DateOffset(months = 4),\
           pd.offsets.DateOffset(months = 5)]

FactorModel['slided_date']= np.select(conditions, choices)
FactorModel['slided_date'] = FactorModel.date - FactorModel.slided_date
FactorModel['slided_date'] = np.where(FactorModel.slided_date.dt.is_month_end == False,\
                                       FactorModel.slided_date + pd.tseries.offsets.MonthEnd(),\
                                       FactorModel.slided_date)
FactorModel = FactorModel[['date', 'slided_date', 'Mkt-RF', 'SMB', 'HML', 'RF', 'Mom', 'Liq']]
del conditions, choices

for eachColumn in FactorModel.columns:
    if (eachColumn != 'date') & (eachColumn != 'slided_date'):
        FactorModel[eachColumn] = (FactorModel[eachColumn]/100)+1
del eachColumn

FactorPortfolioReturns = FactorModel.groupby(['slided_date'])['Mkt-RF', 'SMB', 'HML', 'RF', 'Mom', 'Liq'].prod()
FactorPortfolioReturns = FactorPortfolioReturns.reset_index()

for eachColumn in FactorPortfolioReturns.columns:
    if eachColumn in ['Mkt-RF', 'SMB', 'HML', 'RF', 'Mom', 'Liq']:
        FactorPortfolioReturns[eachColumn] = FactorPortfolioReturns[eachColumn] -1
        
del FactorModel, eachColumn

FactorPortfolioReturns['Liq'] = np.where(FactorPortfolioReturns.Liq == 0, np.nan, FactorPortfolioReturns.Liq)

#%%
# =============================================================================
# Section 6: Calculate Trade-Based AFP 
# Build a function that calculate Trade-based AFP given 3 parameters
# parameter 1: fund holding dataframe mfHoldings
# parameter 2: CAR dataframe threeDayCARDf
# parameter 3: number of months between two holdings date to calculate active weights
# =============================================================================
def calculateTrade_BasedAFP(mfHoldings, threeDayCARDf, months_offset):
    # =============================================================================
    # Section 6.1: Calculating active weight
    # =============================================================================
    #Generate variable mfHoldingsOffset, a dataframe where the report_dt is slided forward by one quarter
    #Note 1: at this point, each observation in mfHoldings is the last date of a quarter
    #Note 2: month_year is actually pandas datetime variable that takes the first date of the given month-year
    #Procedure: 
    #   1. add three months to month-date, creating 'month_year_offset'
    #   2. check if 'month_year_effect' is the end of a quarter or not
    #   3. if yes, keep it. If no, slide it to the end of the quarter using pd.tseries.offsets.QuarterEnd(statingMonth = 1)
    mfHoldingsOffset = mfHoldings.copy(deep = True)
    mfHoldingsOffset = mfHoldingsOffset[['crsp_portno', 'month_year', 'security_name', 'passiveWeight3M', 'passiveWeight6M', 'passiveWeight12M']]
    mfHoldingsOffset['month_year_offset'] = mfHoldingsOffset.month_year + pd.DateOffset(months = months_offset)
    mfHoldingsOffset['month_year_offset'] = np.where(~mfHoldingsOffset.month_year_offset.dt.is_quarter_end,\
                                                             mfHoldingsOffset.month_year_offset + pd.tseries.offsets.QuarterEnd(startingMonth = 0),
                                                             mfHoldingsOffset.month_year_offset)
    
    mfHoldingsOffset.rename({'month_year': 'month_year_preoffset'}, axis= 1, inplace = True)
    
    #Generate variable mfHoldingsTradeBased, a merge between mfHoldings and mfHoldingsOffset 
    #using slided report_dt
    mfHoldingsTradeBased = mfHoldings.copy(deep = True)
    mfHoldingsTradeBased.drop(['passiveWeight3M', 'passiveWeight6M', 'passiveWeight12M'], axis = 1, inplace = True)
    mfHoldingsTradeBased = pd.merge(left = mfHoldingsTradeBased, right = mfHoldingsOffset, \
             left_on = ['crsp_portno', 'slided_report_dt', 'security_name'],\
             right_on = ['crsp_portno', 'month_year_offset', 'security_name'], how = 'left', indicator = True)
    del mfHoldingsOffset
    
    if (months_offset == 3):
        mfHoldingsTradeBased['active_weight'] = mfHoldingsTradeBased.weight - mfHoldingsTradeBased.passiveWeight3M
    elif (months_offset == 6):
        mfHoldingsTradeBased['active_weight'] = mfHoldingsTradeBased.weight - mfHoldingsTradeBased.passiveWeight6M
    elif (months_offset == 12):
        mfHoldingsTradeBased['active_weight'] = mfHoldingsTradeBased.weight - mfHoldingsTradeBased.passiveWeight12M        
    
    #I may want to come back here to analyze the 'left_only' records
    mfHoldingsTradeBased.drop('_merge', axis = 1, inplace = True)
    
    # =============================================================================
    # Section 6.2: Merge mfHoldingsTradeBased and threeDayCARDf by 
    # 'slided_report_dt' and 'slided_rdq', respectively
    # Note: to control whether to use CAR of all earning announcements in the next quarter
    # or only of the two months following a quarter, go to section 4.3
    # =============================================================================
    tempCAR = threeDayCARDf.copy(deep = True)
    tempCAR = tempCAR[['PERMNO', 'CUSIP', 'COMNAM', 'rdq', 'slided_rdq', 'CAR']]
    tempCAR.drop_duplicates(keep = 'first', inplace = True)
    mfHoldingsTradeBased = pd.merge(left = mfHoldingsTradeBased, right = tempCAR, \
             left_on = ['permno', 'slided_report_dt'], right_on = ['PERMNO', 'slided_rdq'], \
             how = 'left', indicator = True)
    print('Merging CAR to holdings data results in {} duplicates'.format(mfHoldingsTradeBased.duplicated(keep = False).sum()))
    del tempCAR
    
    # =============================================================================
    # Note: There is no earning announcement mapped to the 2018Q4
    #       Obviously the data is only from 2001 to 2018.
    # =============================================================================
    mfHoldingsTradeBased['AFP'] = mfHoldingsTradeBased.active_weight * mfHoldingsTradeBased.CAR
    
    # check the only 'slided_report_dt' that is not in 'slided_rdq' 
    print('The only slided_report_dt that is not in slided_rdq: {}'.\
          format(set(mfHoldingsTradeBased.slided_report_dt.unique()).difference(set(mfHoldingsTradeBased.slided_rdq.unique()))))
    toRemove = list(set(mfHoldingsTradeBased.slided_report_dt.unique()).difference(set(mfHoldingsTradeBased.slided_rdq.unique())))[0]
    
    mfHoldingsTradeBased = mfHoldingsTradeBased[mfHoldingsTradeBased.slided_report_dt != toRemove]
    del toRemove
    
    
    mfHoldingsTradeBased = mfHoldingsTradeBased.groupby(['crsp_portno', 'slided_report_dt'])['AFP'].sum()
    
    mfHoldingsTradeBased = mfHoldingsTradeBased.to_frame().reset_index()
    
    #The first observation of any fund will have AFP = 0 because there is no 
    #prior holding to calculate active weight => remove the first date for every fund
    print('Sum of AFP of first dates of all funds: {}'.format(mfHoldingsTradeBased.groupby('crsp_portno', as_index = 'False')['AFP'].first().sum()))
    mfHoldingsTradeBased.sort_values(['crsp_portno', 'slided_report_dt'], inplace = True)
    first_date_index = mfHoldingsTradeBased.groupby('crsp_portno', as_index = 'False')['slided_report_dt'].first().reset_index()
    mfHoldingsTradeBased = pd.merge(left = mfHoldingsTradeBased, right = first_date_index, on = ['crsp_portno', 'slided_report_dt'], how = 'left', indicator = True)
    print('Sum of AFP of all merged observations with _merge == both (0 is good): {}'.format(mfHoldingsTradeBased[mfHoldingsTradeBased._merge == 'both'].AFP.sum()))
    first_date_index = mfHoldingsTradeBased[mfHoldingsTradeBased._merge == 'both'].index
    mfHoldingsTradeBased.drop(first_date_index, axis = 0, inplace = True)
    mfHoldingsTradeBased.drop('_merge', axis = 1, inplace = True)
    
    # =============================================================================
    # Filter out all dates where AFP = 0 for all funds
    # =============================================================================
    mfHoldingsTradeBased['SquaredAFP'] = mfHoldingsTradeBased.AFP.apply(lambda x: x*x)
    gbSquaredAFP = mfHoldingsTradeBased.groupby('slided_report_dt').SquaredAFP.sum()
    datesWith0AFP= gbSquaredAFP[gbSquaredAFP == 0].index
    mfHoldingsTradeBased = mfHoldingsTradeBased[~mfHoldingsTradeBased.slided_report_dt.isin(datesWith0AFP)]   
    mfHoldingsTradeBased.drop('SquaredAFP', axis = 1, inplace = True)
    
    return mfHoldingsTradeBased.copy(deep = True)

#%%
# =============================================================================
# Section 7: Calculate Benchmark-Based AFP
# =============================================================================

# =============================================================================
# Section 7.1: Mapping CRSP's permno to Compustat's sp_constituents
# =============================================================================
exec(compile(open("D:\Master's Thesis\Code\ComputingIndexWeights.py", 'rb').read(), 'ComputingIndexWeights.py', 'exec'))
sp_constituents = ComputingIndexWeights()



#%%
# =============================================================================
# Section 7.2: Caclulate Active Weight
# =============================================================================
def calculateBenchmarkBasedAFP(mfHoldings, threeDayCARDf, sp_constituents): 
    #Merge sp_constituents to mfHoldings
    tempMfHoldings = mfHoldings.copy(deep = True)
    tempMfHoldings = tempMfHoldings[['crsp_portno', 'report_dt', 'eff_dt', 'security_name',\
                                      'cusip', 'permno', 'weight', 'slided_report_dt']]#, 'is_fixed_income']]
    tempMfHoldings = pd.merge(left = tempMfHoldings, right = sp_constituents,\
                    left_on = ['report_dt', 'permno'], right_on = ['Date', 'permno'],\
                    how = 'left')
    
    tempMfHoldings['active_weight'] = tempMfHoldings.weight - tempMfHoldings.constituent_weight
    
    # =============================================================================
    # Section 7.3
    # Merge CAR and calculate AFP for each security at each day for each fund
    # =============================================================================
    tempCAR = threeDayCARDf.copy(deep = True)
    tempCAR = tempCAR[['PERMNO', 'CUSIP', 'COMNAM', 'rdq', 'slided_rdq', 'CAR']]
    tempCAR.drop_duplicates(keep = 'first', inplace = True)
    tempMfHoldings = pd.merge(left = tempMfHoldings, right = tempCAR, \
             left_on = ['permno', 'slided_report_dt'], right_on = ['PERMNO', 'slided_rdq'], \
             how = 'left', indicator = True)
    print('Merging CAR to holdings data results in {} duplicates'.format(tempMfHoldings.duplicated(keep = False).sum()))
    del tempCAR
    
    #Removing all rows that do not have CAR mapped to
    tempMfHoldings = tempMfHoldings[tempMfHoldings.CAR.notnull()]
    #After this, the dataset still has 2464 unique crsp_portno 
    tempMfHoldings.drop('_merge', axis = 1,inplace = True)
    
    tempMfHoldings['AFP'] = tempMfHoldings.active_weight * tempMfHoldings.CAR
    
    # =============================================================================
    # Section 7.4
    # Calculate AFP for each fund 
    # =============================================================================
    tempBenchmarkBasedAFP = tempMfHoldings.groupby(['slided_report_dt', 'crsp_portno']).AFP.sum()
    tempBenchmarkBasedAFP = tempBenchmarkBasedAFP.reset_index()
    #Remove days where all funds have AFP = 0
    tempBenchmarkBasedAFP['Squared_AFP'] = tempBenchmarkBasedAFP.AFP.apply(lambda x: x*x)
    
    gbObj = tempBenchmarkBasedAFP.groupby('slided_report_dt').Squared_AFP.sum()
    days_all_fund_AFP_is_zero = gbObj[gbObj == 0].index
    tempBenchmarkBasedAFP.drop(tempBenchmarkBasedAFP[tempBenchmarkBasedAFP.slided_report_dt.isin(days_all_fund_AFP_is_zero)].index, axis = 0, inplace = True)
    tempBenchmarkBasedAFP.drop('Squared_AFP', axis = 1, inplace = True)
    del gbObj
    
    return tempBenchmarkBasedAFP.copy(deep = True)


#%%
# =============================================================================
# Section 9: Calculate Decile AFP Portfolio Returns
# Note: 
# months_offset = 0: Benchmark based result
# months_offset > 0: the number of months distance to calculate active weights
# =============================================================================
def calculateDecilePortfolioReturn(AFP_Frame_input, mfReturns):
    AFPdf = AFP_Frame_input.copy(deep = True)
    AFPdf.replace([np.inf, -np.inf, np.nan], value = 0, inplace = True)
#    AFPdf = AFPdf[~AFPdf.isin([np.inf, -np.inf, np.nan])]
    AFPdf = AFPdf[AFPdf.AFP != 0]
#    datesWithMoreThan10Funds = AFPdf.groupby(['slided_report_dt']).crsp_portno.nunique().reset_index()
#    AFPdf = AFPdf[AFPdf.slided_report_dt.isin(datesWithMoreThan10Funds.slided_report_dt.unique())]
    AFPdf['DecilePortfolio'] = AFPdf.groupby('slided_report_dt')['AFP'].transform(lambda x: pd.cut(x, 10, labels = range(1,11), include_lowest = True))
    AFPdf['DecilePortfolio'] = AFPdf.DecilePortfolio.astype(int)
    
    
    # The code below is to slide caldt toe the portfolio formation date to evaluate 
    # portfolio performance return. This code should be moved to another location
    # in the code
    tempMfReturns = mfReturns.copy(deep = True)
    conditions = [tempMfReturns.caldt.dt.month.isin([3,6,9,12]),\
                  tempMfReturns.caldt.dt.month.isin([1,4,7,10]), \
                  tempMfReturns.caldt.dt.month.isin([2,5,8,11])]
    
    choices = [pd.offsets.DateOffset(months = 3),\
               pd.offsets.DateOffset(months = 4),\
               pd.offsets.DateOffset(months = 5)]
    
    
    tempMfReturns['slided_caldt'] = np.select(conditions, choices)
    tempMfReturns['slided_caldt'] = tempMfReturns.caldt - tempMfReturns.slided_caldt
    tempMfReturns['is_month_end'] = tempMfReturns.slided_caldt.dt.is_month_end
    tempMfReturns['slided_caldt'] = np.where(tempMfReturns.is_month_end == False,\
                                         tempMfReturns.slided_caldt + pd.tseries.offsets.MonthEnd(),\
                                         tempMfReturns.slided_caldt)
    tempMfReturns.drop('is_month_end', axis = 1,inplace = True)
    
    
    
    AFPdf = pd.merge(left = AFPdf, right = tempMfReturns, \
                    left_on = ['crsp_portno', 'slided_report_dt'], \
                    right_on = ['crsp_portno', 'slided_caldt'], \
                    how = 'outer', indicator = True)
    
    AFPdf.sort_values(['crsp_portno', 'caldt'], inplace = True)
    AFPdf = AFPdf[~AFPdf.DecilePortfolio.isnull()]
    
    
#    AFPdf['mret'] = AFPdf.mret + 1
    
    TradeBasedResult = AFPdf.groupby(['DecilePortfolio', 'slided_report_dt', 'caldt']).mret.mean()
    TradeBasedResult = TradeBasedResult.to_frame().reset_index()
    
    TradeBasedResult['mret'] = TradeBasedResult.mret + 1
    
    TradeBasedResult = TradeBasedResult.groupby(['DecilePortfolio', 'slided_report_dt']).mret.prod()
    TradeBasedResult = TradeBasedResult.to_frame().reset_index()
    
    TradeBasedResult['mret'] = TradeBasedResult.mret - 1
    
    TradeBasedResult = pd.merge(left = TradeBasedResult, right = FactorPortfolioReturns,\
         left_on = 'slided_report_dt', right_on = 'slided_date',
         how = 'outer')
    TradeBasedResult['excess_mret'] = TradeBasedResult.mret - TradeBasedResult.RF
    TradeBasedResult['constant'] = np.ones(TradeBasedResult.shape[0])
    
    #Remove rows where Decile Portfolio is nan
    TradeBasedResult = TradeBasedResult[TradeBasedResult.DecilePortfolio.notnull()]
    
    tempResult = pd.DataFrame(columns = TradeBasedResult.columns)
    for dt, group in TradeBasedResult.groupby('slided_report_dt'):
        tempGroup = group.copy(deep = True)
        if tempGroup.DecilePortfolio.min() != 1:
            tempGroup['DecilePortfolio'] = np.where(tempGroup.DecilePortfolio == tempGroup.DecilePortfolio.min(),
                                                 1, tempGroup.DecilePortfolio)
        if tempGroup.DecilePortfolio.max() != 10:
            tempGroup['DecilePortfolio'] = np.where(tempGroup.DecilePortfolio == tempGroup.DecilePortfolio.max(),
                                                 10, tempGroup.DecilePortfolio)
        tempResult = pd.concat([tempResult, tempGroup], axis = 0)
        
        
    return TradeBasedResult.copy(deep = True)
#    return tempResult.copy(deep = True)




#%%
# =============================================================================
# Section 10: Build generator for univariate analysis
# =============================================================================
def univariateAFPResultGenerator(calculateDecilePortfolioReturn_result):
    AFPPortfolio = calculateDecilePortfolioReturn_result.copy(deep = True)
    AFPPortfolio = AFPPortfolio.replace([np.inf, -np.inf], np.nan)
    AFPPortfolio.dropna(inplace = True)
    DecileGbObject = AFPPortfolio.groupby('DecilePortfolio')
    constantList = []
    
    # =============================================================================
    # =============================================================================
    # Analyse 10 decile portfolios
    # =============================================================================
    # =============================================================================
    for name, group in DecileGbObject:
        X_Carhart = group[['constant', 'Mkt-RF', 'SMB', 'HML', 'Mom']]
        X_FF = group[['constant', 'Mkt-RF', 'SMB', 'HML']]
        X_CAPM = group[['constant', 'Mkt-RF']]
        X_Pastor = group[['constant', 'Mkt-RF', 'SMB', 'HML', 'Mom', 'Liq']]
        y = group.excess_mret
        model_carhart = sm.OLS(y, X_Carhart).fit()
        model_FF = sm.OLS(y, X_FF).fit()
        model_CAPM = sm.OLS(y, X_CAPM).fit()   
        model_Pastor = sm.OLS(y, X_Pastor).fit()
        
        constantList.append(['Average Return', name, group.mret.mean(), \
                         statsmodels.stats.weightstats.ztest(group.mret, value = 0)[0], statsmodels.stats.weightstats.ztest(group.mret, value = 0)[1]])
        constantList.append(['CAPM alpha', name, model_CAPM.params[0], model_CAPM.tvalues[0], model_CAPM.pvalues[0]])
        constantList.append(['Fama French alpha', name, model_FF.params[0], model_FF.tvalues[0], model_FF.pvalues[0]])
        constantList.append(['Carhart alpha', name, model_carhart.params[0], model_carhart.tvalues[0], model_carhart.pvalues[0]])
        constantList.append(['Pastor and Stambaugh alpha', name, model_Pastor.params[0], model_Pastor.tvalues[0], model_Pastor.pvalues[0]])
        
   
    # =============================================================================
    # Analyse portfolio (Decile 10 - Decile 1)
    # =============================================================================
    #Create a separate dataframe for Decile 10 - Decile 1
    #in order to calculate t statistics
    highlowPortfolio = AFPPortfolio[AFPPortfolio.DecilePortfolio.isin([1,10])]
    highlowPortfolio = highlowPortfolio.sort_values(['slided_report_dt', 'DecilePortfolio'])
    tenthPortfolio = highlowPortfolio.loc[highlowPortfolio.groupby('slided_report_dt').DecilePortfolio.idxmax()]
    tenthPortfolio.rename({'excess_mret' : 'excess_mret_tenth'}, axis = 1, inplace = True)
    firstPortfolio = highlowPortfolio.loc[highlowPortfolio.groupby('slided_report_dt').DecilePortfolio.idxmin()]
    firstPortfolio.rename({'excess_mret' : 'excess_mret_first'}, axis = 1, inplace = True)
    firstPortfolio = firstPortfolio[['slided_report_dt', 'excess_mret_first']]
    tenthminusfirstPortfolio = pd.merge(left = tenthPortfolio, right = firstPortfolio,\
                              on = 'slided_report_dt', how = 'inner')
    tempCol = pd.Series(['High-Low'] * len(tenthminusfirstPortfolio))
    tenthminusfirstPortfolio['DecilePortfolio'] = tempCol
    tenthminusfirstPortfolio['mret_difference'] = tenthminusfirstPortfolio.excess_mret_tenth - tenthminusfirstPortfolio.excess_mret_first
    
    #Run the models on this separate dataframe
    X_Carhart = tenthminusfirstPortfolio[['constant', 'Mkt-RF', 'SMB', 'HML', 'Mom']]
    X_FF = tenthminusfirstPortfolio[['constant', 'Mkt-RF', 'SMB', 'HML']]
    X_CAPM = tenthminusfirstPortfolio[['constant', 'Mkt-RF']]
    X_Pastor = tenthminusfirstPortfolio[['constant', 'Mkt-RF', 'SMB', 'HML', 'Mom', 'Liq']]
    y = tenthminusfirstPortfolio.mret_difference - tenthminusfirstPortfolio.RF
    
    model_carhart = sm.OLS(y, X_Carhart).fit()
    model_FF = sm.OLS(y, X_FF).fit()
    model_CAPM = sm.OLS(y, X_CAPM).fit()   
    model_Pastor = sm.OLS(y, X_Pastor).fit()
    
    
    constantList.append(['Average Return', 'High-Low', tenthminusfirstPortfolio.mret_difference.mean(), \
                         statsmodels.stats.weightstats.ztest(tenthminusfirstPortfolio.mret_difference, value = 0)[0],\
                         statsmodels.stats.weightstats.ztest(tenthminusfirstPortfolio.mret_difference, value = 0)[1]])
    #    constantList.append(['Average Return', 'High-Low', tenthminusfirstPortfolio.mret_difference.mean(), \
    #                         stats.ttest_1samp(tenthminusfirstPortfolio.mret_difference, 0)])
    #    constantList.append(['Average Return', 'High-Low', tenthminusfirstPortfolio.mret_difference.mean(), \
    #                         stats.ttest_ind(tenthminusfirstPortfolio.excess_mret_tenth, tenthminusfirstPortfolio.excess_mret_first,\
    #                                         equal_var=False)])
    constantList.append(['CAPM alpha', 'High-Low', model_CAPM.params[0], model_CAPM.tvalues[0], model_CAPM.pvalues[0]])
    constantList.append(['Fama French alpha', 'High-Low', model_FF.params[0], model_FF.tvalues[0], model_FF.pvalues[0]])
    constantList.append(['Carhart alpha', 'High-Low', model_carhart.params[0], model_carhart.tvalues[0], model_carhart.pvalues[0]])
    constantList.append(['Pastor and Stambaugh alpha', 'High-Low', model_Pastor.params[0], model_Pastor.tvalues[0], model_Pastor.pvalues[0]])
    

    
    # =============================================================================
    # Convert constantList to constantDf
    # Pivot constantDf into nice pivot tables
    # =============================================================================
    constantDf = pd.DataFrame(constantList)
    constantDf.columns = ['Model', 'DecilePortfolio', 'constant', 't-value', 'p-value']
    
    constantPivot = constantDf.pivot(index = 'Model', columns = 'DecilePortfolio', values = ['constant'])
    constantPivot.columns = ['Low', 2, 3,4,5,6,7,8,9,'High', 'High-Low']
    
    constantTvaluePivot =  constantDf.pivot(index = 'Model', columns = 'DecilePortfolio', values = ['t-value'])
    constantTvaluePivot.columns = ['Low', 2, 3,4,5,6,7,8,9,'High', 'High-Low']
    constantTvaluePivot.index = ['Average Return _ T-value', 'CAPM alpha _ T-value', 'Carhart alpha _ T-value',\
                                 'Fama French alpha _ T-value', 'Pastor and Stambaugh alpha _ T-value']
    #    constantTvaluePivot['10-1'] = constantTvaluePivot.High - constantTvaluePivot.Low
    
    constantPvaluePivot =  constantDf.pivot(index = 'Model', columns = 'DecilePortfolio', values = ['p-value'])
    constantPvaluePivot.columns = ['Low', 2, 3,4,5,6,7,8,9,'High', 'High-Low']
    constantPvaluePivot.index = ['Average Return _ P-value', 'CAPM alpha _ P-value', 'Carhart alpha _ P-value',\
                                 'Fama French alpha _ P-value', 'Pastor and Stambaugh alpha _ P-value']
    
    constantReturn = pd.concat([constantPivot, constantTvaluePivot, constantPvaluePivot], axis = 0)
    constantReturn.sort_index(axis = 0, inplace=  True)
    del X_CAPM, X_Carhart, X_FF, X_Pastor, y
    del model_carhart, model_FF, model_CAPM, model_Pastor
    del name, group
    
    
    return constantReturn

#%%
# =============================================================================
# Section: Univariate Results
# =============================================================================
indexAFP_frame = calculateBenchmarkBasedAFP(mfHoldings, threeDayCARDf, sp_constituents)
tradeAFP3M_frame = calculateTrade_BasedAFP(mfHoldings, threeDayCARDf, 3)
tradeAFP6M_frame = calculateTrade_BasedAFP(mfHoldings, threeDayCARDf, 6)
tradeAFP12M_frame = calculateTrade_BasedAFP(mfHoldings, threeDayCARDf, 12)
  
AFPPortfolioIndex = calculateDecilePortfolioReturn(indexAFP_frame, mfReturns)    
AFPPortfolioTrade3M = calculateDecilePortfolioReturn(tradeAFP3M_frame, mfReturns)
AFPPortfolioTrade6M = calculateDecilePortfolioReturn(tradeAFP6M_frame, mfReturns)
AFPPortfolioTrade12M = calculateDecilePortfolioReturn(tradeAFP12M_frame, mfReturns)


writer = pd.ExcelWriter('UniAFPResult.xlsx')
univariateAFPResultGenerator(AFPPortfolioIndex[AFPPortfolioIndex.slided_report_dt > '2014-12-31']).to_excel(writer, sheet_name = 'Index>2014')
univariateAFPResultGenerator(AFPPortfolioTrade3M[AFPPortfolioTrade3M.slided_report_dt > '2014-12-31']).to_excel(writer, sheet_name = 'Trade3M>2014')
univariateAFPResultGenerator(AFPPortfolioTrade6M[AFPPortfolioTrade6M.slided_report_dt > '2014-12-31']).to_excel(writer, sheet_name = 'Trade6M>2014')
univariateAFPResultGenerator(AFPPortfolioTrade12M[AFPPortfolioTrade12M.slided_report_dt > '2014-12-31']).to_excel(writer, sheet_name = 'Trade12M>2014')
univariateAFPResultGenerator(AFPPortfolioIndex[AFPPortfolioIndex.slided_report_dt <= '2014-12-31']).to_excel(writer, sheet_name = 'Index<=2014')
univariateAFPResultGenerator(AFPPortfolioTrade3M[AFPPortfolioTrade3M.slided_report_dt <= '2014-12-31']).to_excel(writer, sheet_name = 'Trade3M<=2014')
univariateAFPResultGenerator(AFPPortfolioTrade6M[AFPPortfolioTrade6M.slided_report_dt <= '2014-12-31']).to_excel(writer, sheet_name = 'Trade6M<=2014')
univariateAFPResultGenerator(AFPPortfolioTrade12M[AFPPortfolioTrade12M.slided_report_dt <= '2014-12-31']).to_excel(writer, sheet_name = 'Trade12M<=2014')
univariateAFPResultGenerator(AFPPortfolioIndex).to_excel(writer, sheet_name = 'Index')
univariateAFPResultGenerator(AFPPortfolioTrade3M).to_excel(writer, sheet_name = 'Trade3M')
univariateAFPResultGenerator(AFPPortfolioTrade6M).to_excel(writer, sheet_name = 'Trade6M')
univariateAFPResultGenerator(AFPPortfolioTrade12M).to_excel(writer, sheet_name = 'Trade12M')
writer.save()


#%%
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# # # # # # # Section: Predictive Panel Data Regression
# Note: use tempMfReturns, a copy of mfReturns
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# ==========================================================================================================================================================
#%%
# ==========================================================================================================================================================
# =============================================================================
# Section: compute expected returns for fund using Carhart 4 factor model
# =============================================================================
ThreeFactorModel = pd.read_excel("./Factor Models/F-F_Research_Data_Factors.xlsx")
Momentum = pd.read_excel("./Factor Models/F-F_Momentum_Factor.xlsx")

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

expectedCarhartReturnList = []
expectedFFReturnList = []
for eachPortno, portnoGroup in tempMfReturns.groupby('crsp_portno'):
    tempPortnoGroup = portnoGroup.copy(deep = True)
    tempPortnoGroup = tempPortnoGroup.sort_values('slided_caldt')
    tempFirstDate = tempPortnoGroup.slided_caldt.iloc[0]
    for currentCaldt in tempPortnoGroup.slided_caldt:
        if currentCaldt >= (tempFirstDate + pd.DateOffset(years = 3)):
            tempReturnWindow = portnoGroup[portnoGroup.slided_caldt.between\
                                           (left = currentCaldt - pd.DateOffset(years = 3), \
                                            right = currentCaldt, inclusive = True)]
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
 
# =============================================================================
# Section: Calculate Carhart alpha and FF alpha, fund return in excess of expected return 
# =============================================================================

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

# =============================================================================
# Section: Prepare tempMfSummary to get fund characteristics:
# NAV is the sum of NAVs across all share classes
# TNA is the sum of TNAs across all share classes
# exp_ratio is the tna-weighted avearge of exp_ratio across all share classes
# turn_ratio is the tna-weighted average of turn_ratio across all share clases
# age is the age of the oldest share class of a fund
# =============================================================================
# =============================================================================
# Calculate tna-weighted average for expense ratio and turnover ratio
# Data from mfSummary - fund summary data
# frequency: originally quarterly basis from the data source, then extrapolate
# to monthly basis to use data with higher frequency
# =============================================================================
#Calculate tna-weighted expense ratio and turnover ratio. Resulting variable: mfRatio
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

#mfValue
mfValue = mfSummary.copy(deep = True)
mfValue['slided_caldt'] = np.where(mfValue.caldt.dt.is_month_end,\
                                         mfValue.caldt,\
                                         mfValue.caldt + pd.tseries.offsets.MonthEnd(0))
mfValue = mfValue[['crsp_portno', 'crsp_fundno', 'slided_caldt', 'nav_latest', 'tna_latest']]
mfValue = mfValue.groupby(['crsp_portno', 'slided_caldt'])['nav_latest', 'tna_latest'].sum().reset_index()

#Merge mfRatio and mfValue
mfRatioValue = pd.merge(left = mfRatio, right = mfValue, on = ['crsp_portno', 'slided_caldt'], how = 'left')
mfRatioValue = mfRatioValue[['slided_caldt', 'crsp_portno', 'weighted_exp_ratio', \
                             'weighted_turn_ratio', 'nav_latest', 'tna_latest']]

#Resample expense and turnover ratio from quarterly to monthly to use the higher frequency of other data
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
# Create variable age and merge to resampled
# =============================================================================
tempAge = mfSummary.copy(deep = True)
tempAge = tempAge[['crsp_portno', 'age']]
tempAge = tempAge.groupby('crsp_portno').age.max().reset_index()
tempAge['age'] = tempAge.age.dt.days / 365

resampledFundSummary = pd.merge(left = resampledFundSummary, right = tempAge,
                                on = 'crsp_portno', how = 'left')
# =============================================================================
# Prepare the tempMfReturns
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
    
# =============================================================================
# Resample the dataframes that contain AFP measure for funds 
# from quarterly to monthly so as to take advantage of the monthly return data
# No, I shouldn't do that... I should not just extrapolate AFP to other period like that
# =============================================================================
    
# =============================================================================
# Section: Prepare final panel dataframes ready for regression
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
# =============================================================================
# Section: Panel Data Regression Analysis for Index Based 
# =============================================================================
#statsmodels.regression.linear_model.RegressionResults.
#model = sm.OLS(PanelDataIndexBased.CarhartAlpha, PanelDataIndexBased[['AFP', 'tna_latest', 'nav_latest', 'exp_ratio', 'turn_ratio', 'age']]).fit(cov_type = 'HAC', cov_kwds={'maxlags': 1})
#model.summary()

#

from linearmodels.panel import PanelOLS
temp = PanelDataIndexBased.copy(deep = True)
#temp = temp[temp.slided_caldt > '01-01-2015']
temp = temp.set_index(['crsp_portno', 'slided_caldt'])
temp = temp[temp.AFP != -np.inf]
temp = temp[temp.AFP != np.inf]

#mod = PanelOLS(temp.CarhartAlpha, temp.AFP, time_effects = True)
mod = PanelOLS(temp.FFAlpha, temp.AFP, time_effects = True)

#res = mod.fit()
#res = mod.fit(cov_type = 'clustered', cluster_entity = True)
res = mod.fit(cov_type = 'clustered', cluster_time = True)
print(res)

#del temp
#%%
#'CarhartAlpha1L',
from linearmodels.panel import PanelOLS
temp = PanelDataTrade12M.copy(deep = True)
temp = temp.replace([np.inf, -np.inf], np.nan)
temp['CarhartAlpha1L'] = temp.groupby('crsp_portno')['CarhartAlpha'].shift(1)
temp['FFAlpha1L'] = temp.groupby('crsp_portno')['FFAlpha'].shift(1)
temp = temp.dropna()
temp = temp.set_index(['crsp_portno', 'slided_caldt'])

#mod = PanelOLS(temp.CarhartAlpha, temp[['AFP', 'log(tna)',\
#                                        'log(Age)','weighted_exp_ratio',\
#                                        'weighted_turn_ratio', 'CarhartAlpha1L']], time_effects = True)

mod = PanelOLS(temp.FFAlpha, temp[['AFP', 'log(tna)',\
                                        'log(Age)', 'weighted_exp_ratio',\
                                        'weighted_turn_ratio', 'FFAlpha1L']], time_effects = True)
    

#res = mod.fit(cov_type = 'clustered', cluster_entity = True)
res = mod.fit(cov_type = 'clustered', cluster_time = True)
print(res)

#del temp

#%%
#temp = AFPPortfolioIndex.copy(deep = True)
#temp = temp[temp.DecilePortfolio.isin([2,9])]
#temp['DecilePortfolio'] = temp.DecilePortfolio.astype(str)
#temp = temp.pivot(index = 'slided_report_dt', columns = 'DecilePortfolio', values='mret')
#temp['difference'] = temp['9.0'] - temp['2.0']
#temp = temp.dropna()
#statsmodels.stats.weightstats.ztest(temp.difference, value = 0)[1]