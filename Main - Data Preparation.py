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
# File Description: Data Preparation
# Main outcome: Main variables for subsequent analysis
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================

#%%
# ==========================================================================================================================================================
# ==========================================================================================================================================================
# Section 1: Process Mutual Fund Summary data
# Main variable: mfSummary
# ==========================================================================================================================================================
# ==========================================================================================================================================================

mfSummary = pd.read_csv("./Data/Mutual Funds/[MF] Mutual Fund Summary 2001 - 2018.csv")

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
# ==========================================================================================================================================================
# ==========================================================================================================================================================
# Section 2: Import and Process Mutual Fund Portfolio Holdings data
# Main outcome variable: mfHoldings
# ==========================================================================================================================================================
# ==========================================================================================================================================================

# =============================================================================
# Import Mutual Fund Portfolio Holdings data into variable mfHoldings
# Make the dataset smaller by keeping on the funds that have been selected
# for the variable mfSummary.
# =============================================================================
mfHoldings = pd.read_csv("./Data/Mutual Funds/[MF] CRSP Mutual Fund Holdings 2001 - 2018.csv", dtype = {'security_name': str, \
                         'cusip': str})
chosenFundList = mfSummary.crsp_portno.unique()
mfHoldings = mfHoldings[mfHoldings.crsp_portno.isin(chosenFundList)]
del chosenFundList


# =============================================================================
# additional steps to clean holdings data
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
# Create column 'security_price' in mfHoldings by dividing market_val by nbr_shares
mfHoldings['security_price'] = mfHoldings.market_val / mfHoldings.nbr_shares

# =============================================================================
# Generating Variables 'slided_report_dt'
#    - For each fund, each security, and each quarter, choose the observation closest to the end of the quarter
#      This controls for cases where holding for a stock by a fund is reported twice within a quarter, 
#      in which cases choose the observation closest to quarter end
#    - Then create a variable 'slided_report_dt' that slides all report_dt to the end of the quarter. 
#        This means that:
#            + if report_dt is 28/02, then it slides to 31/03
#            + if report_dt is 30/12, then it slides to 31/12
#      This variable 'slide_report_dt' aligns holdings data to be uniformly at quarter end
#      which will in turn make it easy to compute AFP
# =============================================================================

#Create a variable 'report_yearquarter' to extract year and quarter from report_dt
mfHoldings['report_dt'] = pd.to_datetime(mfHoldings.report_dt, format = '%d/%m/%Y')
mfHoldings['report_yearquarter'] = mfHoldings.report_dt.dt.year.astype(str) + 'Q' + mfHoldings.report_dt.dt.quarter.astype(str)
mfHoldings['report_yearquarter'] = mfHoldings.report_yearquarter.astype(str) # convert from Object to str to reduce memory

# Using 'report_yearquarter' and groupby, select the latest date in each fund-security-quarter
# 'security_name' is used as security identifier here. This step will be done again
# later as permno is mapped from CRSP Stock Header to mfHoldings
# This will be done again below using permno
mfHoldings = mfHoldings.sort_values(['crsp_portno', 'security_name', 'report_dt'])
mfHoldings = mfHoldings.loc[mfHoldings.groupby(['crsp_portno', 'security_name', 'report_yearquarter']).report_dt.idxmax()]


#Create 'slided_report_dt'
mfHoldings['slided_report_dt'] = np.where(~mfHoldings.report_dt.dt.is_quarter_end, \
                                  mfHoldings.report_dt + pd.tseries.offsets.QuarterEnd(), \
                                  mfHoldings.report_dt)

#%%
# =============================================================================
# Download and transform CRSP Stock Header 
# Explanation:
# Because holdings variable mfHoldings may have some imperfections, such as that 
# many permno are missing even though they shouldn't. I download CRSP Stock Header
# to map as many more permno to mfHoldings via Cusip as possible
# =============================================================================
crspStockHeader = pd.read_csv("./Data/Securities/[stock] CRSP Stock Header.csv")
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
# Fill in missing Permno as follows: 
# Sometimes permno is not available but cusip is,
# in which case use crsp stock header to map permno to these cusip
# Because of programming technicality, I must split mfHoldings into three parts:
#   - parts with permno, which we keep intact
#   - parts without both permno and cusip, which we keep intact
#   - parts without permno but with cusip, which we map permno from Stock Header to via cusip
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

#Combine into mfHoldings
mfHoldings = pd.concat([mfHoldings_wPermno, mfHoldings_woPermno], axis = 0)
del crspStockHeader

#%%
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

print("crsp_portno-report_dt-permno has no duplicate: ",\
      mfHoldings_wPermno[mfHoldings_wPermno.duplicated(['crsp_portno', 'report_dt', 'permno'], keep = False)].shape[0] == 0)

mfHoldings_wPermno = mfHoldings_wPermno[mfHoldings_woPermno.columns]

mfHoldings = pd.concat([mfHoldings_wPermno, mfHoldings_woPermno], axis = 0)

del mfHoldings_woPermno, mfHoldings_wPermno

#%%
# =============================================================================
# Sometimes the market value market_val of a security holding is 0 
# even though the number of shares held nbr_shares > 0
# In this case, I obtain daily stock data from CRSP and map price to respective observations, 
# then I use this price to calculate the market value which is then assigned to market_val
# =============================================================================
dailyStock = pd.read_csv('./Data/Securities/[stock] CRPS - Daily Stock 2001 - 2018.csv')
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
# Shift price back to adjust for passive weight 
# Explanation:
# In calculating the active weight change for trade-based AFP and for Section 4 
# I attempt to identify skill at mutual fund industry level, we need to account for passive weight change
# due to changes in prices.
# To this end, I shift price back one quarter, two quarters or four quarters,
# and put them into three variables priceFromForward3M, priceFromForward6M, priceFromForward12M, respectively
# These three variables are then merged to the mfHoldings, so that each observations in mfHoldings
# will now have price from 3 months, 6 months and 12 months ahead.
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
# This block generates 4 main columns for variable mfHoldings:
#- weight: contemporaneous weight of the security in the fund portfolio
#- passiveWeight3M: calculated by the product between the holdings of the previous 3 months and the contemporaneous price, 
#  divided by the fund's total market value of the previous 3 months
#- passiveWeight6M: calculated by the product between the holdings of the previous 6 months and the contemporaneous price
#  divided by the fund's total market value of the previous 6 months
#- passiveWeight12M: calculated by the product between the holdings of the previous 12 months and the contemporaneous price
#  divided by the fund's total market value of the previous 12 months
#To calculate active weight change (which will be done later), I use contemporaneous weight (first column) minus 
#the passiveWeight columns (the second, third and fourth columns)
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


del total_market_val, tempHoldings






#%%
#%%
# ==========================================================================================================================================================
# ==========================================================================================================================================================
# Section 3: Import and Calculate Mutual Fund Monthly TNA-weighted Returns 
# Main outcome variable: mfReturns
# ==========================================================================================================================================================
# ==========================================================================================================================================================
#Import fund returns data from CRSP 
mfReturns = pd.read_csv('./Data/Mutual Funds/[MF] Mutual Fund Monthly Returns 2001 - 2018.csv')

#Remove returns = 'R'
mfReturns = mfReturns[~(mfReturns.mret == 'R')]

#Format return to float
mfReturns['mret'] = mfReturns.mret.astype(float)

#Format caldt to datetime
mfReturns['caldt'] = pd.to_datetime(mfReturns.caldt, format = "%d/%m/%Y")

####
# Because return data is at share class level, i.e. only crsp_fundno is available
# I download CRSP Portno-Fundno Mapping to map crsp_portno (fund identifier) to 
# the return data
####

mfFundPortMapping = pd.read_excel("./Data/Mutual Funds/[MF] CRSP Portno-Fundno Mapping.xlsx")

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
####

#Remove observations without fund identifier crsp_portno
mfReturns = mfReturns[mfReturns.crsp_portno.notnull()]

#Keep only the funds that also exist in mfSummary (which went through the fund filters)
mfReturns = mfReturns[mfReturns.crsp_portno.isin(mfSummary.crsp_portno.unique())]

# if mtna = 'T', treat it as missing value
mfReturns['mtna'] = np.where(mfReturns.mtna == 'T', np.nan, mfReturns.mtna)

# Format mtna (monthly total net asset) as float
mfReturns['mtna'] = mfReturns.mtna.astype(float)

# Remove rows with missing mret
mfReturns = mfReturns[mfReturns.mret.notnull()]

mfReturns.sort_values(['crsp_portno', 'caldt'], inplace = True)


####
# Return is weighted by net asset (tna) of respective share class
# But for many observations tna is missing. I consider mtna as missing if
# it is null, = 0, = -99
# To reduce the number of observations with missing tna, for each fund-month,
# I find the share class with the lowest tna, then I extrapolate this lowest tna
# to the shareclass with missing tna
####
missingTNA = mfReturns[(mfReturns.mtna.isnull())|(mfReturns.mtna == 0)|\
                 (mfReturns.mtna == -99)][['caldt', 'crsp_portno']].drop_duplicates(keep = 'first')

missingTNA = missingTNA.set_index(['caldt', 'crsp_portno'])

mfReturns.set_index(['caldt', 'crsp_portno'], inplace = True)

missingTNA = mfReturns.loc[missingTNA.index]

mfReturns.drop(missingTNA.index, axis = 0, inplace = True)
mfReturns.reset_index(inplace = True)

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

mfReturns = pd.concat([mfReturns, missingTNA], axis = 0)
####

####
# After reducing as many observations with missing tna by extrapolating min-tna of a fund-month,
# I compute monthly tna-weighted return of the fund
####
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
# ==========================================================================================================================================================
# ==========================================================================================================================================================
# Section 4: generates descriptive statistics for mutual fund holdings data, 
# i.e. generate Table 1 in the thesis
# Main variable used: mfSummary from above
# ==========================================================================================================================================================
# ==========================================================================================================================================================
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

# Write the descriptive statistics and correlation table into a file MFDescriptiveStats.xlsx
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
# ==========================================================================================================================================================
# ==========================================================================================================================================================
# Section 5: Import and Calculate Holding Returns of Factor-mimicking Portfolios 
# for AFP Analysis following Figure 2
# The main outcome variable: FactorPortfolioReturns. 
# Reminder Example: If the date is the end of march, 
# then the return is from holding the factor-mimicking portfolios from June to August
# ==========================================================================================================================================================
# ==========================================================================================================================================================

# =============================================================================
# Import and process Factor Model
# =============================================================================
ThreeFactorModel = pd.read_excel("./Data/Factor Models/F-F_Research_Data_Factors.xlsx")
Momentum = pd.read_excel("./Data/Factor Models/F-F_Momentum_Factor.xlsx")

FactorModel = pd.merge(left = ThreeFactorModel, right = Momentum, \
                       on = 'month_year', how = 'inner', indicator = True)
print('Merging Momentum and Three factor model successful: {}'.format(FactorModel._merge.nunique()==1))
FactorModel.drop("_merge", axis = 1, inplace = True)
liq = pd.read_csv("./Data/Factor Models/liq_data_1962_2018.txt", delim_whitespace=True)
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
# ==========================================================================================================================================================
# ==========================================================================================================================================================
# Section 6: Compute Three-day CAR
# Main variable: threeDayCARDf
# ==========================================================================================================================================================
# ==========================================================================================================================================================

# =============================================================================
# Run "calculatingCAR.py" which outputs a variable threeDayCARDf which contains 
# three-day cumulative abnormal return around earnings announcement
#
# Slide the earnings announcements date back to the last date of the previous quarter
# so as to map to the holdings data to calculate AFP

# In other words, create a variable slided_rdq that will map to 'slided_report_dt' from mfHoldings
# =============================================================================


#Run calculatingCAR.py and get threeDayCARDf
exec(compile(open(directory+"\Code\calculatingCAR.py", 'rb').read(), 'calculatingCAR.py', 'exec'))
threeDayCARDf = calculatingCAR()

#Create a variable slided_rdq which slides the rdq (reported date quarterly) to the first day of the quarter
threeDayCARDf['slided_rdq'] = threeDayCARDf.rdq - pd.tseries.offsets.QuarterBegin(startingMonth = 1)

# Only keep earnings announcements two months after a quarter end. 
# Earnings announcements in the third month after a quarter end is discarded.
# The rationale is provided in the thesis
threeDayCARDf = threeDayCARDf[~threeDayCARDf.rdq.dt.month.isin([7, 9, 12, 3])]
              
#Move the slided rdq back one day so that the day becomes the last day of the previous quarter
threeDayCARDf['slided_rdq'] = threeDayCARDf.slided_rdq - pd.DateOffset(1)

#%%
# =============================================================================
# Section 7: Obtain constituent weights for SP500 Proxy by:
# - First run the file ComputingIndexWeights.py to obtain the function ComputingIndexWeights()
# - Then run the function ComputingIndexWeights and assign the output to variable sp_constituents
# =============================================================================

exec(compile(open(directory + "\Code\ComputingIndexWeights.py", 'rb').read(), 'ComputingIndexWeights.py', 'exec'))
sp_constituents = ComputingIndexWeights()