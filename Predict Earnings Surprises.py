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
import os
from datetime import datetime
#%%
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# File Description: Forecasting Earnings Surprises
# Main outcome: Section 7 allows for choosing models and generating results
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
# Section 1: Preparing IBES-CRSP Link Table
# Note: keep only score 1 and score 2
# Main outcome variable: ibes_crsp_link_score1 and ibes_crsp_link_score2
# ==========================================================================================================================================================
# ==========================================================================================================================================================
ibes_crsp_link = pd.read_csv("./Data/Securities/[stock] ibes_crsp_link (from wrds).csv", sep = ";")
ibes_crsp_link['sdate'] = pd.to_datetime(ibes_crsp_link.sdate, format = '%d-%b-%y')
ibes_crsp_link['edate'] = pd.to_datetime(ibes_crsp_link.edate, format = '%d-%b-%y')

#Keep only score 1 and 2
ibes_crsp_link = ibes_crsp_link[ibes_crsp_link.SCORE.isin([1,2])]

#Transform the data so it can be mapped to IBES data later
ibes_crsp_link = pd.melt(ibes_crsp_link, id_vars = ['TICKER', 'PERMNO', 'NCUSIP', 'SCORE'], value_name = 'Date').drop('variable', axis = 1)
ibes_crsp_link = ibes_crsp_link.sort_values(['PERMNO', 'Date'])
ibes_crsp_link.set_index('Date', inplace = True)
ibes_crsp_link = ibes_crsp_link.groupby(['TICKER', 'PERMNO', 'NCUSIP', 'SCORE']).resample('D').ffill()
ibes_crsp_link = ibes_crsp_link.reset_index(level = [0,1,2,3], drop = True)
ibes_crsp_link = ibes_crsp_link.reset_index()
ibes_crsp_link = ibes_crsp_link.sort_values(['PERMNO', 'Date'])


#Split ibes_crsp_link into two dataframes, one for score of 1 and one for score of 2
ibes_crsp_link_score1 = ibes_crsp_link.copy(deep = True)
ibes_crsp_link_score1 = ibes_crsp_link_score1[ibes_crsp_link_score1.SCORE == 1]

ibes_crsp_link_score2 = ibes_crsp_link.copy(deep = True)
ibes_crsp_link_score2 = ibes_crsp_link_score2[ibes_crsp_link_score2.SCORE == 2]

del ibes_crsp_link

#%%
#%%
# ==========================================================================================================================================================
# ==========================================================================================================================================================
# Section 2: Import IBES data into variable esp_estimates 
#            and use IBES-CRSP Link Table to map Permno to IBES data
# Main outcome variable: esp_estimates
# ==========================================================================================================================================================
# ==========================================================================================================================================================

#Import IBES data
esp_estimates = pd.read_csv("./Data/Securities/[stock] IBES - ESP Estimates.csv")

# Format datetime variables 
esp_estimates['ANNDATS'] = pd.to_datetime(esp_estimates.ANNDATS, format = "%d/%m/%Y")
esp_estimates['FPEDATS'] = pd.to_datetime(esp_estimates.FPEDATS, format = "%d/%m/%Y")
esp_estimates['ANNDATS_ACT'] = pd.to_datetime(esp_estimates.ANNDATS_ACT, format = "%d/%m/%Y")


# Merge permno from ibes_crsp_link_score1 to ibes data
esp_estimates = pd.merge(left = esp_estimates, right = ibes_crsp_link_score1,\
                         left_on = ['CUSIP', 'ANNDATS'], \
                         right_on = ['NCUSIP', 'Date'], how = 'left').\
                         drop(['TICKER_y', 'Date', 'NCUSIP', 'SCORE'], axis = 1)
esp_estimates.rename({'TICKER_x':'TICKER'}, axis = 1, inplace = True)

# Merge permno from ibes_crsp_link_score2 to ibes data
esp_estimates = pd.merge(left = esp_estimates, right = ibes_crsp_link_score2,\
                         left_on = ['TICKER', 'ANNDATS'], \
                         right_on = ['TICKER', 'Date'], how = 'left').\
                         drop(['Date', 'NCUSIP', 'SCORE'], axis = 1)

del ibes_crsp_link_score1, ibes_crsp_link_score2


# Check if score 1 and score 2 map to overlapping observations
print("Score1 and Score2 map to unique rows in eps_esimates: {}".format(\
      esp_estimates[esp_estimates.PERMNO_x.notnull()&esp_estimates.PERMNO_y.notnull()].shape[0]==0))

# Double check: in case score 1 and score 2 map to overlapping observations,
# choose permno from score 1
esp_estimates['PERMNO'] = np.where(esp_estimates.PERMNO_x.isnull(), \
                                   esp_estimates.PERMNO_y,\
                                   esp_estimates.PERMNO_x)

esp_estimates.drop(['PERMNO_x', 'PERMNO_y'], axis = 1, inplace = True)

#drop rows where there is no PERMNO or no actual
esp_estimates = esp_estimates[(esp_estimates.PERMNO.notnull())&(esp_estimates.ACTUAL.notnull())]

#%%
# ==========================================================================================================================================================
# ==========================================================================================================================================================
# Section 3: Keep the last estimate in the intended quarter according to FPI
#
# Explanation: The variable FPI indicates which earnings actual the forecast is intended for. 
# For example, FPI = 6 indicates that the forecast is for the earnings actual in the next quarter.
# FPI = 9 indicates that the forecast is for the earnings actual four quarters ahead. 
# However, as I can observe from the dataset, FPI may not always work as intended. For example, 
# after an analyst makes a forecast for the earnings actual 4 quarters ahead, in which case 
# the FPI for this forecast is 9, the analyst can revise the forecast multiple times, each revision 
# appears in the dataset in a new observation. Oftentimes, the forecast revision gets into another quarter,
# even though the FPI is unchanged.
# Therefore, this section will clean the dataset such that it will only keep the last estimate in 
# the quarter intended by FPI. If there are more than estimates within the intended quarter, 
# then I keep the last estimate in that quarter. 
#
# Important assumption: actual announcement is for the quarter in which the announcement takes places
#               Assumption 2 is not correct. For some companies, e.g. CUSIP 36720410
#               can have two announcements in the same quarter, one for the current quarter and
#               one for the previous quarter. The data is so unsystematic I couldn't find 
#               a way to control for this yet. Fortunately, assumption 2 largely holds for 
#               most of the time 
#
# Main outcome variable: esp_estimates
# ==========================================================================================================================================================
# ==========================================================================================================================================================


esp_estimates = esp_estimates[['PERMNO', 'TICKER', 'CUSIP', 'CNAME', 'ESTIMATOR', 'FPI',\
                               'FPEDATS', 'ANNDATS', 'VALUE', 'ACTUAL', 'ANNDATS_ACT',]]

# =============================================================================
# Since each firm may have a different fiscal calendar, I align the fiscal calendar for all observations
# to standard calendar (year starting with January)
# Important assumption: months in FPEDATS indicate fiscal quarter of the firm/stock
# =============================================================================
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

# =============================================================================
#Create a variable slided_actual_annc that slides actual announcement date back according to FPI,
#this variable allows for pinning down which quarter is intended by FPI
# =============================================================================
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

#keep only rows where estimate announcement date is in the same quarter with 
# slided_actual_annc
esp_estimates = esp_estimates[(esp_estimates.ANNDATS.dt.quarter == esp_estimates.slided_actual_annc.dt.quarter)&\
                              (esp_estimates.ANNDATS.dt.year == esp_estimates.slided_actual_annc.dt.year)]

esp_estimates_before = esp_estimates.copy(deep = True)
print('Before idxmax, the dataset length is {}'.format(esp_estimates.shape[0]))


# For each stock, for each actual announcement and by each estimator, 
# keep only the rows of the last estimate in the quarter intended by FPI
esp_estimates = esp_estimates.loc[esp_estimates.groupby(['PERMNO', 'ESTIMATOR', 'FPI', 'ANNDATS_ACT']).ANNDATS.idxmax()]


# Check whether after the cleaning, 
# for each firm, each actual announcement, each estimator (analyst),
# there are now only unique FPI
temp1 = esp_estimates.groupby(['PERMNO','ESTIMATOR', 'ANNDATS_ACT']).FPI.count().reset_index().rename({'FPI':'row_count'}, axis =1)
temp2 = esp_estimates.groupby(['PERMNO','ESTIMATOR', 'ANNDATS_ACT']).FPI.nunique().reset_index().rename({'FPI':'unique_FPI'}, axis =1 )
temp = pd.merge(left = temp1, right = temp2, on = ['PERMNO', 'ESTIMATOR', 'ANNDATS_ACT'], how = 'inner')
temp['check'] = temp.row_count == temp.unique_FPI
print("After idxmax, for each stock, each estimator and each actual announcement, there are now only unique FPI: {}".format(temp[temp.check==False].shape[0]==0))
del temp, temp1, temp2

esp_estimates.drop(['fpemonth', 'align_month'], axis = 1, inplace = True)

#%%
# ==========================================================================================================================================================
# ==========================================================================================================================================================
# Section 4: Compute unexpected earnings (earnings surprises)
# ==========================================================================================================================================================
# ==========================================================================================================================================================

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
# =============================================================================
# Calculate the mean of analyst estimates for each stock-fpi-earnings announcement
# and 
# Calculate unexpected earnings
# =============================================================================
#Find the mean of estimates for each stock-fpi-earning annoucement
temp = esp_estimates.copy(deep = True)

temp = temp.groupby(['PERMNO', 'FPI', 'ANNDATS_ACT']).agg({'TICKER':'first',
                    'CUSIP': 'first', 'CNAME': 'first', 'FPEDATS':'first',\
                    'ANNDATS':'first', 'VALUE': 'mean', 'ACTUAL': 'mean',\
                    'slided_ANNDATS': 'first', 'slided_ANNDATS_ACT':'first'}).reset_index()
temp.rename({'ACTUAL':'ExpectedACTUAL'}, axis = 1, inplace = True) #because the estimate is the median or mean

#in few instances, for each permno and FPI, there could be many earning announcement in the same quarter
# This shows violation of the assumption mentioned in Section 3
# To expedite the analysis, I take the last earning announcement within a quarter
temp.sort_values(['PERMNO', 'FPI', 'ANNDATS_ACT'], inplace = True)
temp = temp.groupby(['PERMNO', 'FPI', 'slided_ANNDATS_ACT']).last()
temp.reset_index(inplace = True)


#Calculate unexpected earnings
temp['UE'] = temp.VALUE - temp.ExpectedACTUAL

temp = temp[['PERMNO', 'FPI', 'CNAME', 'ANNDATS', 'slided_ANNDATS','slided_ANNDATS_ACT', 'UE']]


#Split the resulting dataframe into 
# (1) unexpected earnings for earnings actual one quarter ahead
ibesUE6 = temp.copy(deep = True)
ibesUE6  = ibesUE6 [ibesUE6 .FPI == 6].drop('FPI', axis = 1)
ibesUE6.rename({'slided_ANNDATS': 'estimate3M', 'slided_ANNDATS_ACT': 'announcement3M', 'UE': 'UE3M'}, axis = 1, inplace = True)

# (2) unexpected earnings for earnings actual two quarters ahead
ibesUE7 = temp.copy(deep = True)
ibesUE7 = ibesUE7[ibesUE7.FPI == 7].drop('FPI', axis = 1)
ibesUE7.rename({'slided_ANNDATS': 'estimate6M', 'slided_ANNDATS_ACT': 'announcement6M', 'UE': 'UE6M'}, axis = 1, inplace = True)

# (3) unexpected earnings for earnings actual three quarters ahead
ibesUE8 = temp.copy(deep = True)
ibesUE8 = ibesUE8[ibesUE8.FPI == 8].drop('FPI', axis =1)
ibesUE8.rename({'slided_ANNDATS': 'estimate9M', 'slided_ANNDATS_ACT': 'announcement9M', 'UE': 'UE9M'}, axis = 1, inplace = True)

# (4) unexpected earnings for earnings actual four quarters ahead
ibesUE9 = temp.copy(deep = True)
ibesUE9 = ibesUE9[ibesUE9.FPI == 9].drop('FPI', axis =1)
ibesUE9.rename({'slided_ANNDATS': 'estimate12M', 'slided_ANNDATS_ACT': 'announcement12M', 'UE': 'UE12M'}, axis = 1, inplace = True)

del temp

#%%
# ==========================================================================================================================================================
# ==========================================================================================================================================================
# Section 5: Calculate aggregated active weight and aggregated squared active weight
# Main outcome variable: aggrHoldings
# ==========================================================================================================================================================
# ==========================================================================================================================================================
#Create a copy of mfHoldings to avoid modifying the variable itself
holdings = mfHoldings.copy(deep = True)

holdings.replace([np.inf, -np.inf], np.nan, inplace = True)

# =============================================================================
# Calculate active weight change, accounting to passive weight change due to price changes
# This is similarly done in the calculation of trade-based AFP, 
# function calculateTrade_BasedAFP in AFP Analysis Part 1 - Univariate.py
# I copy the explanation here again for convenience
# Explanation: I will attempt to explain how I calculate the active weight change 
# while accounting for passive weight change due to price changes. 
# The variable mfHoldings contains three columns "PriceFromForward3M", "PriceFromForward6M", "PriceFromForward12M",
# which contains price from 3, 6 and 12 months ahead. These prices are then multiplied by the 
# contemporaneous number of shares held nbr_shares. The products are then divided by 
# the contemporaneous total market value of the fund, the quotients are recorded in 
# three columns 'passiveWeight3M', 'passiveWeight6M', 'passiveWeight12M'. So as you can see, 
# these passive weight columns are calculated using future prices. In other to calculate active weight changes, 
# I need to take these three columns out of the variable mfHoldings, slide the date forward so that 
# the passive weight is aligned with the right period to calculate active weight. 
# For example, I can take the column passiveWeight3M out of mfHoldings, slide the date forward by 3 months
# when the price was used to calculate passiveWeight3M, then I merge this back to mfHoldings. 
# Because the date has been slided forward by 3M, the column passiveWeight3M is not correctly aligned 
# with the observation that contains the price that was used to calculate passiveWeight3M. 
# I repeat this for passiveWeight6M and passiveWeight12M.
# Actually this is my bad coding practice. I shouldn't keep the there variables passiveWeights in mfHoldings, 
# but to put them in a separate variable to avoid confusion.
# =============================================================================

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

# =============================================================================
# Merge the three variables with passive weighst to a copy of mfHoldings
# to calculate active weight. See the explanation above for clarification
# =============================================================================
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

# =============================================================================
# Calculate aggregated active weight changes and the square thereof
# =============================================================================
aggrHoldings = holdings.groupby(['security_name', 'slided_report_dt']).agg({'permno': 'first', 'activeWeight3M': 'sum', \
                               'activeWeight6M': 'sum', 'activeWeight12M':'sum', 'activeWeight3MSquared': 'sum',\
                               'activeWeight6MSquared': 'sum', 'activeWeight12MSquared': 'sum'})
aggrHoldings.reset_index(inplace = True)


# Remove observations with missing permno
aggrHoldings = aggrHoldings[aggrHoldings.permno.notnull()]
aggrHoldings.replace([np.inf, -np.inf], np.nan, inplace = True)

aggrHoldings.dropna(inplace = True)

#Remove where all three squared aggregated weights = 0. Likely due to missing values
aggrHoldings = aggrHoldings[(aggrHoldings.activeWeight3MSquared!=0)|(aggrHoldings.activeWeight6MSquared!=0)|(aggrHoldings.activeWeight12MSquared!=0)]


#%%
# =============================================================================
# Section 6: Merge aggreHoldings (aggregated weight changes) to ibesUE (earnings surprises),
# ready for regression analysis
# Main outcome variable: aggrHoldings
# Notice that, after this section, the variable aggrHoldings contains:
#   - three columns for the aggregated active weight changes relative to 3, 6 and 12 months prior
#   - three columns for the square of the aggregated active weight changes relative eto 3, 6 and 12 months prior
#   - three columns for the earnings surprises 3, 6, and 12 months ahead
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
# ==========================================================================================================================================================
# ==========================================================================================================================================================
# Section 7: Regression Analysis
# Two actions need to be taken:
#   - Comment the chosen model to be run and uncomment the remaining models (see below)
#   - Comment [uncomment] the chosen [unchosen] standard error clustering option (see below)
# ==========================================================================================================================================================
# ==========================================================================================================================================================
#Some preparation
from linearmodels.panel import PanelOLS
temp = aggrHoldings.copy(deep = True)
#temp = temp.replace([np.inf, -np.inf], np.nan)
temp = temp.dropna()
temp = temp.set_index(['permno', 'slided_report_dt'])

# =============================================================================
# Specify model 
# Explanation:
# The variable mod is the model. To specify the model, the following format is used:
# PanelOLS(dependent variable, independent variables [, entity_effects = False, time_effects = False])
# entity_effects (firm FE) and time_effects (Time FE) are false by default
# The models have been specified below. Please uncomment the model to be run and comment the remaining models
# =============================================================================

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


#mod = PanelOLS(temp.UE6M, temp[['activeWeight3MSquared', 'activeWeight6MSquared', 'activeWeight12MSquared']], time_effects = True)
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


mod = PanelOLS(temp.UE12M, temp[['activeWeight3MSquared', 'activeWeight6MSquared', 'activeWeight12MSquared']], entity_effects = True)
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

# =============================================================================
# Specify standard error clustering
# There are two options: cluster standard errors by 
# firm (cluster_entity) or by time (cluster_time)
# =============================================================================
res = mod.fit(cov_type = 'clustered', cluster_entity = True)
#res = mod.fit(cov_type = 'clustered', cluster_time = True)

# =============================================================================
# Print the regression result
# =============================================================================
print(res)

del temp

























