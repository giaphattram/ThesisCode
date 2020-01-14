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
# File Description: Functions for AFP analysis and AFP Univariate Result
# Main outcomes:
#   - Functions for AFP Analysis
#   - Result for univariate analysis saved into "AFPUnivariateResult.xlsx",
#     which contains figures for Table 3, A.1 and A.2
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================


#%%
# ==========================================================================================================================================================
# ==========================================================================================================================================================
# Section 1: Calculate Trade-Based AFP 
# Build a function that calculate Trade-based AFP given 3 parameters
#       parameter 1: fund holding dataframe mfHoldings
#       parameter 2: CAR dataframe threeDayCARDf
#       parameter 3: number of months between two holdings date to calculate active weights (please only input 3, 6 or 12)
# Function output: Quarterly trade-based AFP of Funds
# =============================================================================

# Even though there are some comments in the function to explain how I calculate active weight change,
# I'd like to attempt to write a full explanation how I calculate active weight change.
# Due to the way I structure the variable, this can be quite confusing.
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

# ==========================================================================================================================================================
def calculateTrade_BasedAFP(mfHoldings, threeDayCARDf, months_offset):
    # =============================================================================
    # Calculating active weight
    # =============================================================================
    # I will try to quickly explain the intuition how I code the calculation of active weight. 
    # I create two copies of mfHoldings. 
    # For the first copy, I keep only passiveWeight columns, then I slide the date of this copy forward by the number of months input to the function months_offset
    # Let's say months_offset = 3 (so the active weight will be calculated relative to the weight of the previous 3 months or one quarter) 
    # So, I slide the date of this copy forward by 3 months. Notice that the date column here is month_year, which is the month and year
    # of the column slided_report_dt, which is the holding report date slided to quarter end.
    # Then I create another copy of mfHoldings. 
    # I merge the first copy to the second copy with the keys being (fund identifer crsp_portno, security_name, and date)
    # Because the date column of the first copy has been
    # slided forward by 3 months or one quarter, the passive weight calculated by holdings nbr_shares from the previous 3 months is now 
    # aligned with the current weight, allowing for calculating active weight with a simple substraction betwen columns.
    
    
    # First copy: mfHoldingsOffset
    mfHoldingsOffset = mfHoldings.copy(deep = True)
    mfHoldingsOffset = mfHoldingsOffset[['crsp_portno', 'month_year', 'security_name', 'passiveWeight3M', 'passiveWeight6M', 'passiveWeight12M']]
    mfHoldingsOffset['month_year_offset'] = mfHoldingsOffset.month_year + pd.DateOffset(months = months_offset)
    mfHoldingsOffset['month_year_offset'] = np.where(~mfHoldingsOffset.month_year_offset.dt.is_quarter_end,\
                                                             mfHoldingsOffset.month_year_offset + pd.tseries.offsets.QuarterEnd(startingMonth = 0),
                                                             mfHoldingsOffset.month_year_offset)
    
    mfHoldingsOffset.rename({'month_year': 'month_year_preoffset'}, axis= 1, inplace = True)
    
    # Second copy: mfHoldingsTradeBased
    mfHoldingsTradeBased = mfHoldings.copy(deep = True)
    mfHoldingsTradeBased.drop(['passiveWeight3M', 'passiveWeight6M', 'passiveWeight12M'], axis = 1, inplace = True)
    
    # Merging the first copy to the second copy 
    mfHoldingsTradeBased = pd.merge(left = mfHoldingsTradeBased, right = mfHoldingsOffset, \
             left_on = ['crsp_portno', 'slided_report_dt', 'security_name'],\
             right_on = ['crsp_portno', 'month_year_offset', 'security_name'], how = 'left', indicator = True)
    del mfHoldingsOffset
    
    
    # Genearte and calculate the column active_weight depending on the function input months_offset
    if (months_offset == 3):
        mfHoldingsTradeBased['active_weight'] = mfHoldingsTradeBased.weight - mfHoldingsTradeBased.passiveWeight3M
    elif (months_offset == 6):
        mfHoldingsTradeBased['active_weight'] = mfHoldingsTradeBased.weight - mfHoldingsTradeBased.passiveWeight6M
    elif (months_offset == 12):
        mfHoldingsTradeBased['active_weight'] = mfHoldingsTradeBased.weight - mfHoldingsTradeBased.passiveWeight12M        
    
    
    mfHoldingsTradeBased.drop('_merge', axis = 1, inplace = True)
    
    # =============================================================================
    # Merge mfHoldingsTradeBased and threeDayCARDf by 
    # 'slided_report_dt' and 'slided_rdq', respectively
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
    # Calculate AFP
    # =============================================================================
    mfHoldingsTradeBased['AFP'] = mfHoldingsTradeBased.active_weight * mfHoldingsTradeBased.CAR
    
    # check the only 'slided_report_dt' that is not in 'slided_rdq' 
#    print('The only slided_report_dt that is not in slided_rdq: {}'.\
#          format(set(mfHoldingsTradeBased.slided_report_dt.unique()).difference(set(mfHoldingsTradeBased.slided_rdq.unique()))))
   
    toRemove = list(set(mfHoldingsTradeBased.slided_report_dt.unique()).difference(set(mfHoldingsTradeBased.slided_rdq.unique())))[0]    
    mfHoldingsTradeBased = mfHoldingsTradeBased[mfHoldingsTradeBased.slided_report_dt != toRemove]
    del toRemove
    
    
    mfHoldingsTradeBased = mfHoldingsTradeBased.groupby(['crsp_portno', 'slided_report_dt'])['AFP'].sum()
    
    mfHoldingsTradeBased = mfHoldingsTradeBased.to_frame().reset_index()
    
    # =============================================================================
    # The first observation of any fund will have AFP = 0 because there is no 
    # prior holding to calculate active weight => remove the first date for every fund
    # =============================================================================
#    print('Sum of AFP of first dates of all funds: {}'.format(mfHoldingsTradeBased.groupby('crsp_portno', as_index = 'False')['AFP'].first().sum()))
    mfHoldingsTradeBased.sort_values(['crsp_portno', 'slided_report_dt'], inplace = True)
    first_date_index = mfHoldingsTradeBased.groupby('crsp_portno', as_index = 'False')['slided_report_dt'].first().reset_index()
    mfHoldingsTradeBased = pd.merge(left = mfHoldingsTradeBased, right = first_date_index, on = ['crsp_portno', 'slided_report_dt'], how = 'left', indicator = True)
#    print('Sum of AFP of all merged observations with _merge == both (0 means AFP = 0 for first date of a fund): {}'.format(mfHoldingsTradeBased[mfHoldingsTradeBased._merge == 'both'].AFP.sum()))
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
# ==========================================================================================================================================================
# ==========================================================================================================================================================
# Section 2: Calculate Index-Based AFP 
# Build a function calculateIndexBasedAFP that calculate Index-based AFP given 3 parameters
#       parameter 1: fund holding dataframe mfHoldings
#       parameter 2: CAR dataframe threeDayCARDf
#       parameter 3: Constituent weights of a benchmark index
# Function output: Quarterly index-based AFP of Funds
# =============================================================================
# ==========================================================================================================================================================
def calculateIndexBasedAFP(mfHoldings, threeDayCARDf, sp_constituents): 
    #Merge sp_constituents to mfHoldings
    tempMfHoldings = mfHoldings.copy(deep = True)
    tempMfHoldings = tempMfHoldings[['crsp_portno', 'report_dt', 'eff_dt', 'security_name',\
                                      'cusip', 'permno', 'weight', 'slided_report_dt']]#, 'is_fixed_income']]
    tempMfHoldings = pd.merge(left = tempMfHoldings, right = sp_constituents,\
                    left_on = ['report_dt', 'permno'], right_on = ['Date', 'permno'],\
                    how = 'left')
    
    #Compute active weight 
    tempMfHoldings['active_weight'] = tempMfHoldings.weight - tempMfHoldings.constituent_weight
    
    # =============================================================================
    # Merge CAR 
    # =============================================================================
    tempCAR = threeDayCARDf.copy(deep = True)
    tempCAR = tempCAR[['PERMNO', 'CUSIP', 'COMNAM', 'rdq', 'slided_rdq', 'CAR']]
    tempCAR.drop_duplicates(keep = 'first', inplace = True)
    tempMfHoldings = pd.merge(left = tempMfHoldings, right = tempCAR, \
             left_on = ['permno', 'slided_report_dt'], right_on = ['PERMNO', 'slided_rdq'], \
             how = 'left', indicator = True)
#    print('Merging CAR to holdings data results in {} duplicates'.format(tempMfHoldings.duplicated(keep = False).sum()))
    del tempCAR
    
    #Removing all rows that do not have CAR mapped to
    tempMfHoldings = tempMfHoldings[tempMfHoldings.CAR.notnull()]
    #After this, the dataset still has 2464 unique crsp_portno 
    tempMfHoldings.drop('_merge', axis = 1,inplace = True)
           
    # =============================================================================
    # Calculate AFP for each fund 
    # =============================================================================
    # calculate AFP for each security at each quarter for each fund
    tempMfHoldings['AFP'] = tempMfHoldings.active_weight * tempMfHoldings.CAR
   
    # calculate AFP for each fund at each quarter by summing up AFP across all securities held by the fund
    tempIndexBasedAFP = tempMfHoldings.groupby(['slided_report_dt', 'crsp_portno']).AFP.sum()
    tempIndexBasedAFP = tempIndexBasedAFP.reset_index()
    
    
    # =============================================================================
    # Remove days where all funds have AFP = 0
    # =============================================================================
    tempIndexBasedAFP['Squared_AFP'] = tempIndexBasedAFP.AFP.apply(lambda x: x*x)   
    gbObj = tempIndexBasedAFP.groupby('slided_report_dt').Squared_AFP.sum()
    days_all_fund_AFP_is_zero = gbObj[gbObj == 0].index
    tempIndexBasedAFP.drop(tempIndexBasedAFP[tempIndexBasedAFP.slided_report_dt.isin(days_all_fund_AFP_is_zero)].index, axis = 0, inplace = True)
    tempIndexBasedAFP.drop('Squared_AFP', axis = 1, inplace = True)
    del gbObj
    
    return tempIndexBasedAFP.copy(deep = True)


#%%
# ==========================================================================================================================================================
# ==========================================================================================================================================================
# Section 3: Create a function calculateDecilePortfolioReturn
# that calculates decile portfolio returns given two input parameters:
#   - AFP_Frame_input: a dataframe containing AFP measure of funds. 
#                      This input is generated by the two functions above, namely
#                       calculateTrade_BasedAFP and calculateIndexBasedAFP
#    - mfReturns: monthly tna-weighted fund returns, which is the outcome variable of
#                 of "Main - Data Preparation.py" Section 3
# Additionally, this function also merge holding returns of factor-mimicking portfolios 
# from the variable FactorPortfolioReturns to decile portfolio returns
# Function output: Quarterly Returns of AFP Decile Portfolios 
# ==========================================================================================================================================================
# ==========================================================================================================================================================
    
def calculateDecilePortfolioReturn(AFP_Frame_input, mfReturns):
    AFPdf = AFP_Frame_input.copy(deep = True)
    
    # Remove observations wherein AFP = 0 or infinity
    AFPdf.replace([np.inf, -np.inf, np.nan], value = 0, inplace = True)
    AFPdf = AFPdf[AFPdf.AFP != 0]
    
    # Sort funds into decile portfolios based on their AFP
    # Decile portfolio indicator is a column 'DecilePortfolio' in the dataframe AFPdf
    AFPdf['DecilePortfolio'] = AFPdf.groupby('slided_report_dt')['AFP'].transform(lambda x: pd.cut(x, 10, labels = range(1,11), include_lowest = True))
    AFPdf['DecilePortfolio'] = AFPdf.DecilePortfolio.astype(int)
    
    
    ####
    # The variable mfReturns has a variablet caldt (calendar date)
    # To calculate holding return for decile portfolio,
    # I slide caldt to the quarter end where the AFP decile portfolios are formed
    # For example: After forming decile portfolios at the end of March, I need to calculate return
    # from holding the portfolios from June to August.
    # I create a column slided_caldt in mfReturns that slides fund returns 
    #       - in month 6, 7, 8 to 3
    #       - in month 9, 10, 11 to 6
    #       - in month 12, 1, 2 to 9
    #       - in month 3, 4, 5 to 12 
    # As such, slided_caldt will be aligned with slided_report_dt
    ####
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
    ####    
    
    # Merge tempMfReturns to AFPdf
    AFPdf = pd.merge(left = AFPdf, right = tempMfReturns, \
                    left_on = ['crsp_portno', 'slided_report_dt'], \
                    right_on = ['crsp_portno', 'slided_caldt'], \
                    how = 'outer', indicator = True)
    
    AFPdf.sort_values(['crsp_portno', 'caldt'], inplace = True)
    AFPdf = AFPdf[~AFPdf.DecilePortfolio.isnull()]
    

    # For each decile portfolio at any given quarter end, the monthly return of the portfolio 
    # is the equal-weighted returns across all component funds 
    # E.g. for a decile portfolio formed at the end of March, the monthly return of this portfolio
    # in June, July, August is the equal-weighted return across all component funds in June, July, August ,respectively
    TradeBasedResult = AFPdf.groupby(['DecilePortfolio', 'slided_report_dt', 'caldt']).mret.mean()
    TradeBasedResult = TradeBasedResult.to_frame().reset_index()
    
    ####
    # Use slided_report_dt calculated above to calculate holding return
    ####
    TradeBasedResult['mret'] = TradeBasedResult.mret + 1
    
    TradeBasedResult = TradeBasedResult.groupby(['DecilePortfolio', 'slided_report_dt']).mret.prod()
    TradeBasedResult = TradeBasedResult.to_frame().reset_index()
    
    TradeBasedResult['mret'] = TradeBasedResult.mret - 1
    ####
    
    # Merge FactorPortfolioReturns to decile portfolio returns
    TradeBasedResult = pd.merge(left = TradeBasedResult, right = FactorPortfolioReturns,\
         left_on = 'slided_report_dt', right_on = 'slided_date',
         how = 'outer')
    TradeBasedResult['excess_mret'] = TradeBasedResult.mret - TradeBasedResult.RF
    TradeBasedResult['constant'] = np.ones(TradeBasedResult.shape[0])
    
    #Remove rows where Decile Portfolio is nan
    TradeBasedResult = TradeBasedResult[TradeBasedResult.DecilePortfolio.notnull()]
    
        
        
    return TradeBasedResult.copy(deep = True)


#%%
# ==========================================================================================================================================================
# Section 4: create a function univariateAFPResultGenerator
# that generate the result for univariate analysis
# An example of the function output is one of the panels in Table 3 or Table A.1 or A.2
# Input parameter: Decile portfolio return (output of the function calculateDecilePortfolioReturn)
# ==========================================================================================================================================================
def univariateAFPResultGenerator(calculateDecilePortfolioReturn_result):
    AFPPortfolio = calculateDecilePortfolioReturn_result.copy(deep = True)
    AFPPortfolio = AFPPortfolio.replace([np.inf, -np.inf], np.nan)
    AFPPortfolio.dropna(inplace = True)
    DecileGbObject = AFPPortfolio.groupby('DecilePortfolio')
    constantList = []
    
    # =============================================================================
    # =============================================================================
    # Analyse 10 decile portfolios
    # Store the resulting parameters in constantList
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
    # Analyse the long-short portfolio (Decile 10 - Decile 1)
    # Store the resulting parameters in constantList
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
    # Transform the resulting parameters stored in constantList into nice tabular format
    # To be more detailed:
    #   + Convert constantList to constantDf
    #   + Pivot constantDf into nice pivot tables
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
# Section 5: Use the variables and functions from the previous sections
# to generate Table 3, Table A.1 and Table A.2 
# =============================================================================
indexAFP_frame = calculateIndexBasedAFP(mfHoldings, threeDayCARDf, sp_constituents)
tradeAFP3M_frame = calculateTrade_BasedAFP(mfHoldings, threeDayCARDf, 3)
tradeAFP6M_frame = calculateTrade_BasedAFP(mfHoldings, threeDayCARDf, 6)
tradeAFP12M_frame = calculateTrade_BasedAFP(mfHoldings, threeDayCARDf, 12)
  
AFPPortfolioIndex = calculateDecilePortfolioReturn(indexAFP_frame, mfReturns)    
AFPPortfolioTrade3M = calculateDecilePortfolioReturn(tradeAFP3M_frame, mfReturns)
AFPPortfolioTrade6M = calculateDecilePortfolioReturn(tradeAFP6M_frame, mfReturns)
AFPPortfolioTrade12M = calculateDecilePortfolioReturn(tradeAFP12M_frame, mfReturns)


writer = pd.ExcelWriter('AFPUnivariateResult.xlsx')
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


