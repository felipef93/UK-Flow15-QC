import pandas as pd
import numpy as np
from scipy.stats import genextreme
from auxiliary_calculate_amax import calculate_amax

def gev_fit(data):
    '''
    Fits a Generalized Extreme Value distribution to the data
    args:
        data: pd.Series containing the data to fit
        threshold: float, the threshold above which the data is considered
    returns:
        dict containing the parameters of the distribution
    '''
    # Calculate annual maximas:
    annual_maximas = calculate_amax(data)
    # Fit the distribution
    params=genextreme.fit(annual_maximas['value'])
    cdf_values = genextreme.cdf(data['value'], *params)
    data['return_period']=1/(1-cdf_values)

    return data

def above_1000_rp(data):
    # Get the data above 1000 return period
    data['above_1000_rp']=(data['return_period']>1000).astype(int)
    return data