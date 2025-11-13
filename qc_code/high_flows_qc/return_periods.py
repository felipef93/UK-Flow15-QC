import pandas as pd
import numpy as np
from scipy.stats import genextreme
from high_flow_checks.auxiliary_calculate_amax import calculate_amax

def gev_fit(data: pd.DataFrame,
            value_col: str = 'value') -> pd.DataFrame:
    '''
    Fit a Generalized Extreme Value (GEV) distribution to annual maxima and compute the return period for each observation.
    Parameters
    ----------
    data : pd.DataFrame - A DataFrame containing your flow data.
    value_col: str - The name of the column containing flow values (default 'value').
    
    Returns
    -------
    data: pd.DataFrame - The same df with a new column that estimates the RP for each observation.
    '''

    annual_maximas = calculate_amax(data)

    annual_maximas = annual_maximas.dropna(subset=[value_col])
  
    params=genextreme.fit(annual_maximas[value_col])
    cdf_values = genextreme.cdf(data[value_col], *params)
    data['return_period']=1/(1-cdf_values)

    return data

def detect_high_return_period(data,
                              threshold_rp: int = 1000) -> pd.DataFrame:
    """
    Identify timesteps of high return periods in flow data.
    Parameters
    ----------
    data : pd.DataFrame - A DataFrame containing your flow data.
    threshold : int - The return period threshold above which flows are considered high (default 1000).

    Returns
    -------
    data: pd.DataFrame - The same df with a new column indicating whether the return period exceeds 
    the threshold.
    """
    data['above_1000_rp']=(data['return_period']>1000).astype(int)
    return data