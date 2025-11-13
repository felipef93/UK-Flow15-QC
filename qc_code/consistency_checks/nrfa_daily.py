import pandas as pd
import numpy as np
from .auxiliary_functions import match_nrfa, resample_daily_to_15min

# Check consistency with the NRFA datasets

def detect_differences_daily(data: pd.DataFrame, 
                           nrfa_data: pd.DataFrame, 
                           threshold: float = 0.05)-> pd.DataFrame:
    
    """
    Merge flow data with NRFA reference data and compute relative difference.

    Parameters
    ----------
    data : pd.DataFrame - A DataFrame containing your flow data.
    nrfa_data : pd.DataFrame - nrfa data extracted from the NRFA website.
    data_type : float - Relative difference threshold for flagging. (default 0.05)

    Returns
    -------
    data: pd.DataFrame - The same df with timesteps different from NRFA data flagged in a separate 
    column

    """    
    differences_daily=match_nrfa(data, nrfa_data, data_type='daily')
    differences_daily['flagged']=(differences_daily['diff'] > threshold).astype(int)
    # convert daily data to 15min data
    if len(differences_daily['flagged']) >0:
        data['nrfa_daily']=resample_daily_to_15min(differences_daily['flagged'])
    else:
        data['nrfa_daily']=None
        print('No data to compare')

    return data