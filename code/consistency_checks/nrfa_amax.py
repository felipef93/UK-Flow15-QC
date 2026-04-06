import pandas as pd
from consistency_checks.auxiliary_functions import match_nrfa



def detect_differences_amax(data: pd.DataFrame, 
                          nrfa_data: pd.DataFrame, 
                          threshold: float = 0.2)-> pd.DataFrame:
    

    differences_amax=match_nrfa(data, nrfa_data, data_type='amax')
    differences_amax['flagged']=(differences_amax['diff'] > threshold).astype(int)
    data['nrfa_amax']=differences_amax['flagged']
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
    return data

def detect_differences_pot(data: pd.DataFrame, 
                         nrfa_data: pd.DataFrame, 
                         threshold: float = 0.2)-> pd.DataFrame:
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
    differences_pot=match_nrfa(data, nrfa_data, data_type='pot')
    differences_pot['flagged']=(differences_pot['diff'] > threshold).astype(int)
    data['nrfa_pot']=differences_pot['flagged']

    return data