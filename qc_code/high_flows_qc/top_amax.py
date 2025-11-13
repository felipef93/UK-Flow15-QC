import pandas as pd
import numpy as np
from high_flow_checks.auxiliary_calculate_amax import calculate_amax


def detect_above_second_highest_amax(data: pd.DataFrame, 
             value_col: str = 'value',
             factor_second_highest: int = 2) -> pd.DataFrame:
    """
    Identify the top annual maximum flow in a DataFrame and check if it is significantly higher 
    than the second highest.

    Parameters
    ----------
    data : pd.DataFrame - A DataFrame containing your flow data.
    value_col: str - The name of the column containing flow values (default 'value').
    factor : int - The factor by which the highest amax should exceed the second highest to be 
    considered significant (default 2).

    Returns
    -------
    data: pd.DataFrame - The same df with a new column 'top_amax' indicating the date of the 
    top amax and a value of 1, or NaN if not applicable.
    """


    amax=calculate_amax(data)
    amax=amax.sort_values(value_col, ascending=False)
    amax=list(amax[value_col])


    amax=[x for x in amax if x!=0]  # Drop all the 0 values (some timeseries have 0s for years)

    if len(amax)>2:
        if (amax[0]/amax[1])>factor:
            top_date=data[data[value_col]==amax[0]].index[0]
            top_amax=pd.Series([1], index=[top_date])
        else:
            top_amax=np.NaN
    else:
        top_amax=np.NaN
    data['top_amax']=top_amax
    return data
