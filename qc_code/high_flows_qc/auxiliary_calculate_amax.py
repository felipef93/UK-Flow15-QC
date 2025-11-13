import pandas as pd

"""
annual_maxima — compute annual maximum flows using a hydrological water year.
"""

def calculate_amax(data: pd.DataFrame,
                   value_col: str = 'value') -> pd.DataFrame:
    """
    Calculate the annual maximum flow for each water year. A water year starts on October 1 
    (month=10) and ends on September 30 of the following calendar year.

    Parameters
    ----------
    data : pd.DataFrame - A DataFrame containing your flow data.
    value_col: str - The name of the column containing flow values (default 'value').

    Returns
    -------
    annual_maximas: pd.DataFrame - A DataFrame containing the annual maximum flow values for each 
    water year.
    """
    data.index = pd.to_datetime(data['datetime'])
    water_year = [ele.year if ele.month<10 else ele.year+1 for ele in data.index]
    locator = data.groupby(water_year)[value_col].idxmax()
    annual_maximas= data.loc[locator]
    return annual_maximas
