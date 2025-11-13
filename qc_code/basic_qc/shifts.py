import pandas as pd
import numpy as np

"""
shifts — detect rapid up/down “shifts” in time-series flow data over a sliding window.
"""

def detect_shifts(data: pd.DataFrame,
    value_col: str = 'value',
    window_lenght: int = 16,
    low_diff_quantile: float = 0.05,
    shift_count_thresh: int = 8) -> pd.DataFrame:

    """
    Identify periods with a high frequency of rapid up/down reversals (“shifts”).

    Parameters
    ----------
    data : pd.DataFrame - A DataFrame containing your flow data.
    value_col: str - The name of the column containing flow values (default 'value').
    window : int - Rolling window size (in number of rows) to scan for shifts (default 16)
    low_diff_quantile : float - Quantile of absolute flow changes below which changes are 
                        treated as zero (default 0.05).        
    shift_count_thresh : int - Number of sign flips in the window that triggers a shift flag 
                        (default 8)

    Returns
    -------
    data: pd.DataFrame - The same df with high values flagged in a separate column ('shifts')
    """

    df=data.copy()
    df['sign']=(np.sign(df[value_col].diff()))
    for n in df[df[value_col].diff().abs()<df[value_col].abs().quantile(low_diff_quantile)].index:
        df.at[n,'sign']=0

    def shifts_in_window(window_lenght):
        return np.sum(abs(np.diff(window_lenght)) == 2)
    
    df['shifts'] = df['sign'].rolling(window=window_lenght).apply(shifts_in_window, raw=True).fillna(0).astype(int)
    data['shifts']=(df['shifts']>shift_count_thresh).astype(int)

    for index in data[data['shifts']==1].index:
        data.loc[index-window_lenght:index,'shifts']=1
    
    return data
