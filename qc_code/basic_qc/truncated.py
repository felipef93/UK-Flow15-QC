import pandas as pd



def detect_truncated(
    data: pd.DataFrame,
    value_col: str = 'value',
    truncation_window: int = 672,
    high_flow_truncation_window: int = 96,
    high_flow_quantile: float = 0.99,) -> pd.DataFrame:

    """
        Detect “truncation” (constant runs) on weekly and daily scales,
        plus daily truncation at high flows.

        Internally defines a helper `truncated()` that returns a 0/1 Series
        marking runs of length `window` where all values are identical.

        Parameters
        ----------
        data : pd.DataFrame - A DataFrame containing your flow data.
        value_col: str - The name of the column containing flow values (default 'value').
        rel_threshold : Multiplicative factor for the relative drop (default 5.0).
        truncation_window : int - Number of timesteps for a truncation to be flagged for
        any flow value (default 672 - 1 week).
        high_flow_truncation_window :  int - Number of timesteps for a truncation to be flagged for
        during high flows (default 96 - 1 day).
        high_flow_quantile : float - Quantile above which high flow truncations are flagged as 
        'high' (default 0.99)

        Returns
        -------
        data: pd.DataFrame - The same df with two additiona columns of flags, one for any 
        truncation ('truncated') and one for high flows truncation ('high_flow_truncations').
    """
    def truncated(data, window: int):
        # If the same value is repeated for more than x timesteps than it's considered truncated
        truncated = (data[value_col].rolling(window=window)
                                    .apply(lambda x: len(set(x)) == 1, raw=True)
                                    .fillna(0)
                                    .astype(int))
        # Flag all the truncated values in the window, not just the last one
        for n in truncated[truncated==1].index:
            truncated[n-window+1:n]=1
        return truncated


    data['truncation']=truncated(data, truncation_window)
    data['high_truncation']=((data[value_col] > data[value_col].quantile(high_flow_quantile)) 
                             & (truncated(data, high_flow_truncation_window))).astype(int)
    return data
