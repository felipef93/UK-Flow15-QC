import pandas as pd

"""
spikes — detect relative and absolute spikes in time‐series flow data.
"""

def detect_rel_spikes(data: pd.DataFrame,
               value_col: str = 'value', 
               rel_threshold: float = 5.0):
    """
        Identify rows where the flow series has a sharp relative spike (details: see publication).

        Parameters
        ----------
        data : pd.DataFrame - A DataFrame containing your flow data.
        value_col: str - The name of the column containing flow values (default 'value').
        rel_threshold : Multiplicative factor for the relative up/down check (default 5.0).

        Returns
        -------
        data: pd.DataFrame - The same df with relative spikes values flagged in a separate column 
        ('rel_spike')
        """    

    df=data.copy()
    
    df['r_up']   = df[value_col].diff() / df[value_col].shift(1)       # (q_x - q_{x-1})/q_{x-1}
    df['r_down'] = -(df[value_col].diff(-1)) / df[value_col]             # (q_{x+1}-q_x)/q_x


    data['rel_spike'] = (
        (df['r_up'] > rel_threshold-1) & (df['r_down'] < 1/(rel_threshold)-1) # 1/5 = 0.2 drop 
    | (df['r_up'] < 1/(rel_threshold)-1) & (df['r_down'] > rel_threshold-1) #→ rate_of_fall < -0.8
        ).astype(int)
    
    return data

def detect_abs_spikes(data: pd.DataFrame,
               value_col: str = 'value', 
               abs_threshold: float = 0.99):
    """
        Identify rows where the flow series has a sharp absolute spike (details: see publication).

        Parameters
        ----------
        data : pd.DataFrame - A DataFrame containing your flow data.
        value_col: str - The name of the column containing flow values (default 'value').
        abs_threshold : For quantile for the absolute up/down check (default 0.99).

        Returns
        -------
        data: pd.DataFrame - The same df with relative spikes values flagged in a separate column 
        ('abs_spike')
        """    
    threshold=data[value_col].quantile(abs_threshold)
    data['abs_spike']= ((data[value_col].diff()> threshold) & 
                        (data[value_col].diff().shift(-1) < -threshold)  |
                        (data[value_col].diff()< -threshold) 
                        & (data[value_col].diff().shift(-1) > threshold) ).astype(int)    
    return data
