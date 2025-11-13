import pandas as pd

"""
high_values — detect and flag excessively high flow values in time‐series data.
"""

def detect_high_values(data: pd.DataFrame, 
                       value_col: str = 'value', 
                       threshold: float = 5000) -> pd.DataFrame:
    """
        Identify and flag any rows where the flow measurement exceeds a specified threshold.

        Parameters
        ----------
        data : pd.DataFrame - A DataFrame containing your flow data.
        value_col: str - The name of the column containing flow values (default 'value').
        threshold: float - The threshold value to flag high flow values (default 5000 m3/s).
        
        Returns
        -------
        data: pd.DataFrame - The same df with high values flagged in a separate column ('high')
    """          
     
    data['high'] = (data[value_col] > threshold).astype(int)
    return data

