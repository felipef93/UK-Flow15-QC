import pandas as pd

"""
drops — detect drops in the time‐series flow data.
"""

def detect_drops(data: pd.DataFrame,
               value_col: str = 'value', 
               threshold: float = 5.0):
    """
        Identify rows where the flow series has a drop (details: see publication).

        Parameters
        ----------
        data : pd.DataFrame - A DataFrame containing your flow data.
        value_col: str - The name of the column containing flow values (default 'value').
        threshold : Multiplicative factor for the relative drop (default 5.0).

        Returns
        -------
        data: pd.DataFrame - The same df with relative drops flagged in a separate column ('drops')
        """    

    df=data.copy()

    df['drop_pct'] = df[value_col].diff() / df[value_col].shift(1)
    data['drops'] = (df['drop_pct'] < 1/(threshold)-1).astype(int)

    return data