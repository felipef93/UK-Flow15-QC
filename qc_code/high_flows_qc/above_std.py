import numpy as np
import pandas as pd

def detect_high_std(data: pd.DataFrame,
                    value_col: str = 'value',
                    n_std: int = 6) -> pd.DataFrame:
    """
    Flag flow values that exceed mean + n_std * std in log-transformed space.

    Parameters
    ----------
    data : pd.DataFrame - A DataFrame containing your flow data.
    value_col: str - The name of the column containing flow values (default 'value').
    n_std : int - Number of standard deviations above mean to define high flows (default 6).

    Returns
    -------
    data: pd.DataFrame - The same df with values above x_std flagged in a separate column 
    (above_x_std).
    """
    logged=np.log(data[value_col]+0.01) # Adding a small constant to avoid log(0)

    mean=logged.mean()
    std=logged.std()

    threshold=mean+n_std*std

    high_dev = (logged > threshold).astype(int)

    data['above_6std']=high_dev
    return data
