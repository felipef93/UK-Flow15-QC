import numpy as np
import pandas as pd

def detect_high_std(data: pd.DataFrame,
                    value_col: str = "value",
                    n_std: int = 6) -> pd.DataFrame:
    """
    Flag flow values that exceed mean + n_std * std in log-transformed space.
    """
    logged = np.log(data[value_col] + 0.01)
    mean = logged.mean()
    std = logged.std()
    threshold = mean + n_std * std

    data['above_6std'] = (logged > threshold).astype(int)
    return data
