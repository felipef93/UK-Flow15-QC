import pandas as pd
import numpy as np
from scipy.stats import genextreme
from high_flows_qc.auxiliary_calculate_amax import calculate_amax


def gev_fit(data: pd.DataFrame,
            value_col: str = "value") -> pd.DataFrame:
    """
    Fit a GEV distribution to annual maxima and compute the return period for
    each observation.
    """
    annual_maximas = calculate_amax(data, value_col=value_col)
    annual_maximas = annual_maximas.dropna(subset=[value_col])

    params = genextreme.fit(annual_maximas[value_col])
    cdf_values = genextreme.cdf(data[value_col], *params)
    data['return_period'] = 1 / (1 - cdf_values)

    return data


def detect_high_return_period(data: pd.DataFrame,
                              threshold_rp: int = 1000) -> pd.DataFrame:
    """
    Flag timesteps whose estimated return period exceeds threshold_rp.
    """
    data['above_1000_rp'] = (data['return_period'] > threshold_rp).astype(int)
    return data
