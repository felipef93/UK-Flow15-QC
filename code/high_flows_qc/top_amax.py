import pandas as pd
from high_flows_qc.auxiliary_calculate_amax import calculate_amax


def detect_above_second_highest_amax(data: pd.DataFrame,
                                     value_col: str = "value",
                                     factor_second_highest: int = 2) -> pd.DataFrame:
    """
    Flag the top annual maximum when it is larger than the second highest annual
    maximum by the requested factor.
    """
    amax = calculate_amax(data, value_col=value_col)
    amax = amax.dropna(subset=[value_col]).sort_values(value_col, ascending=False)
    amax = amax[amax[value_col] != 0]

    data['top_amax'] = 0

    if len(amax) > 1:
        if (amax.iloc[0][value_col] / amax.iloc[1][value_col]) > factor_second_highest:
            top_datetime = amax.iloc[0]['datetime']
            data.loc[pd.to_datetime(data['datetime']) == pd.to_datetime(top_datetime), 'top_amax'] = 1

    return data
