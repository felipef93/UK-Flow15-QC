import pandas as pd


def calculate_amax(data: pd.DataFrame,
                   value_col: str = "value") -> pd.DataFrame:
    """
    Calculate the annual maximum flow for each water year without modifying the
    input DataFrame. Water year runs from October to September.
    """
    working = data.copy()
    working['datetime'] = pd.to_datetime(working['datetime'])

    water_year = [dt.year if dt.month < 10 else dt.year + 1 for dt in working['datetime']]
    locator = working.groupby(water_year)[value_col].idxmax()
    annual_maximas = working.loc[locator]

    return annual_maximas
