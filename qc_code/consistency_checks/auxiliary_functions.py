import pandas as pd

"""
auxiliary_functions — helper routines for consistency QC checks against NRFA datasets.
"""


def match_nrfa(data: pd.DataFrame, 
               nrfa_data: pd.DataFrame, 
               data_type: str ='daily')-> pd.DataFrame or None:
    """
    Merge flow data with NRFA reference data and compute relative difference.

    Parameters
    ----------
    data : pd.DataFrame - A DataFrame containing your flow data.
    nrfa_data : pd.DataFrame - nrfa data extracted from the NRFA website.
    data_type : {'daily','amax','pot'} - Type of comparison to perform.

    Returns
    -------
    merged_set (pd.DataFrame) or None:
        Merged dataset if NRFA data is available and inputs are valid
        or None if inputs are invalid.
    """
    def convert_date_and_time_to_datetime(nrfa_data: pd.DataFrame)-> pd.DataFrame:
        """
        Combine separate date and time columns into a single 'datetime' column.

        Returns
        -------
        nrfa_data : pd.DataFrame - DataFrame w/ a new 'datetime' instead of 'Date' and 'Time' columns.
        """
        nrfa_data['Date'] = pd.to_datetime(nrfa_data['Date'], format='%d/%m/%Y')
        nrfa_data['datetime'] = pd.to_datetime(nrfa_data['Date'].dt.strftime('%Y-%m-%d') + ' ' + nrfa_data['Time'])
        nrfa_data = nrfa_data.drop(columns=['Date', 'Time'])
        return nrfa_data

    def daily_resample(data: pd.DataFrame,
                    value_col: str = 'value') -> pd.Series:
        """
        Resample 15-min data to daily following NRFA conventions (9 AM start of the day, mean value).

        Parameters
        ----------
        value_col: str - The name of the column containing flow values (default 'value').

        Returns
        -------
        daily_data: pd.Series - Daily-aggregated series indexed by date.        
        """    
        data.index=pd.to_datetime(data['datetime'])
        daily_data=data[value_col].resample('D', offset='9H').mean()
        daily_data.index=daily_data.index.date
        return daily_data

    if len(data) > 0 and (not nrfa_data is None):
        if data_type=='daily':
            values=daily_resample(data)
        elif data_type=='amax' or data_type=='pot':
            values=data
            # set the index as datetime
            values.index=pd.to_datetime(values['datetime'])
            convert_date_and_time_to_datetime(nrfa_data)
            nrfa_data.index = pd.to_datetime(nrfa_data['datetime'])
            nrfa_data=nrfa_data.rename(columns={'Flow (m3/s)': 'value'})
          
        merged_set = pd.merge(
            nrfa_data, 
            values, 
            left_index=True, 
            right_index=True, 
            how='inner'
        )
        # Calculate the difference between the sets
        merged_set['diff'] = abs(merged_set['value_x'] - merged_set['value_y']) / (
            (merged_set['value_x'] + merged_set['value_y']) * 0.5
        )
        

        return merged_set
    else:
        return None
    

def resample_daily_to_15min(daily_data: pd.Series) -> pd.Series:
    """
    Auxiliary function to convert the flagged days with difference to a 15-min flagged series.

    Parameters
    ----------
    daily_data: pd.Series - Daily-aggregated flags indexed by date.

    Returns
    -------
    daily_extended:pd.Series - Flags extended to fill all the timesteps of the 15-minute series.
    """
    # Shift the daily index to align with the custom day start
    shifted_index = daily_data.index 

    # Create a new time range spanning the full period at 15-minute intervals
    full_range = pd.date_range(
        start=shifted_index.min(), 
        end=shifted_index.max() + pd.Timedelta(hours=23, minutes=59, seconds=59), 
        freq='15T'
    )
    # Reindex the daily data to match the full range, forward-filling values to span each "day"
    daily_extended = daily_data.reindex(shifted_index).reindex(full_range, method='ffill')
    # shift the data 9 hours
    daily_extended.index = daily_extended.index + pd.Timedelta(hours=+9)
    
    return daily_extended
