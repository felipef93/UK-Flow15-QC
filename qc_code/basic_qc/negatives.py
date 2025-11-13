import pandas as pd

"""
negatives — detect negative flow values in time‐series data.
"""

def detect_negatives(data: pd.DataFrame,
                     value_col: str = 'value') -> pd.DataFrame:
    """
        Identify all rows where the flow measurement is negative.

        Parameters
        ----------
        data : pd.DataFrame - A DataFrame containing your flow data.
        value_col: str - The name of the column containing flow values (default 'value').

        Returns
        -------
        data: pd.DataFrame - The same df with negative values flagged in a separate column 
        (negatives).
    """           

    data['negatives'] = (data[value_col]<0).astype(int)
    return data



### Function used for testing negatives that happened consecutively. Could be implemented in future
### if useful
# def detect_longer_negatives(data):
#     data['negative'] = (data['value']<0).astype(int)
#     # Create a group identifier for consecutive negative values
#     data['group'] = (data['negative'] != data['negative'].shift()).cumsum()

#     # Count size of each group
#     data['group_sizes'] = data.groupby('group')['negative'].transform('sum')

#     # If the group size is greater than 96 then data['negative'] = 2
#     data.loc[data['group_sizes'] >= 96, 'negative'] = 2

#     # remove all created columns but keep the 'negative' column
#     data.drop(columns=['group', 'group_sizes'], inplace=True)

#     return data
