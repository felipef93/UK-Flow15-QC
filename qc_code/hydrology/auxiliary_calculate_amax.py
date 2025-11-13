import pandas as pd

def calculate_amax(data):
    water_year = [ele.year if ele.month<10 else ele.year+1 for ele in data.index]
    locator = data.groupby(water_year)['value'].idxmax()
    annual_maximas= data.loc[locator]
    return annual_maximas
