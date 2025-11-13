import pandas as pd
import numpy as np
from return_periods import gev_fit

def gev_fit_rainfall(rainfall_data, flow_data):
    # Crop the rainfall data to only the period where I have flows
    flow_start=flow_data.index[0].date()
    flow_end=flow_data.index[-1].date()
    rainfall_data=rainfall_data[flow_start:flow_end]
    gev_fit(rainfall_data)
    # Get the moving average of 7 days rainfall
    #rainfall_data['moving_avg_7d']=rainfall_data['value'].rolling(window=7).sum()
    return rainfall_data

def compare_flow_rainfall(predictor, 
                          response, 
                          value_col='value',
                          return_period=10,
                          predictor_type='flow',
                          window_days=3,
                          resp_val_col='value',
                          quantile=0.99,
                          ratio_thresh=20
                          ):
    def get_peak_events(predictor: pd.DataFrame = predictor, 
                        return_period: int =return_period, 
                        value_col='value') -> pd.DataFrame:
        above_rp=predictor[predictor['return_period']>=return_period]
        if above_rp.empty or response.empty:
            extreme_peaks=pd.DataFrame()
            print('No data or no event above 10yr RP')
            pass
        else:    
            # From the flow timeseries, identify only events with 10+ return periods with at least a day inbetween them
            above_rp['time_difference']=above_rp.index.to_series().diff().dt.days
            above_rp['event_nbr']=(above_rp['time_difference']>1).cumsum()
            peak_locations=above_rp.groupby('event_nbr')[value_col].idxmax()
            extreme_peaks=above_rp.loc[peak_locations]
        return extreme_peaks
    # Now, for each event, find the corresponding rainfall data

    response = gev_fit_rainfall(response, predictor)

    def get_response_events(extreme_peaks: pd.DataFrame,
                            response: pd.DataFrame,
                            predictor_type: str = 'flow',
                            window_days: int = window_days,
                            resp_val_col: str = resp_val_col,
                            quantile: float = quantile) -> pd.DataFrame:
        """
        For each peak in `extreme_peaks`, extract the corresponding response window
        and compute:
        - max_response (max return period in that window)
        - above_99th_quantile (1 if NONE of the resp_val_col exceed the global `quantile`, else 0)

        Returns a DataFrame indexed the same as `extreme_peaks` with columns:
        ['max_response','above_99th_quantile']
        """
        # ensure datetime column
        extreme_peaks['datetime'] = pd.to_datetime(extreme_peaks['datetime'])
        
        records = []
        for idx, row in extreme_peaks.iterrows():
            dt = row['datetime']
            if predictor_type == 'flow':
                start, end = dt - pd.Timedelta(days=window_days), dt
            else:  # 'rainfall'
                start, end = dt, dt + pd.Timedelta(days=window_days)
            
            window = response.loc[start:end]
            if window.empty:
                max_resp = np.nan
                above_q = np.nan
            else:
                max_resp = window['return_period'].max()
                q = response[resp_val_col].quantile(quantile)
                # 1 if no values exceed, 0 if any exceed
                above_q = int((window[resp_val_col] > q).sum() == 0)
            
            records.append((idx, max_resp, above_q))
        


        response_to_extremes = pd.DataFrame.from_records(
            records,
            columns=['index', 'return_period_response', 'rainfall_1_intensity']
        ).set_index("index")

        
        return response_to_extremes

    def join_results(extreme_peaks: pd.DataFrame,
                     response_to_extremes: pd.DataFrame,
                     ratio_thresh: float = ratio_thresh) -> pd.DataFrame:
        """
        Join `metrics` onto `extreme_peaks`, compute:
        - ratio = return_period / max_response
        - above_20_ratio = 1 if ratio > ratio_thresh else 0

        Returns a tidy DataFrame with columns:
        ['datetime', rp_col, 'max_response', 'above_99th_quantile', 'ratio', 'above_20_ratio']
        """
        df = extreme_peaks.join(response_to_extremes)
        df["ratio_calculation"] = df['return_period'] / df['return_period_response']
        df['rainfall_2_ratio'] = (df["ratio_calculation"] > ratio_thresh).astype(int)
        
        return df[["datetime", "return_period", "return_period_response", "rainfall_1_intensity", "ratio_calculation", "rainfall_2_ratio"]]
    
    # aplly functions
    extreme_peaks = get_peak_events(predictor, return_period, value_col)
    if extreme_peaks.empty:
        return None
    response_to_extremes = get_response_events(extreme_peaks, response, predictor_type)
    if response_to_extremes.empty:
        return None
    extreme_peaks = join_results(extreme_peaks, response_to_extremes)

    predictor['rainfall_1_intensity'] = np.nan
    predictor['return_period_response'] = np.nan
    predictor['rainfall_2_ratio'] = np.nan

    if extreme_peaks is not None:
        for idx, row in extreme_peaks.iterrows():
            predictor.loc[idx, 'rainfall_1_intensity'] = row['rainfall_1_intensity']
            predictor.loc[idx, 'return_period_response'] = row['return_period_response']
            predictor.loc[idx, 'rainfall_2_ratio'] = row['rainfall_2_ratio']

    return predictor


def main():
    nrfa_list=pd.read_csv('C:/Users/c1026040/OneDrive - Newcastle University/15_min_flows_2025/flow_stations_final.txt',header=None)

    # Remove the 0s to the left of the list
    nrfa_list=nrfa_list[0].astype(int).tolist()

    # nrfa_list=[42016]

    values={}


    for station in nrfa_list:
        try:
            flow_data=pd.read_csv(f'D:/sensitivity_testing/additional_run/processed_stage/{station}.csv', index_col='datetime', parse_dates=True)
            # rename datetime column to avoid confusion
            flow_data.rename(columns={'datetime.1':'datetime'}, inplace=True)
            rainfall_data=pd.read_csv(f'C:/Users/c1026040/OneDrive - Newcastle University/15_min_2024/nrfa_rainfall/{station}.csv', index_col='datetime', parse_dates=True)
            # add a datetime column to the rainfall data
            rainfall_data=gev_fit_rainfall(rainfall_data, flow_data)
            rainfall_data['datetime']=rainfall_data.index
            value=compare_flow_rainfall(flow_data, rainfall_data, return_period=10, predictor_type='flow')
            values[station]=value
            # ingest the values in the flow data
            flow_data['no_rainfall']=-999
            flow_data['ratio']=-999
            flow_data['above_20_ratio']=-999


            
            if value is None:
                #create columns with -999
                flow_data['no_rainfall']=-999
                flow_data['ratio']=-999
                flow_data['above_20_ratio']=-999
            else:
                for i in range(len(value)):

                    flow_data.loc[value['datetime'].iloc[i],'no_rainfall']=value['above_99th_quantile'].iloc[i]
                    flow_data.loc[value['datetime'].iloc[i],'ratio']=value['ratio'].iloc[i]
                    flow_data.loc[value['datetime'].iloc[i],'above_20_ratio']=value['above_20_ratio'].iloc[i]
            # Sort

            flow_data.to_csv(f'D:/sensitivity_testing/additional_run/processed_stage/{station}.csv')
        except:
            print(f'Error with station {station}')
            continue
        # flow_data=pd.read_csv(f'C:/Users/c1026040/OneDrive - Newcastle University/15_min_2024/qc/qced_stations/{station}.csv', index_col='datetime', parse_dates=True)
        # rainfall_data=pd.read_csv(f'C:/Users/c1026040/OneDrive - Newcastle University/15_min_2024/nrfa_rainfall/{station}.csv', index_col='datetime', parse_dates=True)

    # Transform the dictionary of pandas df into a df
    df=pd.concat(values, names=["station"]).reset_index(level=0)

    # Save the df to a csv
    df.to_csv('D:/sensitivity_testing/additional_run/rainfall_QC.csv')




    # flow_data=pd.read_csv('C:/Users/c1026040/OneDrive - Newcastle University/15_min_2024/qc/qced_stations/36012.csv', index_col='datetime', parse_dates=True)
    # rainfall_data=pd.read_csv('C:/Users/c1026040/OneDrive - Newcastle University/15_min_2024/nrfa_rainfall/36012.csv', index_col='datetime', parse_dates=True)
    # compare_flow_rainfall(flow_data, rainfall_data)


if __name__ == '__main__':
    main()





