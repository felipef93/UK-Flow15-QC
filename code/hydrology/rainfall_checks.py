import pandas as pd
import numpy as np
from high_flows_qc.return_periods import gev_fit


def gev_fit_rainfall(rainfall_data, flow_data):
    rainfall_data = rainfall_data.copy()
    flow_data = flow_data.copy()

    flow_data['datetime'] = pd.to_datetime(flow_data['datetime'])
    flow_start = flow_data['datetime'].iloc[0]
    flow_end = flow_data['datetime'].iloc[-1]

    if 'datetime' in rainfall_data.columns:
        rainfall_data['datetime'] = pd.to_datetime(rainfall_data['datetime'])
    else:
        rainfall_data['datetime'] = pd.to_datetime(rainfall_data.index)

    rainfall_data = rainfall_data[(rainfall_data['datetime'] >= flow_start) & (rainfall_data['datetime'] <= flow_end)].copy()
    rainfall_data = gev_fit(rainfall_data)
    rainfall_data.index = pd.to_datetime(rainfall_data['datetime'])
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
    predictor = predictor.copy()
    predictor['datetime'] = pd.to_datetime(predictor['datetime'])
    predictor.index = predictor['datetime']

    response = gev_fit_rainfall(response, predictor)

    def get_peak_events(predictor: pd.DataFrame = predictor,
                        return_period: int = return_period,
                        value_col='value') -> pd.DataFrame:
        above_rp = predictor[predictor['return_period'] >= return_period].copy()
        if above_rp.empty or response.empty:
            extreme_peaks = pd.DataFrame()
            print('No data or no event above 10yr RP')
        else:
            above_rp['time_difference'] = above_rp.index.to_series().diff().dt.days
            above_rp['event_nbr'] = (above_rp['time_difference'] > 1).cumsum()
            peak_locations = above_rp.groupby('event_nbr')[value_col].idxmax()
            extreme_peaks = above_rp.loc[peak_locations]
        return extreme_peaks

    def get_response_events(extreme_peaks: pd.DataFrame,
                            response: pd.DataFrame,
                            predictor_type: str = 'flow',
                            window_days: int = window_days,
                            resp_val_col: str = resp_val_col,
                            quantile: float = quantile) -> pd.DataFrame:
        extreme_peaks = extreme_peaks.copy()
        extreme_peaks['datetime'] = pd.to_datetime(extreme_peaks['datetime'])

        records = []
        for idx, row in extreme_peaks.iterrows():
            dt = row['datetime']
            if predictor_type == 'flow':
                start, end = dt - pd.Timedelta(days=window_days), dt
            else:
                start, end = dt, dt + pd.Timedelta(days=window_days)

            window = response.loc[start:end]
            if window.empty:
                max_resp = np.nan
                above_q = np.nan
            else:
                max_resp = window['return_period'].max()
                q = response[resp_val_col].quantile(quantile)
                above_q = int((window[resp_val_col] > q).sum() == 0)

            records.append((idx, max_resp, above_q))

        response_to_extremes = pd.DataFrame.from_records(
            records,
            columns=['index', 'return_period_response', 'rainfall_1_intensity']
        ).set_index('index')

        return response_to_extremes

    def join_results(extreme_peaks: pd.DataFrame,
                     response_to_extremes: pd.DataFrame,
                     ratio_thresh: float = ratio_thresh) -> pd.DataFrame:
        df = extreme_peaks.join(response_to_extremes)
        df['ratio_calculation'] = df['return_period'] / df['return_period_response']
        df['rainfall_2_ratio'] = (df['ratio_calculation'] > ratio_thresh).astype(int)
        return df[['datetime', 'return_period', 'return_period_response', 'rainfall_1_intensity', 'ratio_calculation', 'rainfall_2_ratio']]

    extreme_peaks = get_peak_events(predictor, return_period, value_col)
    if extreme_peaks.empty:
        return predictor

    response_to_extremes = get_response_events(extreme_peaks, response, predictor_type)
    if response_to_extremes.empty:
        return predictor

    extreme_peaks = join_results(extreme_peaks, response_to_extremes)

    predictor['rainfall_1_intensity'] = np.nan
    predictor['rainfall_2_ratio'] = np.nan

    for idx, row in extreme_peaks.iterrows():
        predictor.loc[idx, 'rainfall_1_intensity'] = row['rainfall_1_intensity']
        predictor.loc[idx, 'rainfall_2_ratio'] = row['rainfall_2_ratio']

    predictor.index = range(len(predictor))
    return predictor


def rainfall_checks(predictor, response, value_col='value', return_period=10, predictor_type='flow', window_days=3, resp_val_col='value', quantile=0.99, ratio_thresh=20):
    return compare_flow_rainfall(
        predictor,
        response,
        value_col=value_col,
        return_period=return_period,
        predictor_type=predictor_type,
        window_days=window_days,
        resp_val_col=resp_val_col,
        quantile=quantile,
        ratio_thresh=ratio_thresh
    )
