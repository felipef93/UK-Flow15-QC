import os
import csv
import pandas as pd
import numpy as np

from basic_qc.negatives import detect_negatives
from basic_qc.unrealistically_high import detect_high_values
from basic_qc.spikes import detect_rel_spikes, detect_abs_spikes
from basic_qc.drops import detect_drops
from basic_qc.shifts import detect_shifts
from basic_qc.truncated import detect_truncated

from consistency_checks.nrfa_daily import detect_differences_daily
from consistency_checks.nrfa_amax import detect_differences_amax, detect_differences_pot

from high_flows_qc.above_std import detect_high_std
from high_flows_qc.return_periods import gev_fit, detect_high_return_period
from high_flows_qc.top_amax import detect_above_second_highest_amax
from hydrology.rainfall_checks import rainfall_checks


class station:
    '''

    '''
    def __init__(   self,
                    station_id=str,
                    station_metadata=list,

                    data=pd.DataFrame,
                    nrfa_amax=pd.DataFrame,
                    nrfa_pot=pd.DataFrame,
                    nrfa_daily=pd.DataFrame,
                    nrfa_rainfall=pd.DataFrame,

                    qc_metadata=dict,
                 ):

        self.station_id=station_id
        self.station_metadata=station_metadata

        self.data=data
        self.nrfa_amax=nrfa_amax
        self.nrfa_pot=nrfa_pot
        self.nrfa_daily=nrfa_daily
        self.nrfa_rainfall=nrfa_rainfall

        self.qc_metadata=qc_metadata

    def basic_qc(self) -> pd.DataFrame:
        self.data = detect_negatives(self.data)
        self.data = detect_high_values(self.data)
        self.data = detect_rel_spikes(self.data)
        self.data = detect_abs_spikes(self.data)
        self.data = detect_drops(self.data)
        self.data = detect_shifts(self.data)
        self.data = detect_truncated(self.data)

    def consistency_qc(self,
                       daily_threshold: float = 0.05,
                       amax_threshold: float = 0.2,
                       pot_threshold: float = 0.2):
        if self.nrfa_amax is not None:
            self.data=detect_differences_amax(self.data, self.nrfa_amax, amax_threshold)
        if self.nrfa_pot is not None:
            self.data= detect_differences_pot(self.data, self.nrfa_pot, pot_threshold)
        if self.nrfa_daily is not None:
            self.data=detect_differences_daily(self.data, self.nrfa_daily,daily_threshold)

    def high_flows(self,
                   n_std: int = 6,
                   threshold_rp: int = 1000,
                   factor_second_highest: int = 2):
        self.data=detect_high_std(self.data, n_std=n_std)
        self.data=gev_fit(self.data)
        self.data=detect_high_return_period(self.data, threshold_rp=threshold_rp)
        self.data=detect_above_second_highest_amax(self.data, factor_second_highest=factor_second_highest)

    def rainfall_qc(self,
                    threshold_rp: int = 10,
                    window_size: int = 3,
                    quantile_extreme: float = 0.99,
                    ratio_threshold: float = 20):
        if self.nrfa_rainfall is not None:
            self.data=rainfall_checks(self.data, self.nrfa_rainfall, return_period=threshold_rp, window_days=window_size, quantile=quantile_extreme, ratio_thresh=ratio_threshold)
            print('rainfall')

    def metadata(self):
        self.qc_metadata={}
        self.qc_metadata['station_id']=self.station_id
        self.qc_metadata['high']=self.data['high'][self.data['high']>0].count() if 'high' in self.data.columns else 0
        self.qc_metadata['negatives']=self.data['negatives'][self.data['negatives']>0].count() if 'negatives' in self.data.columns else 0
        self.qc_metadata['rel_spike']=self.data['rel_spike'][self.data['rel_spike']>0].count() if 'rel_spike' in self.data.columns else 0
        self.qc_metadata['abs_spike']=self.data['abs_spike'][self.data['abs_spike']>0].count() if 'abs_spike' in self.data.columns else 0
        self.qc_metadata['drops']=self.data['drops'][self.data['drops']>0].count() if 'drops' in self.data.columns else 0
        self.qc_metadata['shifts']=self.data['shifts'][self.data['shifts']>0].count() if 'shifts' in self.data.columns else 0
        self.qc_metadata['truncation']=self.data['truncation'][self.data['truncation']>0].count() if 'truncation' in self.data.columns else 0
        self.qc_metadata['high_truncation']=self.data['high_truncation'][self.data['high_truncation']>0].count() if 'high_truncation' in self.data.columns else 0

        if self.nrfa_amax is not None and 'nrfa_amax' in self.data.columns:
            self.qc_metadata['nrfa_amax']=self.data['nrfa_amax'][self.data['nrfa_amax']>0].count()
        else:
            self.qc_metadata['nrfa_amax']=0

        if self.nrfa_pot is not None and 'nrfa_pot' in self.data.columns:
            self.qc_metadata['nrfa_pot']=self.data['nrfa_pot'][self.data['nrfa_pot']>0].count()
        else:
            self.qc_metadata['nrfa_pot']=0

        if self.nrfa_daily is not None and 'nrfa_daily' in self.data.columns:
            self.qc_metadata['nrfa_daily']=self.data['nrfa_daily'][self.data['nrfa_daily']>0].count()
        else:
            self.qc_metadata['nrfa_daily']=0

        self.qc_metadata['above_6std']=self.data['above_6std'][self.data['above_6std']>0].count() if 'above_6std' in self.data.columns else 0
        self.qc_metadata['above_1000_rp']=self.data['above_1000_rp'][self.data['above_1000_rp']>0].count() if 'above_1000_rp' in self.data.columns else 0
        self.qc_metadata['top_amax']=self.data['top_amax'][self.data['top_amax']>0].count() if 'top_amax' in self.data.columns else 0

        self.qc_metadata['rainfall_1_intensity']=self.data['rainfall_1_intensity'][self.data['rainfall_1_intensity']>0].count() if 'rainfall_1_intensity' in self.data.columns else 0
        self.qc_metadata['rainfall_2_ratio']=self.data['rainfall_2_ratio'][self.data['rainfall_2_ratio']>0].count() if 'rainfall_2_ratio' in self.data.columns else 0
        self.qc_metadata['hydro_region_2_ratio']=self.data['hydro_region_2_ratio'][self.data['hydro_region_2_ratio']>0].count() if 'hydro_region_2_ratio' in self.data.columns else 0
        self.qc_metadata['hydro_region_1_intensity']=self.data['hydro_region_1_intensity'][self.data['hydro_region_1_intensity']>0].count() if 'hydro_region_1_intensity' in self.data.columns else 0

    def write_results(self):
        self.data.to_csv(f'D:/sensitivity_testing/additional_run/processed_stage/{self.station_id}.csv')
        if not os.path.exists('D:/sensitivity_testing/additional_run/basic_qc_metadata.csv'):
            with open('D:/sensitivity_testing/additional_run/basic_qc_metadata.csv', 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(self.qc_metadata.keys())
                writer.writerow(self.qc_metadata.values())
        else:
            with open('D:/sensitivity_testing/additional_run/basic_qc_metadata.csv', 'a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(self.qc_metadata.values())
