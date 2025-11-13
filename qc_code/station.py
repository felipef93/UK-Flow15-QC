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
import high_flows_qc.top_amax as detect_above_second_highest_amax





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
        """
        Run the full suite of basic QC checks in the following order:
        1. detect_negatives
        2. detect_high_values
        3. detect_rel_spikes
        4. detect_abs_spikes
        5. detect_drops
        6. detect_shifts
        7. detect_truncated

        Parameters
        ----------
        data : pd.DataFrame
        Time-indexed DataFrame containing the flow series in column 'value'.

        Returns
        -------
        pd.DataFrame
        DataFrame with new columns added for each QC flag:
        - 'negatives'
        - 'high'
        - 'rel_spike'
        - 'abs_spike'
        - 'drops'
        - 'shifts'
        - 'truncation' and 'high_truncation'
        """
        self.data = detect_negatives(self.data) ########### Note: I migh not even need to return the data here if I correct the funcitons
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
        '''
        Run consistency checks comparing station data to NRFA references:
          - Daily flows
          - Annual maximum flows
          - POT series

        Parameters
        ----------
        daily_threshold : float - Tolerance for daily flow differences (default 0.05).
        amax_threshold : float - Tolerance for annual max flow differences (default 0.2).
        pot_threshold : float - Tolerance for POT series differences (default 0.2).

        Returns
        -------
        pd.DataFrame - The station DataFrame with NRFA flag columns added.
        '''
        if self.nrfa_amax is not None:
            self.data=detect_differences_amax(self.data, self.nrfa_amax, amax_threshold)
        if self.nrfa_pot is not None:
            self.data= detect_differences_pot(self.data, self.nrfa_pot, pot_threshold)
        if self.nrfa_daily is not None:
            self.data=detect_differences_daily(self.data, self.nrfa_daily,daily_threshold)
###
###
##  I trasform the index in here in the function calculate_amax
##
###
###        
    def high_flows(self,
                   n_std: int = 6,
                   threshold_rp: int = 1000,
                   factor_second_highest: int = 2):
        
        '''
        Function that performs high flow checks on the data
        '''
        self.data=detect_high_std(self.data, n_std=n_std)
        self.data=gev_fit(self.data)
        self.data=detect_high_return_period(self.data, threshold_rp=threshold_rp)
        self.data=detect_above_second_highest_amax(self.data, factor_second_highest=factor_second_highest)

    def rainfall_qc(self,
                    threshold_rp: int = 10,
                    window_size: int = 3,
                    quantile_extreme: float = 0.99,
                    ratio_threshold: float = 20):
        self.data=compare_flow_rainfall(self.data, self.nrfa_rainfall, return_period=threshold_rp, window_days=window_size,quantile=quantile_extreme, ratio_thresh=ratio_threshold)


    def metadata(self):
        '''
        Function that returns the metadata of the station
        '''
        self.qc_metadata={}
        self.qc_metadata['station_id']=self.station_id
        self.qc_metadata['high_values']=self.data['high'][self.data['high']>0].count()
        self.qc_metadata['negatives']=self.data['negative'][self.data['negative']>0].count()
        self.qc_metadata['rel_spikes']=self.data['rel_spike'][self.data['rel_spike']>0].count()
        self.qc_metadata['abs_spikes']=self.data['abs_spike'][self.data['abs_spike']>0].count()
        self.qc_metadata['drops']=self.data['drops'][self.data['drops']>0].count()
        self.qc_metadata['fluctuations']=self.data['shifts'][self.data['shifts']>0].count()
        self.qc_metadata['weekly_truncated']=self.data['weekly_truncated'][self.data['weekly_truncated']>0].count()
        self.qc_metadata['daily_truncated_high']=self.data['daily_truncated_high'][self.data['daily_truncated_high']>0].count()

        if self.nrfa_amax is not None:
            self.qc_metadata['nrfa_amax_differences']=self.data['nrfa_amax'][self.data['nrfa_amax']>0].count()
        else:
            self.qc_metadata['nrfa_amax_differences']=-9999
        
        if self.nrfa_pot is not None:
            self.qc_metadata['nrfa_pot_differences']=self.data['nrfa_pot'][self.data['nrfa_pot']>0].count()
        else:
            self.qc_metadata['nrfa_pot_differences']=-9999

        if self.nrfa_daily is not None:
            self.qc_metadata['nrfa_daily_differences']=self.data['nrfa_daily'][self.data['nrfa_daily']>0].count()
        else:
            self.qc_metadata['nrfa_daily_differences']=-9999

        self.qc_metadata['above_6std']=self.data['above_6std'][self.data['above_6std']>0].count()
        self.qc_metadata['above_1000_rp']=self.data['above_1000_rp'][self.data['above_1000_rp']>0].count()
        self.qc_metadata['top_amax']=self.data['top_amax'][self.data['top_amax']>0].count()

        if self.nrfa_rainfall is not None:
            self.qc_metadata['rainfall_1_intensity']=self.data['rainfall_1_intensity'][self.data['rainfall_1_intensity']>0].count()
            self.qc_metadata['rainfall_2_ratio']=self.data['rainfall_2_ratio'][self.data['rainfall_2_ratio']>0].count()


    def write_results(self):
        self.data.to_csv(f'D:/sensitivity_testing/additional_run/processed_stage/{self.station_id}.csv')
        # If no csv_file for metadata create a file for meta
        if not os.path.exists('D:/sensitivity_testing/additional_run/basic_qc_metadata.csv'):
            with open('D:/sensitivity_testing/additional_run/basic_qc_metadata.csv', 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(self.qc_metadata.keys())
                writer.writerow(self.qc_metadata.values())
        else:
            with open('D:/sensitivity_testing/additional_run/basic_qc_metadata.csv', 'a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(self.qc_metadata.values())




def read_nrfa_files(nrfa_id,amax_folder, pot_folder, daily_folder, rainfall_folder):
    try:
        nrfa_amax=pd.read_csv(f'{amax_folder}/{str(nrfa_id)}.csv',index_col=0,parse_dates=True)
    except:
        nrfa_amax=None
        print(f'No amax data for station {nrfa_id}')
    try:
        nrfa_pot=pd.read_csv(f'{pot_folder}/{str(nrfa_id)}.csv',index_col=0,parse_dates=True)
    except:
        nrfa_pot=None
        print(f'No pot data for station {nrfa_id}')
    try:
        nrfa_daily=pd.read_csv(f'{daily_folder}/{str(nrfa_id)}.csv',index_col=0,parse_dates=True)
    except:
        nrfa_daily=None
        print(f'No daily data for station {nrfa_id}')
    try:
        nrfa_rainfall=pd.read_csv(f'{rainfall_folder}/{str(nrfa_id)}.csv',index_col=0,parse_dates=True)
    except:
        nrfa_rainfall=None
        print(f'No rainfall data for station {nrfa_id}')

    return nrfa_amax,nrfa_pot,nrfa_daily, nrfa_rainfall

def read_data(nrfa_id, flow_station_folder, nrfa_metadata, amax_folder=None, pot_folder=None, daily_folder=None, rainfall_folder=None):

    nrfa_data=pd.read_csv(nrfa_metadata)
    try:

        data = pd.read_csv(f'{flow_station_folder}/{str(int(nrfa_id))}.csv')

        if amax_folder is not None:
            nrfa_amax,nrfa_pot,nrfa_daily, nrfa_rainfall = read_nrfa_files(nrfa_id, amax_folder, pot_folder, daily_folder, rainfall_folder)

        
            station_x=station(
                station_id=nrfa_id,
                station_metadata=nrfa_data.loc[nrfa_data['id']==nrfa_id],
                data=data,
                nrfa_amax=nrfa_amax,
                nrfa_pot=nrfa_pot,
                nrfa_daily=nrfa_daily,
                nrfa_rainfall=nrfa_rainfall
                )
        else:
            station_x=station(
                station_id=nrfa_id,
                station_metadata=nrfa_data.loc[nrfa_data['id']==nrfa_id],
                data=data)
        
        return station_x 
    except:
        data=None
        print(f'No flow data for station {nrfa_id}')
                



def workflow(nrfa_id,nrfa_metadata, amax_folder, pot_folder, daily_folder, rainfall_folder, flow_station_folder):
    # try:
    station_test=read_data(nrfa_id,nrfa_metadata=nrfa_metadata, amax_folder=amax_folder, pot_folder=pot_folder, daily_folder=daily_folder, rainfall_folder=rainfall_folder, flow_station_folder=flow_station_folder)
    if station_test is not None:
        station_test.basic_qc()
        station_test.consistency_qc()
        station_test.high_flows()    
        station_test.metadata()
        station_test.write_results()

        return station_test
    # except:
    #     print(f'Issue with station {nrfa_id}, whubalulubalulba')
    #     return None

def parallel_workflow(nrfa_list,nrfa_metadata, amax_folder, pot_folder, daily_folder, rainfall_folder, flow_station_folder):
    func = partial(workflow, nrfa_metadata=nrfa_metadata, amax_folder=amax_folder, pot_folder=pot_folder, daily_folder=daily_folder, rainfall_folder=rainfall_folder, flow_station_folder=flow_station_folder)
    pool = mp.Pool(mp.cpu_count(), maxtasksperchild=1)
    pool.map(func, nrfa_list)
    pool.close()
    pool.join()
    # for i in nrfa_list:
    #     workflow(i,nrfa_metadata=nrfa_metadata, amax_folder=amax_folder, pot_folder=pot_folder, daily_folder=daily_folder, rainfall_folder=rainfall_folder, flow_station_folder=flow_station_folder)
    # # test only one 
    # workflow(42813,nrfa_metadata=nrfa_metadata, amax_folder=amax_folder, pot_folder=pot_folder, daily_folder=daily_folder, rainfall_folder=rainfall_folder, flow_station_folder=flow_station_folder)

def main():

    nrfa_list=pd.read_csv('C:/Users/c1026040/OneDrive - Newcastle University/15_min_flows_2025/flow_stations_final.txt',header=None)

    # Remove the 0s to the left of the list
    nrfa_list=nrfa_list[0].astype(int).tolist()
    nrfa_list.sort()
    nrfa_list[0]

    nrfa_metadata='C:/Users/c1026040/OneDrive - Newcastle University/15_min_2024/qc/nrfa-station-metadata-2024-10-22.csv'
    amax_folder='C:/Users/c1026040/OneDrive - Newcastle University/15_min_2024/nrfa_amax'
    pot_folder= 'C:/Users/c1026040/OneDrive - Newcastle University/15_min_2024/nrfa_pot'
    daily_folder='C:/Users/c1026040/OneDrive - Newcastle University/15_min_2024/nrfa_daily'
    rainfall_folder='C:/Users/c1026040/OneDrive - Newcastle University/15_min_2024/nrfa_rainfall'
    flow_station_folder='C:/Users/c1026040/OneDrive - Newcastle University/15_min_flows_2025/station_output'

    # Split the list into 300 stations subsections
    nrfa_list=[nrfa_list[i:i + 300] for i in range(0, len(nrfa_list), 300)]
    

    for i in nrfa_list:
        parallel_workflow(i,nrfa_metadata, amax_folder, pot_folder, daily_folder, rainfall_folder, flow_station_folder)

    # parallel_workflow(nrfa_list,nrfa_metadata, amax_folder, pot_folder, daily_folder, rainfall_folder, flow_station_folder)

    # station_test=read_data(68005,nrfa_metadata=nrfa_metadata, amax_folder=amax_folder, pot_folder=pot_folder, daily_folder=daily_folder, rainfall_folder=rainfall_folder, flow_station_folder=flow_station_folder)
    # if station_test is not None:
    #     station_test.basic_qc()
    #     station_test.consistency_qc()
    #     station_test.high_flows()    
    #     station_test.metadata()
    #     station_test.write_results()

    print('Parallel workflow')

   


if __name__ == '__main__':
    main()
