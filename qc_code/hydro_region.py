import pandas as pd

from station import read_data
from station import station

import multiprocessing as mp
from functools import partial
from functools import reduce

class hydro_region:
    def __init__(self,
                hydro_region_nbr=str,

                station_data=list,
                extreme_events_hydro_region=pd.DataFrame,    

                   
                ):
        
            
        self.hydro_region_nbr=hydro_region_nbr

        self.station_data=station_data
        self.extreme_events_hydro_region=extreme_events_hydro_region

    def __str__(self):
        return f'Hydro region {self.hydro_region_nbr}'
    
    def events_nearby(self):
        self.extreme_event_range=[]
        # for all events above 10yr return period get a 3 day window before and after the event
        # convert to datetime
        start_dates= pd.to_datetime(self.extreme_events_hydro_region['datetime'])-pd.Timedelta(days=3)
        end_dates= pd.to_datetime(self.extreme_events_hydro_region['datetime'])+pd.Timedelta(days=3)
        all_events=[]
        for i in range(len(start_dates)):
            all_stations=[]            
            for station in self.station_data:
                station_id=station.station_id
                data=station.data
                station_data=data[start_dates.iloc[i]:end_dates.iloc[i]].copy()
                # find the max value in the window and get the corresponding date
                if station_data.empty:
                    continue
                else:
                    max_value=station_data['value'].max()
                    max_row=station_data[station_data['value']==max_value]
                    # if more than one max row keep only the first one

                    # get only the dateime, return period and flow
                    max_row=max_row[['datetime', 'return_period', 'value']]




                    max_row['station_id']=station_id
                    all_stations.append(max_row)
            all_stations=pd.concat(all_stations)
            # if more than one column with same station id, remove it
            all_stations=all_stations.drop_duplicates(subset='station_id')

            all_events.append(all_stations)
            


        self.extreme_event_range=all_events
    
    def highest_2_3_median(self):
        # get the highest, second highest and thrird highest events and the median return period
        list_event=[]
        median_list=[]
        for n in self.extreme_event_range:
            median_list.append(n['return_period'].median())
            n['rank']=n['return_period'].rank(ascending=False, method='first')
            n=n[n['rank']<=3]

            list_event.append(n)
        
        list_event
        transformed_data = []
        for n in range(len(list_event)):
            # Add an 'event_nbr' column with the current index `n`
            list_event[n]['event_nbr'] = n

            # Pivot the DataFrame
            pivoted = list_event[n].pivot(index='event_nbr', columns='rank', values=['return_period','datetime','station_id'])

            # Append the pivoted DataFrame to the transformed_data list
            transformed_data.append(pivoted)
        result = pd.concat(transformed_data)
        result['median']=median_list
        self.highest_event_data=result

    def validate_events(self):
        ratio=(self.highest_event_data['return_period'][1]/self.highest_event_data['return_period'][2]>20).astype(int)
        return_period=(self.highest_event_data['return_period'][2]<2).astype(int)
        #add codes to hydro-region
        self.extreme_events_hydro_region['ratio']=ratio.values
        self.extreme_events_hydro_region['return_period_qc']=return_period.values

    def feed_events_to_qced_data(self):
        # For every event in extreme_events_hydro_region find the station id in station data
        for n in self.station_data:
            match_rows=self.extreme_events_hydro_region[self.extreme_events_hydro_region['station_id']==n.station_id]
            # create a ratio and return period qc column
            for i in range(len(match_rows)):
                # Feed the ratio and return period qc to the station data with the correct datetime
                datetime=match_rows.iloc[i]['datetime']
                ratio=match_rows.iloc[i]['ratio']
                return_period_qc=match_rows.iloc[i]['return_period_qc']
                n.data.loc[datetime,'ratio_hydro_region']=ratio
                n.data.loc[datetime,'return_period_qc']=return_period_qc
            #Export to csv
            n.data.to_csv(f'D:/sensitivity_testing/additional_run/processed_stage/{str(int(n.station_id))}.csv')
                
                

        
        print('test')


        pass








                

def hydro_region_separation(hydroregion_nbr, station_list):
    hydroregion = str(hydroregion_nbr).zfill(3)  
    # convert station_list to string
    station_list = [str(station_nbr).zfill(6) for station_nbr in station_list]
    return [station_nbr for station_nbr in station_list if station_nbr.startswith(hydroregion)]    

def read_stations_from_hydroregion(hydroregion_nbr, nrfa_list, nrfa_metadata, flow_station_folder):
    hydroregion_list=hydro_region_separation(hydroregion_nbr, nrfa_list)
    station_data=[]
    high_rp_events=[]
    for station in hydroregion_list:

        station_test=read_data(station, flow_station_folder,nrfa_metadata)

        if station_test is not None:

            station_test.data.index=pd.to_datetime(station_test.data.datetime)
            if station_test is not None:
                station_test.high_flows() 
                filetered_data = station_test.data[station_test.data['return_period']>=10].copy()
                # Filter for events with at least a day inbetween them  
                filetered_data['time_difference']=filetered_data.index.to_series().diff().dt.days
                filetered_data['event_nbr']=(filetered_data['time_difference']>1).cumsum()
                max_indices=filetered_data.groupby('event_nbr')['value'].idxmax()
                max_values=filetered_data.loc[max_indices]
                max_values['station_id']=station_test.station_id
                station_data.append(station_test)
                high_rp_events.append(max_values[['station_id','datetime', 'return_period', 'value']])
        #Flatten high_rpevents
    high_rp_events=pd.concat(high_rp_events)

    hydro_region_data=hydro_region(hydro_region_nbr=hydroregion_nbr, station_data=station_data, extreme_events_hydro_region=high_rp_events)
    return hydro_region_data
    
def main():
    nrfa_list=pd.read_csv('C:/Users/c1026040/OneDrive - Newcastle University/15_min_flows_2025/flow_stations_final.txt',header=None)

    # Remove the 0s to the left of the list
    nrfa_list=nrfa_list[0].astype(int).tolist()
    nrfa_metadata='C:/Users/c1026040/OneDrive - Newcastle University/15_min_2024/qc/nrfa-station-metadata-2024-10-22.csv'
    flow_station_folder='D:/sensitivity_testing/additional_run/processed_stage/'#'C:/Users/c1026040/OneDrive - Newcastle University/15_min_2024/qc/0_pre_treatment/station_output'    
    # hydroregion_nbr=68
    #generate a list from 56 to 108
    hydroregions=pd.read_csv(r'D:\sensitivity_testing\run_05_rel_spikes_5_abs_spikes_99_drops_5_sluctuations_8_truncation_low_7_truncation_high_99/hydro_regions_numbers.csv',header=None)
    hydroregions=hydroregions[0].astype(int).tolist()
    # hydroregions=[70,71,72,73,74,75,76,
    #               77,78,79,80,81,82,83,84,85,86,89,92,93,94,95,96,101,106,
    #               201,202,203,205,206,236]
    # hydroregions=[54,55]


    for hydroregion_nbr in hydroregions:
        workflow(nrfa_list, nrfa_metadata, flow_station_folder, hydroregion_nbr)
    # func = partial(workflow, nrfa_list, nrfa_metadata, flow_station_folder)
    # with mp.Pool(processes=mp.cpu_count()) as pool:
    #     pool.map(func, hydroregions)
    #     pool.close()
    #     pool.join()
    # hydro_region=read_stations_from_hydroregion(hydroregion_nbr=hydroregion_nbr, nrfa_list=nrfa_list, nrfa_metadata=nrfa_metadata, flow_station_folder=flow_station_folder)
    # hydro_region.events_nearby()
    # hydro_region.highest_2_3_median()
    # hydro_region.validate_events()
    # hydro_region.feed_events_to_qced_data()


    # # export to csv
    # hydro_region.extreme_events_hydro_region.to_csv(f'C:/Users/c1026040/OneDrive - Newcastle University/15_min_2024/qc/hydro_region/{hydroregion_nbr}.csv')


    print(hydro_region.highest_2_3_median)

def workflow(nrfa_list, nrfa_metadata, flow_station_folder, hydroregion_nbr):
    hydro_region=read_stations_from_hydroregion(hydroregion_nbr=hydroregion_nbr, nrfa_list=nrfa_list, nrfa_metadata=nrfa_metadata, flow_station_folder=flow_station_folder)
    hydro_region.events_nearby()
    hydro_region.highest_2_3_median()
    hydro_region.validate_events()
    hydro_region.feed_events_to_qced_data()
        # export to csv
    hydro_region.extreme_events_hydro_region.to_csv(f'D:/sensitivity_testing/additional_run/hydro_region/{hydroregion_nbr}.csv')

if __name__ == '__main__':
    main()




    
        
    
           

