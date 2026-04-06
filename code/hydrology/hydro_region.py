import pandas as pd


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
        start_dates = pd.to_datetime(self.extreme_events_hydro_region['datetime']) - pd.Timedelta(days=3)
        end_dates = pd.to_datetime(self.extreme_events_hydro_region['datetime']) + pd.Timedelta(days=3)
        all_events=[]
        for i in range(len(start_dates)):
            all_stations=[]
            for station_x in self.station_data:
                station_id=station_x.station_id
                data=station_x.data
                station_data=data.loc[start_dates.iloc[i]:end_dates.iloc[i]].copy()
                if station_data.empty:
                    continue
                max_value=station_data['value'].max()
                max_row=station_data[station_data['value']==max_value].head(1).copy()
                max_row=max_row[['datetime', 'return_period', 'value']]
                max_row['station_id']=station_id
                all_stations.append(max_row)
            if len(all_stations)==0:
                continue
            all_stations=pd.concat(all_stations)
            all_stations=all_stations.drop_duplicates(subset='station_id')
            all_events.append(all_stations)

        self.extreme_event_range=all_events

    def highest_2_3_median(self):
        list_event=[]
        median_list=[]
        for n in self.extreme_event_range:
            median_list.append(n['return_period'].median())
            n=n.copy()
            n['rank']=n['return_period'].rank(ascending=False, method='first')
            n=n[n['rank']<=3]
            list_event.append(n)

        transformed_data = []
        for i in range(len(list_event)):
            list_event[i]=list_event[i].copy()
            list_event[i]['event_nbr'] = i
            pivoted = list_event[i].pivot(index='event_nbr', columns='rank', values=['return_period','datetime','station_id'])
            transformed_data.append(pivoted)

        if len(transformed_data) == 0:
            self.highest_event_data = pd.DataFrame()
        else:
            result = pd.concat(transformed_data)
            result['median']=median_list
            self.highest_event_data=result

    def validate_events(self):
        if self.highest_event_data.empty:
            self.extreme_events_hydro_region['hydro_region_2_ratio']=0
            self.extreme_events_hydro_region['hydro_region_1_intensity']=0
            return

        ratio=(self.highest_event_data['return_period'][1]/self.highest_event_data['return_period'][2]>20).astype(int)
        return_period=(self.highest_event_data['return_period'][2]<2).astype(int)
        self.extreme_events_hydro_region['hydro_region_2_ratio']=ratio.values
        self.extreme_events_hydro_region['hydro_region_1_intensity']=return_period.values

    def feed_events_to_qced_data(self):
        for station_x in self.station_data:
            match_rows=self.extreme_events_hydro_region[self.extreme_events_hydro_region['station_id']==station_x.station_id]
            station_x.data['hydro_region_2_ratio']=station_x.data.get('hydro_region_2_ratio', 0)
            station_x.data['hydro_region_1_intensity']=station_x.data.get('hydro_region_1_intensity', 0)
            station_x.data.index = pd.to_datetime(station_x.data.index)
            for i in range(len(match_rows)):
                datetime=pd.to_datetime(match_rows.iloc[i]['datetime'])
                hydro_region_2_ratio=match_rows.iloc[i]['hydro_region_2_ratio']
                hydro_region_1_intensity=match_rows.iloc[i]['hydro_region_1_intensity']
                station_x.data.loc[datetime,'hydro_region_2_ratio']=hydro_region_2_ratio
                station_x.data.loc[datetime,'hydro_region_1_intensity']=hydro_region_1_intensity


def hydro_region_separation(hydroregion_nbr, station_list):
    hydroregion = str(hydroregion_nbr).zfill(3)
    station_list = [str(station_nbr).zfill(6) for station_nbr in station_list]
    return [station_nbr for station_nbr in station_list if station_nbr.startswith(hydroregion)]
