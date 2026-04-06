from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd


OUTPUT_COLUMNS = [
    'datetime',
    'value',
    'return_period',
    'resolution',
    'negatives',
    'high',
    'rel_spike',
    'abs_spike',
    'drops',
    'shifts',
    'truncation',
    'high_truncation',
    'nrfa_amax',
    'nrfa_pot',
    'nrfa_daily',
    'above_6std',
    'above_1000_rp',
    'top_amax',
    'rainfall_1_intensity',
    'rainfall_2_ratio',
    'hydro_region_2_ratio',
    'hydro_region_1_intensity',
]

SUMMARY_COLUMNS = [
    'station_id',
    'high',
    'negatives',
    'rel_spike',
    'abs_spike',
    'drops',
    'shifts',
    'truncation',
    'high_truncation',
    'nrfa_amax',
    'nrfa_pot',
    'nrfa_daily',
    'above_6std',
    'above_1000_rp',
    'top_amax',
    'rainfall_1_intensity',
    'rainfall_2_ratio',
    'hydro_region_2_ratio',
    'hydro_region_1_intensity',
]

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / 'src'

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from station import station
from hydrology.hydro_region import hydro_region, hydro_region_separation


def _build_station_object(station_id: str, flow_file: Path) -> station:
    data = pd.read_csv(flow_file)
    station_obj = station(
        station_id=station_id,
        station_metadata=pd.DataFrame(),
        data=data,
        nrfa_amax=None,
        nrfa_pot=None,
        nrfa_daily=None,
        nrfa_rainfall=None,
        qc_metadata={},
    )
    station_obj.high_flows()
    station_obj.data['datetime'] = pd.to_datetime(station_obj.data['datetime'])
    station_obj.data.index = station_obj.data['datetime']
    return station_obj


def _get_high_rp_events(station_obj: station) -> pd.DataFrame:
    filtered_data = station_obj.data[station_obj.data['return_period'] >= 10].copy()
    if filtered_data.empty:
        return pd.DataFrame(columns=['station_id', 'datetime', 'return_period', 'value'])

    filtered_data['time_difference'] = filtered_data.index.to_series().diff().dt.days
    filtered_data['event_nbr'] = (filtered_data['time_difference'] > 1).cumsum()
    max_indices = filtered_data.groupby('event_nbr')['value'].idxmax()
    max_values = filtered_data.loc[max_indices].copy()
    max_values['station_id'] = station_obj.station_id
    return max_values[['station_id', 'datetime', 'return_period', 'value']]


def _format_output_data(data: pd.DataFrame) -> pd.DataFrame:
    data = data.copy()
    if 'flag' in data.columns:
        data = data.drop(columns='flag')
    if 'return_period_response' in data.columns:
        data = data.drop(columns='return_period_response')
    if 'resolution' in data.columns:
        data['resolution'] = data['resolution'].fillna(0).astype(int)
    for col in OUTPUT_COLUMNS:
        if col not in data.columns:
            data[col] = 0
    return data[[col for col in OUTPUT_COLUMNS if col in data.columns]]


def _update_summary(outputs_dir: Path) -> None:
    summary_path = outputs_dir / 'qc_summary.csv'
    if not summary_path.exists():
        return

    summary_df = pd.read_csv(summary_path)
    if 'station_id' in summary_df.columns:
        summary_df['station_id'] = summary_df['station_id'].astype(str).str.zfill(6)

    for station_file in sorted(outputs_dir.glob('*_qced.csv')):
        station_id = station_file.stem.replace('_qced', '')
        if len(station_id) != 6 or not station_id.isdigit():
            continue

        data = pd.read_csv(station_file)
        hydro_region_2_ratio = int(pd.to_numeric(data.get('hydro_region_2_ratio', pd.Series([0] * len(data))), errors='coerce').fillna(0).gt(0).sum())
        hydro_region_1_intensity = int(pd.to_numeric(data.get('hydro_region_1_intensity', pd.Series([0] * len(data))), errors='coerce').fillna(0).gt(0).sum())

        match = summary_df['station_id'] == station_id
        if match.any():
            summary_df.loc[match, 'hydro_region_2_ratio'] = hydro_region_2_ratio
            summary_df.loc[match, 'hydro_region_1_intensity'] = hydro_region_1_intensity
        else:
            row = {col: 0 for col in SUMMARY_COLUMNS}
            row['station_id'] = station_id
            row['hydro_region_2_ratio'] = hydro_region_2_ratio
            row['hydro_region_1_intensity'] = hydro_region_1_intensity
            summary_df = pd.concat([summary_df, pd.DataFrame([row])], ignore_index=True)

    for col in SUMMARY_COLUMNS:
        if col not in summary_df.columns:
            summary_df[col] = 0
    summary_df = summary_df[SUMMARY_COLUMNS]
    summary_df.to_csv(summary_path, index=False)


def main() -> None:
    sample_dir = PROJECT_ROOT / 'sample_stations'
    flow_folder = sample_dir / '15_min'
    outputs_dir = PROJECT_ROOT / 'outputs'

    flow_files = sorted([file for file in flow_folder.glob('*.csv') if len(file.stem) == 6 and file.stem.isdigit()])
    if not flow_files:
        raise FileNotFoundError(f'No sample station files found in {flow_folder}')

    station_objects = []
    for flow_file in flow_files:
        station_id = flow_file.stem
        try:
            station_objects.append(_build_station_object(station_id, flow_file))
        except Exception as exc:
            print(f'Failed to build hydro-region station {station_id}: {exc}')

    hydro_regions = sorted({station_obj.station_id[:3] for station_obj in station_objects})

    for hydro_region_nbr in hydro_regions:
        hydro_station_ids = hydro_region_separation(hydro_region_nbr, [station_obj.station_id for station_obj in station_objects])
        hydro_station_data = [station_obj for station_obj in station_objects if station_obj.station_id in hydro_station_ids]
        high_rp_events = []
        for station_obj in hydro_station_data:
            station_events = _get_high_rp_events(station_obj)
            if not station_events.empty:
                high_rp_events.append(station_events)

        if len(high_rp_events) == 0:
            continue

        high_rp_events = pd.concat(high_rp_events, ignore_index=True)
        hydro_region_data = hydro_region(
            hydro_region_nbr=hydro_region_nbr,
            station_data=hydro_station_data,
            extreme_events_hydro_region=high_rp_events,
        )
        hydro_region_data.events_nearby()
        hydro_region_data.highest_2_3_median()
        hydro_region_data.validate_events()
        hydro_region_data.feed_events_to_qced_data()

    for station_obj in station_objects:
        output_file = outputs_dir / f'{station_obj.station_id}_qced.csv'
        if not output_file.exists():
            continue

        existing = pd.read_csv(output_file)
        station_data = station_obj.data.reset_index(drop=True)

        if 'hydro_region_2_ratio' in station_data.columns:
            existing['hydro_region_2_ratio'] = station_data['hydro_region_2_ratio'].fillna(0).astype(int)
        else:
            existing['hydro_region_2_ratio'] = 0

        if 'hydro_region_1_intensity' in station_data.columns:
            existing['hydro_region_1_intensity'] = station_data['hydro_region_1_intensity'].fillna(0).astype(int)
        else:
            existing['hydro_region_1_intensity'] = 0

        existing = _format_output_data(existing)
        existing.to_csv(output_file, index=False)
        print(f'Updated {output_file}')

    _update_summary(outputs_dir)
    print(f'Updated {outputs_dir / "qc_summary.csv"}')


if __name__ == '__main__':
    main()
