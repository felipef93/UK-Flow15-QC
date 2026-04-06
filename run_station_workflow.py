from __future__ import annotations

import sys
import types
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


def _register_high_flow_aliases(src_dir: Path) -> None:
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))

    import high_flows_qc.auxiliary_calculate_amax as amax_mod

    alias_pkg = types.ModuleType('high_flow_checks')
    alias_pkg.__path__ = []
    sys.modules.setdefault('high_flow_checks', alias_pkg)
    sys.modules['high_flow_checks.auxiliary_calculate_amax'] = amax_mod


def _read_optional_nrfa_file(folder: Path | None, station_id: str) -> pd.DataFrame | None:
    if folder is None or not folder.exists():
        return None

    candidate = folder / f'{station_id}.csv'
    if candidate.exists():
        try:
            return pd.read_csv(candidate)
        except Exception as exc:
            print(f'Could not read {candidate.name}: {exc}')
            return None
    return None


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


def _summarise_flags(data: pd.DataFrame, station_id: str) -> dict:
    summary = {'station_id': station_id}
    for col in SUMMARY_COLUMNS[1:]:
        summary[col] = int(pd.to_numeric(data[col], errors='coerce').fillna(0).gt(0).sum()) if col in data.columns else 0
    return summary


def process_station(
    station_id: str,
    flow_file: Path,
    station_module,
    nrfa_metadata: pd.DataFrame | None,
    amax_folder: Path | None,
    pot_folder: Path | None,
    daily_folder: Path | None,
    rainfall_folder: Path | None,
    outputs_dir: Path,
) -> dict:
    data = pd.read_csv(flow_file)

    station_metadata = pd.DataFrame()
    if nrfa_metadata is not None and 'id' in nrfa_metadata.columns:
        try:
            station_metadata = nrfa_metadata.loc[nrfa_metadata['id'].astype(int) == int(station_id)]
        except Exception:
            station_metadata = pd.DataFrame()

    station_obj = station_module.station(
        station_id=station_id,
        station_metadata=station_metadata,
        data=data,
        nrfa_amax=_read_optional_nrfa_file(amax_folder, station_id),
        nrfa_pot=_read_optional_nrfa_file(pot_folder, station_id),
        nrfa_daily=_read_optional_nrfa_file(daily_folder, station_id),
        nrfa_rainfall=_read_optional_nrfa_file(rainfall_folder, station_id),
    )

    station_obj.basic_qc()
    station_obj.consistency_qc()
    station_obj.high_flows()
    station_obj.rainfall_qc()

    station_obj.data = _format_output_data(station_obj.data)

    output_file = outputs_dir / f'{station_id}_qced.csv'
    station_obj.data.to_csv(output_file, index=False)
    print(f'Saved {output_file}')

    return _summarise_flags(station_obj.data, station_id)


def main() -> None:
    project_root = Path(__file__).resolve().parent
    src_dir = project_root / 'src'
    sample_dir = project_root / 'sample_stations'
    outputs_dir = project_root / 'outputs'
    outputs_dir.mkdir(exist_ok=True)

    _register_high_flow_aliases(src_dir)
    import station as station_module

    flow_folder = sample_dir / '15_min'
    nrfa_dir = sample_dir / 'NRFA'
    amax_folder = nrfa_dir / 'AMAX'
    pot_folder = nrfa_dir / 'POT'
    daily_folder = nrfa_dir / 'Daily'
    rainfall_folder = nrfa_dir / 'Rainfall'

    metadata_path = sample_dir / 'nrfa_metadata.csv'
    nrfa_metadata = pd.read_csv(metadata_path) if metadata_path.exists() else None

    flow_files = sorted([file for file in flow_folder.glob('*.csv') if len(file.stem) == 6 and file.stem.isdigit()])
    if not flow_files:
        raise FileNotFoundError(f'No sample station files found in {flow_folder}')

    summaries = []
    for flow_file in flow_files:
        station_id = flow_file.stem
        try:
            summaries.append(
                process_station(
                    station_id=station_id,
                    flow_file=flow_file,
                    station_module=station_module,
                    nrfa_metadata=nrfa_metadata,
                    amax_folder=amax_folder,
                    pot_folder=pot_folder,
                    daily_folder=daily_folder,
                    rainfall_folder=rainfall_folder,
                    outputs_dir=outputs_dir,
                )
            )
        except Exception as exc:
            print(f'Failed for station {station_id}: {exc}')

    if summaries:
        summary_df = pd.DataFrame(summaries)
        for col in SUMMARY_COLUMNS:
            if col not in summary_df.columns:
                summary_df[col] = 0
        summary_df = summary_df[SUMMARY_COLUMNS]
        summary_path = outputs_dir / 'qc_summary.csv'
        summary_df.to_csv(summary_path, index=False)
        print(f'Saved {summary_path}')


if __name__ == '__main__':
    main()
